// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use dashmap::DashMap;
use lore_base::error::Disconnected;
use lore_base::error::NotAuthenticated;
use lore_base::error::NotAuthorized;
use lore_base::error::NotFound;
use lore_base::error::Oversized;
use lore_base::error::SlowDown;
use lore_base::lore_debug;
use lore_base::lore_spawn;
use lore_base::lore_trace;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::Fragment;
use lore_base::types::Hash;
use lore_base::types::HealResult;
use lore_base::types::KeyType;
use lore_base::types::RepositoryId;
use lore_base::types::VerifyResult;
use lore_error_set::prelude::*;
use tokio::sync::Semaphore;
use tokio::sync::SemaphorePermit;
use zerocopy::IntoBytes;

use super::super::QuicClientError;
use super::super::QuicOpCode;
use super::super::client::AuthAdapter;
use super::super::client::DEFAULT_EXPECTED_RTT_MS;
use super::super::client::EndpointConfig;
use super::super::client::QuicConnection;
use super::super::client::SendWithReconnectError;
use super::super::client::ServiceClient;
use super::super::client::TransportConfig;
use super::super::client::connect;
use super::super::client::send_high_priority_with_reconnect;
use super::super::client::send_normal;
use super::super::client::send_normal_with_reconnect;
use super::super::storage_service;
use super::super::storage_service::Command;
use super::super::storage_service::MAX_CHUNK_SIZE;
use super::super::storage_service::auth::StorageClientAuth;
use crate::auth::exchange::RENEW_TIMEOUT;
use crate::auth::exchange::auth_exchange;
use crate::auth::exchange::renew_authorization;
use crate::connection::Connection;
use crate::error::ProtocolError;
use crate::quic::client::CongestionAlgorithm;
use crate::traits::Storage;

const INFLIGHT_COMMAND_LIMIT: usize = 10000;

// Bounds stopping a server session that a renewal replaced, which runs off the renewal's path.
const STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// A session this client started, under the id its holders use. When the server refuses a
/// command because the session's token expired, the session is authorized again with a fresh
/// token and its server id replaced, so every holder carries on under the same id.
struct ClientSession {
    repository: RepositoryId,
    correlation_id: String,
    /// The identity the session was authorized for; renewals exchange for it alone.
    identity: String,
    server_id: AtomicU32,
    /// The server session the last renewal replaced. It is stopped at the next renewal or when
    /// this session stops, so a command still in flight under it is answered as expired and
    /// sent again rather than failing.
    retired: parking_lot::Mutex<Option<u32>>,
    /// The authorization token the current server session was started with.
    token: parking_lot::Mutex<String>,
    /// Serializes renewals of this session, holding when the last one failed.
    renewal: tokio::sync::Mutex<Option<Instant>>,
}

const MAX_BYTES_BANDWIDTH_PER_SEC: u64 = (1024 * 1024 * 1024) / 8;

/// Stop server session `server_id` on `quic`, if the connection is still up.
async fn stop_server_session(quic: Arc<QuicConnection>, server_id: u32) {
    if !quic.has_streams().await {
        return;
    }
    let _ = send_normal(
        quic,
        Command::Authorize as QuicOpCode,
        server_id,
        true,
        &mut [Bytes::default(), Bytes::from_static(&[1u8])],
    )
    .await;
}

#[allow(dead_code)]
pub struct StorageClient {
    connection: Weak<Connection>,
    remote_url: String,
    transport_config: TransportConfig,
    auth_adapter: Arc<dyn AuthAdapter<ErrorType = ProtocolError>>,
    auth_url: String,
    recipient_domain: String,
    identity: String,
    repository: RepositoryId,
    counter: AtomicUsize,
    quic: Arc<QuicConnection>,
    connection_establish: Semaphore,
    command_limit: Semaphore,
    sent: AtomicUsize,
    /// Sessions by the id their holders use.
    sessions: DashMap<u32, Arc<ClientSession>>,
    /// The same sessions by the id the server knows them under.
    server_sessions: DashMap<u32, Arc<ClientSession>>,
    next_session: AtomicU32,
}

impl Drop for StorageClient {
    fn drop(&mut self) {
        // Close the QUIC connection immediately without waiting for streams to
        // drain. Quinn sends a CLOSE frame and RSTs any open streams. The server
        // handles this gracefully (cleans up all sessions on connection close).
        // This avoids blocking the runtime during shutdown which would deadlock
        // with any in-flight session_stop sends on the same connection.
        self.quic.close_immediate();
        lore_trace!("QUIC storage client dropped");
    }
}

impl StorageClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        connection: Weak<Connection>,
        remote_url: &str,
        transport_config: TransportConfig,
        auth_adapter: Arc<dyn AuthAdapter<ErrorType = ProtocolError>>,
        auth_url: &str,
        recipient_domain: &str,
        identity: &str,
        repository: RepositoryId,
        quinn: quinn::Connection,
    ) -> Self {
        let quic = QuicConnection::with_v4(quinn, MAX_CHUNK_SIZE, true);
        StorageClient {
            connection,
            remote_url: remote_url.to_string(),
            transport_config,
            auth_adapter,
            auth_url: auth_url.to_string(),
            recipient_domain: recipient_domain.to_string(),
            identity: identity.to_string(),
            repository,
            quic: Arc::new(quic),
            connection_establish: Semaphore::new(1),
            counter: AtomicUsize::new(0),
            sent: AtomicUsize::new(0),
            command_limit: Semaphore::new(INFLIGHT_COMMAND_LIMIT),
            sessions: DashMap::new(),
            server_sessions: DashMap::new(),
            next_session: AtomicU32::new(1),
        }
    }

    /// The id the server knows session `session_id` under. An id this client did not issue is
    /// sent as it is, and the server answers for it.
    fn server_id(&self, session_id: u32) -> u32 {
        self.sessions
            .get(&session_id)
            .map_or(session_id, |session| {
                session.server_id.load(Ordering::Acquire)
            })
    }

    /// Exchange for a token authorizing `repository` for `identity`, as `session_start` does.
    async fn exchange(&self, identity: &str, repository: RepositoryId) -> (String, String) {
        if self.auth_url.is_empty() {
            return (String::new(), identity.to_string());
        }
        let (_, token, resolved) =
            auth_exchange(&self.auth_url, &self.recipient_domain, identity, repository).await;
        (token, resolved)
    }

    /// Authorize a server session for `repository` with `token`, returning its server id.
    async fn authorize_start(
        &self,
        repository: RepositoryId,
        correlation_id: &str,
        token: &str,
    ) -> Result<u32, ProtocolError> {
        let token_bytes = token.as_bytes();

        // Build Authorize start payload:
        // action(1=0) + repository_id(16) + corr_len(1) + corr(N) + token_len(2, u16 LE) + token(M)
        let corr_bytes = correlation_id.as_bytes();
        let mut payload =
            BytesMut::with_capacity(1 + 16 + 1 + corr_bytes.len() + 2 + token_bytes.len());
        payload.put_u8(0); // action = start
        payload.extend_from_slice(repository.as_bytes());
        payload.put_u8(corr_bytes.len() as u8);
        payload.extend_from_slice(corr_bytes);
        payload.extend_from_slice(&(token_bytes.len() as u16).to_le_bytes());
        payload.extend_from_slice(token_bytes);
        let payload = payload.freeze();

        let response = send_normal_with_reconnect(self, Command::Authorize, 0, || {
            [Bytes::default(), payload.clone()]
        })
        .await?;

        if response.len() != 4 {
            return Err(ProtocolError::internal(format!(
                "session_start: expected 4-byte response, got {} bytes",
                response.len()
            )));
        }

        Ok(u32::from_le_bytes(response[..4].try_into().unwrap()))
    }

    async fn authorize_stop(&self, server_id: u32) {
        stop_server_session(self.quic.clone(), server_id).await;
    }

    /// Authorize `session` again with a fresh token after the server refused its server session
    /// `expired`, unless another renewal already replaced it. Returns the session's server id.
    /// The attempt is bounded by [`RENEW_TIMEOUT`], and a renewal that failed while this one
    /// waited for it fails this one too.
    async fn renew(
        &self,
        session: &Arc<ClientSession>,
        expired: u32,
    ) -> Result<u32, ProtocolError> {
        let asked = Instant::now();
        let mut renewal = session.renewal.lock().await;
        let current = session.server_id.load(Ordering::Acquire);
        if current != expired {
            return Ok(current);
        }
        if renewal.is_some_and(|failed| failed >= asked) {
            return Err(ProtocolError::from(NotAuthorized));
        }
        let outcome = tokio::time::timeout(RENEW_TIMEOUT, self.reauthorize(session))
            .await
            .unwrap_or_else(|_| {
                lore_debug!(
                    "Storage session renewal for {} timed out",
                    session.repository
                );
                Err(ProtocolError::from(NotAuthorized))
            });
        let (token, renewed) = match outcome {
            Ok(renewed) => renewed,
            Err(err) => {
                *renewal = Some(Instant::now());
                return Err(err);
            }
        };
        *session.token.lock() = token;
        session.server_id.store(renewed, Ordering::Release);
        self.server_sessions.insert(renewed, session.clone());
        let stale = session.retired.lock().replace(expired);
        if let Some(stale) = stale {
            self.forget_server_id(stale, session);
            let quic = self.quic.clone();
            lore_spawn!(async move {
                let _ = tokio::time::timeout(STOP_TIMEOUT, stop_server_session(quic, stale)).await;
            });
        }
        lore_debug!(
            "Renewed storage session for {} ({expired} -> {renewed})",
            session.repository
        );
        Ok(renewed)
    }

    /// Drop `server_id` from the server-id index if it still names `session`. After a reconnect
    /// the server numbers sessions afresh, so the id may name another session by then.
    fn forget_server_id(&self, server_id: u32, session: &Arc<ClientSession>) {
        self.server_sessions
            .remove_if(&server_id, |_, indexed| Arc::ptr_eq(indexed, session));
    }

    /// Exchange for a fresh token for `session` and start a server session with it.
    async fn reauthorize(&self, session: &ClientSession) -> Result<(String, u32), ProtocolError> {
        let old_token = session.token.lock().clone();
        let (_, token) = renew_authorization(&old_token, || async {
            let (token, identity) = self.exchange(&session.identity, session.repository).await;
            (String::new(), token, identity)
        })
        .await?;
        let renewed = self
            .authorize_start(session.repository, &session.correlation_id, &token)
            .await?;
        Ok((token, renewed))
    }

    /// This client's sessions for `repository`, each with the server id it has now.
    fn sessions_for(&self, repository: RepositoryId) -> Vec<(Arc<ClientSession>, u32)> {
        self.sessions
            .iter()
            .filter(|entry| entry.repository == repository)
            .map(|entry| {
                let server_id = entry.server_id.load(Ordering::Acquire);
                (entry.value().clone(), server_id)
            })
            .collect()
    }

    /// Renew `sources`, this client's sessions for a Copy's source as they were when the Copy was
    /// sent, after the server refused the Copy because its only grants to read the source had
    /// expired. A session that another request has renewed since is left as it is.
    async fn renew_source(
        &self,
        sources: &[(Arc<ClientSession>, u32)],
    ) -> Result<(), ProtocolError> {
        if sources.is_empty() {
            return Err(ProtocolError::from(NotAuthorized));
        }
        for (session, observed) in sources {
            self.renew(session, *observed).await?;
        }
        Ok(())
    }

    pub async fn connect(
        connection: Weak<Connection>,
        remote_url: &str,
        remote_domain: String,
        auth_url: &str,
        identity: &str,
        repository: RepositoryId,
    ) -> Result<Self, ProtocolError> {
        let auth_adapter = Arc::new(StorageClientAuth {
            recipient_domain: remote_domain.clone(),
            auth_url: auth_url.to_string(),
            identity: identity.to_string(),
            repository,
        });
        let transport_config = TransportConfig {
            max_bytes_bandwidth_per_second: MAX_BYTES_BANDWIDTH_PER_SEC,
            expected_rtt_ms: DEFAULT_EXPECTED_RTT_MS,
            congestion_algorithm: CongestionAlgorithm::Bbr,
            initial_cwnd: None,
        };

        lore_trace!("QUIC connecting to {remote_url} for repository {repository}");

        let start = Instant::now();

        let quinn = connect(
            &EndpointConfig {
                remote_url: remote_url.to_string(),
                default_port: Self::DEFAULT_PORT,
                sni_override: None,
            },
            auth_adapter.client_certs(),
            Self::ALPN,
            transport_config.clone(),
        )
        .await?;
        let connection_id = quinn.stable_id();

        let storage = StorageClient::new(
            connection,
            remote_url,
            transport_config,
            auth_adapter.clone(),
            auth_url,
            &remote_domain,
            identity,
            repository,
            quinn,
        );

        lore_trace!(
            "QUIC connected to {remote_url} in {}ms",
            start.elapsed().as_millis()
        );

        storage
            .quic
            .create_initial_stream()
            .await
            .internal_with(|| {
                format!("creating initial QUIC stream to {remote_url} for repository {repository}")
            })?;

        auth_adapter.initial_authorize(storage.quic.clone()).await?;
        storage.quic.stream_count.store(1, Ordering::Relaxed);

        lore_debug!(
            "QUIC connection {connection_id} to {remote_url} for repository {repository} complete in {}ms",
            start.elapsed().as_millis()
        );

        Ok(storage)
    }
}

impl ServiceClient for StorageClient {
    type RequestType = Command;
    type ErrorType = ProtocolError;

    const ALPN: &'static str = "lore-storage/0.4";
    const DEFAULT_PORT: u16 = 41337;

    async fn acquire_command_permit(&self) -> Option<SemaphorePermit<'_>> {
        self.command_limit.acquire().await.ok()
    }

    fn quic(&self) -> &Arc<QuicConnection> {
        &self.quic
    }

    fn endpoint_config(&self) -> EndpointConfig {
        EndpointConfig {
            remote_url: self.remote_url.clone(),
            default_port: Self::DEFAULT_PORT,
            sni_override: None,
        }
    }

    fn alpn(&self) -> &str {
        Self::ALPN
    }

    fn map_send_error(
        &self,
        failed_request: Self::RequestType,
        error: SendWithReconnectError,
    ) -> Self::ErrorType {
        match error {
            SendWithReconnectError::PermitAcquire => ProtocolError::internal("permit acquire"),
            SendWithReconnectError::Disconnected => ProtocolError::from(Disconnected),
            SendWithReconnectError::ReconnectFailed => {
                if let Some(connection) = self.connection.upgrade() {
                    connection.stale.store(true, Ordering::Relaxed);
                }
                ProtocolError::from(Disconnected)
            }
            SendWithReconnectError::ClientError(client_error) => match client_error {
                QuicClientError::SlowDown => ProtocolError::from(SlowDown),
                QuicClientError::NotAuthorized | QuicClientError::SessionExpired => {
                    ProtocolError::from(NotAuthorized)
                }
                // Distinct so `copy` can renew its source sessions; nothing else sees it.
                QuicClientError::SourceSessionExpired => ProtocolError::from(NotAuthenticated),
                QuicClientError::NotFound => ProtocolError::from(NotFound),
                QuicClientError::Oversized => ProtocolError::from(Oversized {
                    context: format!(
                        "{}: server rejected oversized fragment",
                        storage_service::command_name(&failed_request)
                    ),
                }),
                _ => {
                    let name = storage_service::command_name(&failed_request);
                    ProtocolError::internal(format!(
                        "{name}: Failed sending command: {client_error}"
                    ))
                }
            },
        }
    }

    fn auth_adapter(&self) -> &Arc<dyn AuthAdapter<ErrorType = Self::ErrorType>> {
        &self.auth_adapter
    }

    fn transport_config(&self) -> TransportConfig {
        self.transport_config.clone()
    }

    fn v4_protocol(&self) -> bool {
        true
    }

    fn renew_session(
        &self,
        session_id: u32,
    ) -> Pin<Box<dyn Future<Output = Option<u32>> + Send + '_>> {
        Box::pin(async move {
            let session = self
                .server_sessions
                .get(&session_id)
                .map(|entry| entry.value().clone())?;
            match self.renew(&session, session_id).await {
                Ok(renewed) => Some(renewed),
                Err(err) => {
                    lore_debug!(
                        "Storage session for {} not renewed: {err}",
                        session.repository
                    );
                    None
                }
            }
        })
    }
}

#[async_trait]
impl Storage for StorageClient {
    async fn session_start(
        &self,
        repository: RepositoryId,
        correlation_id: &str,
    ) -> Result<u32, ProtocolError> {
        // Fetch auth token via token exchange (cached if already exchanged)
        let (token, identity) = self.exchange(&self.identity, repository).await;
        let server_id = self
            .authorize_start(repository, correlation_id, &token)
            .await?;
        let session = Arc::new(ClientSession {
            repository,
            correlation_id: correlation_id.to_string(),
            identity,
            server_id: AtomicU32::new(server_id),
            retired: parking_lot::Mutex::new(None),
            token: parking_lot::Mutex::new(token),
            renewal: tokio::sync::Mutex::new(None),
        });
        let session_id = self.next_session.fetch_add(1, Ordering::Relaxed);
        self.server_sessions.insert(server_id, session.clone());
        self.sessions.insert(session_id, session);
        Ok(session_id)
    }

    async fn session_stop(&self, session_id: u32) -> Result<(), ProtocolError> {
        let Some((_, session)) = self.sessions.remove(&session_id) else {
            return Ok(());
        };
        let server_id = session.server_id.load(Ordering::Acquire);
        let retired = session.retired.lock().take();
        for server_id in std::iter::once(server_id).chain(retired) {
            self.forget_server_id(server_id, &session);
            self.authorize_stop(server_id).await;
        }
        Ok(())
    }

    async fn get(
        &self,
        session_id: u32,
        address: &Address,
    ) -> Result<(Fragment, Bytes), ProtocolError> {
        let mut payload =
            send_normal_with_reconnect(self, Command::Get, self.server_id(session_id), || {
                [Bytes::default(), Bytes::from_owner(*address)]
            })
            .await?;

        if payload.len() < size_of::<Fragment>() {
            return Err(ProtocolError::internal("get: Invalid server response"));
        }

        let fragment_bytes = payload.split_to(size_of::<Fragment>());
        let fragment = unsafe { fragment_bytes.as_ptr().cast::<Fragment>().read_unaligned() };

        if let Err(reason) = lore_base::types::validate_fragment_response(&fragment) {
            return Err(ProtocolError::internal(format!(
                "get: invalid fragment {fragment:?}: {reason}"
            )));
        }
        if payload.len() != fragment.size_payload as usize {
            return Err(ProtocolError::internal(format!(
                "get: Invalid server payload for fragment {fragment:?}, got {} bytes",
                payload.len()
            )));
        }

        Ok((fragment, payload))
    }

    async fn get_metadata(
        &self,
        session_id: u32,
        address: &Address,
    ) -> Result<Fragment, ProtocolError> {
        let payload = send_normal_with_reconnect(
            self,
            Command::GetMetadata,
            self.server_id(session_id),
            || [Bytes::default(), Bytes::from_owner(*address)],
        )
        .await?;

        if payload.len() != size_of::<Fragment>() {
            return Err(ProtocolError::internal(format!(
                "get_metadata: expected {} bytes for Fragment, got {}",
                size_of::<Fragment>(),
                payload.len()
            )));
        }

        let fragment = unsafe { payload.as_ptr().cast::<Fragment>().read_unaligned() };
        Ok(fragment)
    }

    async fn get_priority(
        &self,
        session_id: u32,
        address: &Address,
    ) -> Result<(Fragment, Bytes), ProtocolError> {
        let mut payload = send_high_priority_with_reconnect(
            self,
            Command::Get,
            self.server_id(session_id),
            || [Bytes::default(), Bytes::from_owner(*address)],
        )
        .await?;

        if payload.len() < size_of::<Fragment>() {
            return Err(ProtocolError::internal("get: Invalid server response"));
        }

        let fragment_bytes = payload.split_to(size_of::<Fragment>());
        let fragment = unsafe { fragment_bytes.as_ptr().cast::<Fragment>().read_unaligned() };

        if let Err(reason) = lore_base::types::validate_fragment_response(&fragment) {
            return Err(ProtocolError::internal(format!(
                "get: invalid fragment {fragment:?}: {reason}"
            )));
        }
        if payload.len() != fragment.size_payload as usize {
            return Err(ProtocolError::internal(format!(
                "get: Invalid server payload for fragment {fragment:?}, got {} bytes",
                payload.len()
            )));
        }

        Ok((fragment, payload))
    }

    async fn put(
        &self,
        session_id: u32,
        address: Address,
        fragment: Fragment,
        payload: Option<Bytes>,
    ) -> Result<(), ProtocolError> {
        send_normal_with_reconnect(self, Command::Put, self.server_id(session_id), || {
            [
                Bytes::default(),
                Bytes::from_owner(address),
                Bytes::from_owner(fragment),
                payload.clone().unwrap_or_default(),
            ]
        })
        .await
        .map(|_| ())
    }

    async fn query(&self, session_id: u32, address: &[Address]) -> Result<Bytes, ProtocolError> {
        const MAX_BATCH: usize = lore_base::types::FRAGMENT_SIZE_EXPECTED / size_of::<Address>();
        let address_count = address.len();
        if address_count > MAX_BATCH {
            return Err(ProtocolError::internal(format!(
                "query: Invalid address batch count: {address_count}"
            )));
        }

        // Allocate a copy on the heap to avoid lifetime issues
        let address = Bytes::copy_from_slice(address.as_bytes());
        let payload =
            send_normal_with_reconnect(self, Command::Query, self.server_id(session_id), || {
                [Bytes::default(), address.clone()]
            })
            .await?;

        if payload.len() != address_count {
            return Err(ProtocolError::internal(format!(
                "query: Server returned query payload of wrong size, expected {} got {}",
                address_count,
                payload.len()
            )));
        }

        Ok(payload)
    }

    async fn verify(
        &self,
        session_id: u32,
        address: &Address,
        heal: bool,
    ) -> Result<VerifyResult, ProtocolError> {
        let heal_byte = if heal { 1u8 } else { 0u8 };
        let mut request_bytes = BytesMut::with_capacity(size_of::<Address>() + 1);
        request_bytes.extend_from_slice(address.as_bytes());
        request_bytes.put_u8(heal_byte);
        let request_bytes = request_bytes.freeze();

        let payload =
            send_normal_with_reconnect(self, Command::Verify, self.server_id(session_id), || {
                [Bytes::default(), request_bytes.clone()]
            })
            .await?;

        // Response should be 2 bytes: corrupted + healed
        if payload.len() != 2 {
            return Err(ProtocolError::internal(format!(
                "verify: Invalid server payload for address {address}, got {} bytes",
                payload.len()
            )));
        }

        Ok(VerifyResult {
            corrupted: payload[0] != 0,
            healed: HealResult::from(payload[1]),
        })
    }

    async fn copy(
        &self,
        session_id: u32,
        source_repository: RepositoryId,
        source_address: Address,
        target_context: Context,
    ) -> Result<(), ProtocolError> {
        // Wire payload layout for Copy is: source_repository (16) + source_address (32+16) +
        // target_context (16) = 80 bytes. The target_context tail allows the destination's
        // dedup tag to differ from the source's without transferring the payload.
        let chunks = || {
            [
                Bytes::default(),
                Bytes::from_owner(source_repository),
                Bytes::from_owner(source_address),
                Bytes::from_owner(target_context),
            ]
        };
        let sources = self.sessions_for(source_repository);
        match send_normal_with_reconnect(self, Command::Copy, self.server_id(session_id), chunks)
            .await
        {
            // The only grants to read the source on this connection expired: renew them and
            // copy once more.
            Err(ProtocolError::NotAuthenticated(_)) => {
                self.renew_source(&sources).await?;
                send_normal_with_reconnect(self, Command::Copy, self.server_id(session_id), chunks)
                    .await
                    .map_err(|err| match err {
                        ProtocolError::NotAuthenticated(_) => ProtocolError::from(NotAuthorized),
                        other => other,
                    })
                    .map(|_| ())
            }
            other => other.map(|_| ()),
        }
    }

    async fn mutable_load(
        &self,
        session_id: u32,
        key: &Hash,
        key_type: KeyType,
    ) -> Result<Hash, ProtocolError> {
        let payload = send_normal_with_reconnect(
            self,
            Command::MutableLoad,
            self.server_id(session_id),
            || {
                [
                    Bytes::default(),
                    Bytes::from_owner(*key),
                    Bytes::copy_from_slice(&[key_type as u8]),
                ]
            },
        )
        .await?;

        if payload.len() != size_of::<Hash>() {
            return Err(ProtocolError::internal(format!(
                "mutable_load: Invalid server response, expected {} bytes got {}",
                size_of::<Hash>(),
                payload.len()
            )));
        }

        Ok(Hash::from(&payload[..]))
    }

    async fn mutable_store(
        &self,
        session_id: u32,
        key: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<(), ProtocolError> {
        send_normal_with_reconnect(
            self,
            Command::MutableStore,
            self.server_id(session_id),
            || {
                [
                    Bytes::default(),
                    Bytes::from_owner(key),
                    Bytes::from_owner(value),
                    Bytes::copy_from_slice(&[key_type as u8]),
                ]
            },
        )
        .await
        .map(|_| ())
    }

    async fn mutable_compare_and_swap(
        &self,
        session_id: u32,
        key: Hash,
        expected: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<Hash, ProtocolError> {
        let payload = send_normal_with_reconnect(
            self,
            Command::MutableCas,
            self.server_id(session_id),
            || {
                [
                    Bytes::default(),
                    Bytes::from_owner(key),
                    Bytes::from_owner(expected),
                    Bytes::from_owner(value),
                    Bytes::copy_from_slice(&[key_type as u8]),
                ]
            },
        )
        .await?;

        if payload.len() != size_of::<Hash>() {
            return Err(ProtocolError::internal(format!(
                "mutable_cas: Invalid server response, expected {} bytes got {}",
                size_of::<Hash>(),
                payload.len()
            )));
        }

        Ok(Hash::from(&payload[..]))
    }

    async fn close(&self) {
        self.quic.close().await;
    }
}
