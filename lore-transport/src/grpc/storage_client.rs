// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use dashmap::DashMap;
use lore_base::error::NotFound;
use lore_base::error::SlowDown;
use lore_base::lore_debug;
use lore_base::lore_error;
use lore_base::lore_spawn;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_base::types::Fragment;
use lore_base::types::Hash;
use lore_base::types::HealResult;
use lore_base::types::KeyType;
use lore_base::types::RepositoryId;
use lore_base::types::VerifyResult;
use lore_error_set::prelude::*;
use lore_proto::lore::model::v1 as model_v1;
use lore_proto::lore::storage::v1 as storage_v1;
use lore_proto::lore::storage::v1::storage_service_client::StorageServiceClient;
use tokio::sync::Semaphore;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;

use super::CORRELATION_ID_HEADER;
use super::Channel;
use super::GRPCAuth;
use super::GRPCAuthRef;
use super::PARTITION_ID_KEY;
use super::REPOSITORY_ID_KEY;
use super::is_stream_token_expiry;
use crate::error::ProtocolError;

type Responder<T> = oneshot::Sender<Result<T, ProtocolError>>;
type Inflight<T> = DashMap<Address, Vec<Responder<T>>>;

const STREAM_WRITE_BUFFER_SIZE: usize = 32 * 1024;
const INFLIGHT_COMMAND_LIMIT: usize = 10000;

/// Session context for gRPC metadata injection, cached at `session_start` time. Requests carry
/// the session's current authorization, which the credential refresher and stream renewal keep
/// fresh.
#[derive(Clone)]
pub struct GrpcSessionContext {
    pub repository: RepositoryId,
    pub correlation_id: String,
    pub auth: GRPCAuthRef,
}

impl GrpcSessionContext {
    fn authorization(&self) -> String {
        self.auth.read().authorization_token.clone()
    }
}

#[derive(Clone)]
enum StreamState {
    Open,
    /// The stream task has finished, with the status that ended it, if any.
    Ended(Option<tonic::Status>),
}

/// One request stream of a session, opened with `token`.
struct OpenStream<Req, Resp> {
    sender: mpsc::Sender<(Req, Responder<Resp>)>,
    token: String,
    state: watch::Receiver<StreamState>,
}

impl<Req, Resp> OpenStream<Req, Resp> {
    fn is_open(&self) -> bool {
        !self.sender.is_closed()
            && self.state.has_changed().is_ok()
            && matches!(*self.state.borrow(), StreamState::Open)
    }

    /// The server ended the stream because the token it was opened with expired.
    fn expired(&self) -> bool {
        matches!(&*self.state.borrow(), StreamState::Ended(Some(status)) if is_stream_token_expiry(status))
    }

    fn closed_error(&self, operation: &str) -> ProtocolError {
        match &*self.state.borrow() {
            StreamState::Ended(Some(status)) => ProtocolError::from(status.clone()),
            _ => ProtocolError::internal(format!("{operation}: stream closed")),
        }
    }

    async fn closed(&self) {
        let mut state = self.state.clone();
        let _ = state
            .wait_for(|state| matches!(state, StreamState::Ended(_)))
            .await;
    }
}

/// A session's stream of one kind. A stream that has ended is replaced by the next request.
struct StreamSlot<Req, Resp>(tokio::sync::Mutex<SlotState<Req, Resp>>);

struct SlotState<Req, Resp> {
    stream: Option<Arc<OpenStream<Req, Resp>>>,
    /// When the last renewal of an expired stream failed.
    renewal_failed: Option<(Instant, ProtocolError)>,
}

impl<Req, Resp> Default for StreamSlot<Req, Resp> {
    fn default() -> Self {
        Self(tokio::sync::Mutex::new(SlotState {
            stream: None,
            renewal_failed: None,
        }))
    }
}

impl<Req, Resp> StreamSlot<Req, Resp> {
    /// The slot's open stream, or a new one. A stream the server ended because its token
    /// expired is replaced only after the session's authorization is renewed, under the slot's
    /// lock, so no request is sent on a stream reopened with the expired token. A renewal that
    /// failed while a request waited for the lock fails that request too.
    async fn open(
        &self,
        ctx: &GrpcSessionContext,
        spawn: impl FnOnce() -> OpenStream<Req, Resp>,
    ) -> Result<Arc<OpenStream<Req, Resp>>, ProtocolError> {
        let asked = Instant::now();
        let mut slot = self.0.lock().await;
        if let Some(stream) = slot.stream.clone() {
            if stream.is_open() {
                return Ok(stream);
            }
            if stream.expired() {
                if let Some((failed, err)) = &slot.renewal_failed
                    && *failed >= asked
                {
                    return Err(err.clone());
                }
                lore_debug!("gRPC stream token expired, renewing authorization");
                if let Err(err) = GRPCAuth::renew(&ctx.auth, &stream.token).await {
                    slot.renewal_failed = Some((Instant::now(), err.clone()));
                    return Err(err);
                }
            }
        }
        let stream = Arc::new(spawn());
        slot.stream = Some(stream.clone());
        Ok(stream)
    }
}

/// Per-session streaming state keyed by session ID.
#[derive(Default)]
struct SessionStreams {
    get: StreamSlot<Address, Arc<storage_v1::GetResponse>>,
    get_metadata: StreamSlot<Address, Arc<storage_v1::GetResponse>>,
    put: StreamSlot<storage_v1::PutRequest, Arc<storage_v1::PutResponse>>,
    copy: StreamSlot<storage_v1::CopyRequest, ()>,
}

pub struct StorageService {
    client: StorageServiceClient<Channel>,
    /// Per-session streams keyed by session ID.
    streams: DashMap<u32, Arc<SessionStreams>>,
    get_put_limiter: Semaphore,
}

fn inject_metadata<T>(request: &mut tonic::Request<T>, ctx: &GrpcSessionContext, token: &str) {
    let md = request.metadata_mut();
    md.insert_bin(
        PARTITION_ID_KEY,
        tonic::metadata::BinaryMetadataValue::from_bytes(ctx.repository.data()),
    );
    md.insert_bin(
        REPOSITORY_ID_KEY,
        tonic::metadata::BinaryMetadataValue::from_bytes(ctx.repository.data()),
    );
    if !ctx.correlation_id.is_empty()
        && let Ok(val) = MetadataValue::from_str(&ctx.correlation_id)
    {
        md.insert(CORRELATION_ID_HEADER, val);
    }
    if !token.is_empty()
        && let Ok(mut val) = MetadataValue::from_str(&format!("Bearer {token}"))
    {
        val.set_sensitive(true);
        md.insert("authorization", val);
    }
}

/// Answer every request still waiting on a stream that has ended.
fn fail_inflight<T>(inflight: &Inflight<T>, error: &ProtocolError) {
    let addresses: Vec<Address> = inflight.iter().map(|entry| *entry.key()).collect();
    for address in addresses {
        if let Some((_, responders)) = inflight.remove(&address) {
            for responder in responders {
                let _ = responder.send(Err(error.clone()));
            }
        }
    }
}

/// Record how a stream ended, then fail the requests it will never answer. The state is set
/// first, so a failed request can tell whether the stream expired.
fn end_stream<T>(
    state: &watch::Sender<StreamState>,
    inflight: &Inflight<T>,
    status: Option<tonic::Status>,
    operation: &str,
) {
    let error = match &status {
        Some(status) => ProtocolError::from(status.clone()),
        None => ProtocolError::internal(format!("{operation}: stream closed")),
    };
    let _ = state.send(StreamState::Ended(status));
    fail_inflight(inflight, &error);
}

impl StorageService {
    pub fn new(channel: Channel) -> Self {
        let client = StorageServiceClient::new(channel);

        Self {
            client,
            streams: DashMap::new(),
            get_put_limiter: Semaphore::new(INFLIGHT_COMMAND_LIMIT),
        }
    }

    /// Remove streams for a session. Dropping the senders terminates the stream tasks.
    pub fn remove_session_streams(&self, session_id: u32) {
        self.streams.remove(&session_id);
    }

    fn session_streams(&self, session_id: u32) -> Arc<SessionStreams> {
        #[allow(clippy::disallowed_methods)] // Write lock is brief; no await while held.
        self.streams
            .entry(session_id)
            .or_insert_with(|| Arc::new(SessionStreams::default()))
            .clone()
    }

    /// Send `request` on the stream in `slot`, opening one with the session's current
    /// authorization when there is none. When the server ended the stream because the token it
    /// was opened with expired, resend once on the slot's next stream, which is opened only
    /// after the authorization is renewed; a renewal that finds no fresh authorization fails the
    /// request instead.
    async fn stream_request<Req: Clone, Resp>(
        &self,
        ctx: &GrpcSessionContext,
        slot: &StreamSlot<Req, Resp>,
        spawn: impl Fn(&GrpcSessionContext) -> OpenStream<Req, Resp>,
        request: Req,
        operation: &'static str,
    ) -> Result<Resp, ProtocolError> {
        let _permit = self
            .get_put_limiter
            .acquire()
            .await
            .internal("permit acquire")?;
        let mut resent = false;
        loop {
            let stream = slot.open(ctx, || spawn(ctx)).await?;
            let (tx, rx) = oneshot::channel();
            let outcome = match stream.sender.send((request.clone(), tx)).await {
                Ok(()) => tokio::select! {
                    biased;
                    response = rx => response.unwrap_or_else(|_| Err(stream.closed_error(operation))),
                    () = stream.closed() => Err(stream.closed_error(operation)),
                },
                Err(_) => Err(stream.closed_error(operation)),
            };
            match outcome {
                Err(_) if !resent && stream.expired() => {
                    resent = true;
                    lore_debug!("gRPC {operation} stream token expired, sending again");
                }
                outcome => return outcome,
            }
        }
    }

    pub async fn get(
        &self,
        session_id: u32,
        ctx: &GrpcSessionContext,
        address: &Address,
    ) -> Result<(Fragment, Bytes), ProtocolError> {
        lore_debug!("gRPC get fragment: {}", address);

        let streams = self.session_streams(session_id);
        let res = self
            .stream_request(
                ctx,
                &streams.get,
                |ctx| self.spawn_get_stream(ctx),
                *address,
                "get",
            )
            .await?;

        let Some(fragment) = res.fragment else {
            lore_error!("Invalid get response, missing fragment");
            return Err(ProtocolError::internal("get: Missing fragment"));
        };

        let fragment = Fragment {
            flags: fragment.flags,
            size_payload: fragment.size_payload,
            size_content: fragment.size_content,
        };

        if let Err(reason) = lore_base::types::validate_fragment_response(&fragment) {
            lore_error!("Invalid fragment in get response {fragment:?}: {reason}");
            return Err(ProtocolError::internal(format!(
                "get: invalid fragment: {reason}"
            )));
        }
        if res.payload.len() != fragment.size_payload as usize {
            lore_error!(
                "Fragment payload is invalid in get response : {} bytes, expected {}",
                res.payload.len(),
                fragment.size_payload
            );
            return Err(ProtocolError::internal("get: Invalid payload"));
        }

        Ok((fragment, res.payload.clone()))
    }

    /// Fetch only the fragment metadata for an address. Same wire request as `get` (just an
    /// `Address`), but the server's response carries `Fragment` only — no payload bytes — so
    /// callers that don't need the payload skip the transfer cost. Used by the storage API's
    /// query op for remote-hit metadata lookups.
    pub async fn get_metadata(
        &self,
        session_id: u32,
        ctx: &GrpcSessionContext,
        address: &Address,
    ) -> Result<Fragment, ProtocolError> {
        lore_debug!("gRPC get_metadata fragment: {}", address);

        let streams = self.session_streams(session_id);
        let res = self
            .stream_request(
                ctx,
                &streams.get_metadata,
                |ctx| self.spawn_get_metadata_stream(ctx),
                *address,
                "get_metadata",
            )
            .await?;

        let Some(fragment) = res.fragment else {
            lore_error!("Invalid get_metadata response, missing fragment");
            return Err(ProtocolError::internal("get_metadata: Missing fragment"));
        };

        Ok(Fragment {
            flags: fragment.flags,
            size_payload: fragment.size_payload,
            size_content: fragment.size_content,
        })
    }

    pub async fn put(
        &self,
        session_id: u32,
        ctx: &GrpcSessionContext,
        address: Address,
        fragment: Fragment,
        payload: Option<Bytes>,
    ) -> Result<(), ProtocolError> {
        lore_debug!("Put fragment: {address}");

        let streams = self.session_streams(session_id);
        let v1_address: model_v1::Address = address.into();
        let v1_fragment: model_v1::Fragment = fragment.into();
        self.stream_request(
            ctx,
            &streams.put,
            |ctx| self.spawn_put_stream(ctx),
            storage_v1::PutRequest {
                address: Some(v1_address),
                fragment: Some(v1_fragment),
                payload,
            },
            "put",
        )
        .await?;

        Ok(())
    }

    pub async fn query(
        &self,
        ctx: &GrpcSessionContext,
        address: &[Address],
    ) -> Result<Bytes, ProtocolError> {
        lore_debug!("Query {} fragments", address.len());

        let request = storage_v1::QueryRequest {
            addresses: address.iter().map(model_v1::Address::from).collect(),
        };
        let mut client = self.client.clone();
        let mut req = tonic::Request::new(request);
        inject_metadata(&mut req, ctx, &ctx.authorization());

        let res = client
            .query(req)
            .await
            .map(|res| res.into_inner())
            .map_err(|err| match err.code() {
                tonic::Code::Unavailable => ProtocolError::from(SlowDown),
                _ => ProtocolError::internal_with_context(err, "query"),
            })?;

        let mut buffer = BytesMut::with_capacity(res.results.len());
        for value in res.results.iter() {
            buffer.put_u8(*value as u8);
        }
        Ok(buffer.freeze())
    }

    pub async fn verify(
        &self,
        ctx: &GrpcSessionContext,
        address: &Address,
        heal: bool,
    ) -> Result<VerifyResult, ProtocolError> {
        lore_debug!("Verify fragment: {address}");

        let request = storage_v1::VerifyRequest {
            address: Some((*address).into()),
            heal,
        };
        let mut client = self.client.clone();
        let mut req = tonic::Request::new(request);
        inject_metadata(&mut req, ctx, &ctx.authorization());

        client
            .verify(req)
            .await
            .map(|res| res.into_inner())
            .map_err(|err| match err.code() {
                tonic::Code::Unavailable => ProtocolError::from(SlowDown),
                tonic::Code::NotFound => ProtocolError::from(NotFound),
                tonic::Code::Unimplemented => ProtocolError::internal("unsupported: verify"),
                _ => ProtocolError::internal_with_context(err, "verify"),
            })
            .map(|res| VerifyResult {
                corrupted: res.corrupted,
                healed: HealResult::from(res.healed),
            })
    }

    pub async fn copy(
        &self,
        session_id: u32,
        ctx: &GrpcSessionContext,
        source_repository: RepositoryId,
        source_address: Address,
        target_context: Context,
    ) -> Result<(), ProtocolError> {
        lore_debug!(
            "gRPC copy fragment: {} from repository {} (target context {})",
            source_address,
            source_repository,
            target_context
        );

        let streams = self.session_streams(session_id);
        self.stream_request(
            ctx,
            &streams.copy,
            |ctx| self.spawn_copy_stream(ctx),
            storage_v1::CopyRequest {
                source_repository_id: Bytes::copy_from_slice(source_repository.data()),
                source_address: Some(source_address.into()),
                target_context: Bytes::copy_from_slice(zerocopy::IntoBytes::as_bytes(
                    &target_context,
                )),
            },
            "copy",
        )
        .await
    }

    pub async fn mutable_load(
        &self,
        ctx: &GrpcSessionContext,
        key: &Hash,
        key_type: KeyType,
    ) -> Result<Hash, ProtocolError> {
        lore_debug!("gRPC mutable_load: {}", key);

        let request = storage_v1::MutableLoadRequest {
            key: Bytes::copy_from_slice(key.data()),
            key_type: key_type as u32,
        };
        let mut client = self.client.clone();
        let mut req = tonic::Request::new(request);
        inject_metadata(&mut req, ctx, &ctx.authorization());

        let res = client
            .mutable_load(req)
            .await
            .map(|res| res.into_inner())
            .map_err(|err| match err.code() {
                tonic::Code::Unavailable => ProtocolError::from(SlowDown),
                tonic::Code::NotFound => ProtocolError::from(NotFound),
                tonic::Code::Unimplemented => ProtocolError::internal("unsupported: mutable_load"),
                _ => ProtocolError::internal_with_context(err, "mutable_load"),
            })?;

        Ok(Hash::from(&res.value[..]))
    }

    pub async fn mutable_store(
        &self,
        ctx: &GrpcSessionContext,
        key: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<(), ProtocolError> {
        lore_debug!("gRPC mutable_store: {}", key);

        let request = storage_v1::MutableStoreRequest {
            key: Bytes::copy_from_slice(key.data()),
            value: Bytes::copy_from_slice(value.data()),
            key_type: key_type as u32,
        };
        let mut client = self.client.clone();
        let mut req = tonic::Request::new(request);
        inject_metadata(&mut req, ctx, &ctx.authorization());

        client
            .mutable_store(req)
            .await
            .map(|_| ())
            .map_err(|err| match err.code() {
                tonic::Code::Unavailable => ProtocolError::from(SlowDown),
                tonic::Code::Unimplemented => ProtocolError::internal("unsupported: mutable_store"),
                _ => ProtocolError::internal_with_context(err, "mutable_store"),
            })
    }

    pub async fn mutable_compare_and_swap(
        &self,
        ctx: &GrpcSessionContext,
        key: Hash,
        expected: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<Hash, ProtocolError> {
        lore_debug!("gRPC mutable_cas: {}", key);

        let request = storage_v1::MutableCompareAndSwapRequest {
            key: Bytes::copy_from_slice(key.data()),
            expected: Bytes::copy_from_slice(expected.data()),
            value: Bytes::copy_from_slice(value.data()),
            key_type: key_type as u32,
        };
        let mut client = self.client.clone();
        let mut req = tonic::Request::new(request);
        inject_metadata(&mut req, ctx, &ctx.authorization());

        let res = client
            .mutable_compare_and_swap(req)
            .await
            .map(|res| res.into_inner())
            .map_err(|err| match err.code() {
                tonic::Code::Unavailable => ProtocolError::from(SlowDown),
                tonic::Code::Unimplemented => ProtocolError::internal("unsupported: mutable_cas"),
                _ => ProtocolError::internal_with_context(err, "mutable_cas"),
            })?;

        Ok(Hash::from(&res.current_value[..]))
    }

    fn spawn_copy_stream(
        &self,
        ctx: &GrpcSessionContext,
    ) -> OpenStream<storage_v1::CopyRequest, ()> {
        let mut client = self.client.clone();
        let (tx, mut rx) =
            mpsc::channel::<(storage_v1::CopyRequest, Responder<()>)>(STREAM_WRITE_BUFFER_SIZE);

        let inflight = Arc::new(Inflight::<()>::new());

        let request_inflight = inflight.clone();
        let request_stream = async_stream::stream! {
            while let Some((request, sender)) = rx.recv().await {
                let Some(address) = request.source_address.clone() else {
                    lore_error!("Missing source_address in copy request");
                    break;
                };

                let mut send = false;
                #[allow(clippy::disallowed_methods)]
                {
                    let address = Address::from(address);
                    request_inflight.entry(address).or_insert_with(|| { send = true; vec![] }).push(sender);
                }
                if send {
                    yield request;
                }
            }
        };

        let token = ctx.authorization();
        let (state, stream_state) = watch::channel(StreamState::Open);
        let ctx = ctx.clone();
        let stream_token = token.clone();
        lore_spawn!({
            async move {
                let mut req = tonic::Request::new(request_stream);
                inject_metadata(&mut req, &ctx, &stream_token);

                let ended = match client.copy(req).await {
                    Err(err) => Some(err),
                    Ok(response) => {
                        let mut response_stream = response.into_inner();
                        loop {
                            match response_stream.next().await {
                                None => break None,
                                Some(Ok(response)) => {
                                    let Some(address) = response.source_address.clone() else {
                                        break Some(tonic::Status::internal(
                                            "copy: Copy response missing source_address",
                                        ));
                                    };
                                    let address = Address::from(address);
                                    let senders = inflight.remove(&address);
                                    if let Some((_, senders)) = senders {
                                        for sender in senders {
                                            let _ = sender.send(Ok(())).map_err(|err| {
                                                lore_error!("Copy request failed sending response to requestor: {err:?}");
                                            });
                                        }
                                    } else {
                                        lore_error!(
                                            "Received unexpected copy result for fragment address: {address}",
                                        );
                                    }
                                }
                                Some(Err(e)) => {
                                    lore_error!("Error during fragment copy: {e}");
                                    let address = Address::from(e.details());
                                    if address.is_zero() {
                                        break Some(e);
                                    }
                                    let senders = inflight.remove(&address);
                                    if let Some((_, senders)) = senders {
                                        // Preserve the original status code (NotFound,
                                        // NotAuthorized, Unavailable, …) by going through
                                        // `ProtocolError::From<tonic::Status>` rather than
                                        // wrapping every error as Internal. Callers downstream
                                        // (e.g. `lore::storage::copy`'s tier-2 fallback) need to
                                        // pattern-match on the variant to decide whether a
                                        // recovery path makes sense.
                                        let err = ProtocolError::from(e);
                                        for sender in senders {
                                            let _ = sender.send(Err(err.clone())).map_err(|err| {
                                                lore_error!("Copy request failed sending error response to requestor: {err:?}");
                                            });
                                        }
                                    } else {
                                        lore_error!(
                                            "Missing hash received from Status error: {:?}",
                                            address.hash
                                        );
                                    }
                                }
                            }
                        }
                    }
                };
                end_stream(&state, &inflight, ended, "copy");
            }
        });

        OpenStream {
            sender: tx,
            token,
            state: stream_state,
        }
    }

    fn spawn_get_stream(
        &self,
        ctx: &GrpcSessionContext,
    ) -> OpenStream<Address, Arc<storage_v1::GetResponse>> {
        let mut client = self.client.clone();
        let (tx, mut rx) = mpsc::channel::<(Address, Responder<Arc<storage_v1::GetResponse>>)>(
            STREAM_WRITE_BUFFER_SIZE,
        );

        let inflight = Arc::new(Inflight::<Arc<storage_v1::GetResponse>>::new());

        let request_inflight = inflight.clone();
        let request = async_stream::stream! {
            while let Some((address, sender)) = rx.recv().await {
                let mut send = false;
                #[allow(clippy::disallowed_methods)]
                {
                    request_inflight.entry(address).or_insert_with(|| { send = true; vec![] }).push(sender);
                }
                if send {
                    let v1_address: model_v1::Address = address.into();
                    yield v1_address;
                }
            }
        };

        let token = ctx.authorization();
        let (state, stream_state) = watch::channel(StreamState::Open);
        let ctx = ctx.clone();
        let stream_token = token.clone();
        lore_spawn!(async move {
            let mut req = tonic::Request::new(request);
            inject_metadata(&mut req, &ctx, &stream_token);

            let ended = match client.get(req).await {
                Err(err) => {
                    lore_error!("Get request failed: {err}");
                    Some(err)
                }
                Ok(response) => {
                    let mut response_stream = response.into_inner();
                    loop {
                        match response_stream.next().await {
                            None => break None,
                            Some(Ok(response)) => {
                                let Some(address) = response.address.as_ref() else {
                                    break Some(tonic::Status::internal(
                                        "get: Get response missing address",
                                    ));
                                };
                                let senders = inflight.remove(&Address::from(address));
                                if let Some((_, senders)) = senders {
                                    let response = Arc::new(response);
                                    for sender in senders {
                                        let _ = sender.send(Ok(response.clone())).map_err(|err| {
                                            lore_error!(
                                                "Get request failed sending response to requestor: {err:?}"
                                            );
                                        });
                                    }
                                } else {
                                    lore_error!(
                                        "Received unexpected result for fragment address during get stream: {:?}",
                                        address.hash
                                    );
                                }
                            }
                            Some(Err(e)) => {
                                let address = Address::from(e.details());
                                if address.is_zero() {
                                    lore_error!("Get stream ended: {e:?}");
                                    break Some(e);
                                }
                                let senders = inflight.remove(&address);
                                if let Some((_, senders)) = senders {
                                    for sender in senders {
                                        let _ = sender.send(Err(ProtocolError::internal(format!("get: {e}")))).map_err(|err| {
                                                lore_error!("Get request failed sending error response to requestor: {err:?}");
                                            });
                                    }
                                } else {
                                    lore_error!("Missing hash received with error: {e:?}");
                                }
                            }
                        }
                    }
                }
            };
            end_stream(&state, &inflight, ended, "get");
        });

        OpenStream {
            sender: tx,
            token,
            state: stream_state,
        }
    }

    fn spawn_get_metadata_stream(
        &self,
        ctx: &GrpcSessionContext,
    ) -> OpenStream<Address, Arc<storage_v1::GetResponse>> {
        let mut client = self.client.clone();
        let (tx, mut rx) = mpsc::channel::<(Address, Responder<Arc<storage_v1::GetResponse>>)>(
            STREAM_WRITE_BUFFER_SIZE,
        );

        let inflight = Arc::new(Inflight::<Arc<storage_v1::GetResponse>>::new());

        let request_inflight = inflight.clone();
        let request = async_stream::stream! {
            while let Some((address, sender)) = rx.recv().await {
                let mut send = false;
                #[allow(clippy::disallowed_methods)]
                {
                    request_inflight.entry(address).or_insert_with(|| { send = true; vec![] }).push(sender);
                }
                if send {
                    let v1_address: model_v1::Address = address.into();
                    yield v1_address;
                }
            }
        };

        let token = ctx.authorization();
        let (state, stream_state) = watch::channel(StreamState::Open);
        let ctx = ctx.clone();
        let stream_token = token.clone();
        lore_spawn!(async move {
            let mut req = tonic::Request::new(request);
            inject_metadata(&mut req, &ctx, &stream_token);

            let ended = match client.get_metadata(req).await {
                Err(err) => {
                    lore_error!("GetMetadata request failed: {err}");
                    Some(err)
                }
                Ok(response) => {
                    let mut response_stream = response.into_inner();
                    loop {
                        match response_stream.next().await {
                            None => break None,
                            Some(Ok(response)) => {
                                let Some(address) = response.address.as_ref() else {
                                    break Some(tonic::Status::internal(
                                        "get_metadata: response missing address",
                                    ));
                                };
                                let senders = inflight.remove(&Address::from(address));
                                if let Some((_, senders)) = senders {
                                    let response = Arc::new(response);
                                    for sender in senders {
                                        let _ = sender.send(Ok(response.clone())).map_err(|err| {
                                            lore_error!(
                                                "GetMetadata sending response to requestor failed: {err:?}"
                                            );
                                        });
                                    }
                                } else {
                                    lore_error!(
                                        "GetMetadata received unexpected result for fragment address: {:?}",
                                        address.hash
                                    );
                                }
                            }
                            Some(Err(e)) => {
                                let address = Address::from(e.details());
                                if address.is_zero() {
                                    lore_error!("GetMetadata stream ended: {e:?}");
                                    break Some(e);
                                }
                                let senders = inflight.remove(&address);
                                if let Some((_, senders)) = senders {
                                    // Preserve original status code so callers can match on
                                    // `NotFound` etc., mirroring the copy-stream fix.
                                    let err = ProtocolError::from(e);
                                    for sender in senders {
                                        let _ = sender.send(Err(err.clone())).map_err(|send_err| {
                                            lore_error!(
                                                "GetMetadata sending error to requestor failed: {send_err:?}"
                                            );
                                        });
                                    }
                                } else {
                                    lore_error!("GetMetadata missing hash with error: {e:?}");
                                }
                            }
                        }
                    }
                }
            };
            end_stream(&state, &inflight, ended, "get_metadata");
        });

        OpenStream {
            sender: tx,
            token,
            state: stream_state,
        }
    }

    fn spawn_put_stream(
        &self,
        ctx: &GrpcSessionContext,
    ) -> OpenStream<storage_v1::PutRequest, Arc<storage_v1::PutResponse>> {
        let mut client = self.client.clone();
        let (tx, mut rx) = mpsc::channel::<(
            storage_v1::PutRequest,
            Responder<Arc<storage_v1::PutResponse>>,
        )>(STREAM_WRITE_BUFFER_SIZE);

        let inflight = Arc::new(Inflight::<Arc<storage_v1::PutResponse>>::new());

        let request_inflight = inflight.clone();
        let request_stream = async_stream::stream! {
            while let Some((request, sender)) = rx.recv().await {
                let Some(address) = request.address.clone() else {
                    lore_error!("Missing address");
                    break;
                };

                let mut send = false;
                #[allow(clippy::disallowed_methods)]
                {
                    let address = Address::from(address);
                    request_inflight.entry(address).or_insert_with(|| { send = true; vec![] }).push(sender);
                }
                if send {
                    yield request;
                }
            }
        };

        let token = ctx.authorization();
        let (state, stream_state) = watch::channel(StreamState::Open);
        let ctx = ctx.clone();
        let stream_token = token.clone();
        lore_spawn!({
            async move {
                let mut req = tonic::Request::new(request_stream);
                inject_metadata(&mut req, &ctx, &stream_token);

                let ended = match client.put(req).await {
                    Err(err) => Some(err),
                    Ok(response) => {
                        let mut response_stream = response.into_inner();
                        loop {
                            match response_stream.next().await {
                                None => break None,
                                Some(Ok(response)) => {
                                    let Some(address) = response.address.clone() else {
                                        break Some(tonic::Status::internal(
                                            "put: Put response missing address",
                                        ));
                                    };
                                    let address = Address::from(address);
                                    let senders = inflight.remove(&address);
                                    if let Some((_, senders)) = senders {
                                        let response = Arc::new(response);
                                        for sender in senders {
                                            let _ = sender.send(Ok(response.clone())).map_err(|err| {
                                                lore_error!("Put request failed sending response to requestor: {err:?}");
                                            });
                                        }
                                    } else {
                                        lore_error!(
                                            "Received unexpected put result for fragment address: {address}",
                                        );
                                    }
                                }
                                Some(Err(e)) => {
                                    lore_error!("Error during fragment put: {e}");
                                    let address = Address::from(e.details());
                                    if address.is_zero() {
                                        break Some(e);
                                    }
                                    let senders = inflight.remove(&address);
                                    if let Some((_, senders)) = senders {
                                        for sender in senders {
                                            let _ = sender
                                                .send(Err(ProtocolError::internal(format!("put: {e}")))).map_err(|err| {
                                                lore_error!("Put request failed sending error response to requestor: {err:?}");
                                            });
                                        }
                                    } else {
                                        lore_error!(
                                            "Missing hash received from Status error: {:?}",
                                            address.hash
                                        );
                                    }
                                }
                            }
                        }
                    }
                };
                end_stream(&state, &inflight, ended, "put");
            }
        });

        OpenStream {
            sender: tx,
            token,
            state: stream_state,
        }
    }
}
