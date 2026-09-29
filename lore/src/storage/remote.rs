// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! `RemoteEndpoint` — connection-pool wrapper used by the storage API to drive remote-side
//! ops without depending on `lore_revision::store::remote::RemoteImmutableStore`.
//!
//! Holds only what the storage API actually needs: the `(remote_url, identity)` pair plus
//! per-partition `Connection` caches for the storage-session and admin paths. Construction
//! does not open the wire — connections are established lazily by the first call that needs
//! one. Each `lore_storage_open` with a `remote_config` builds its own `RemoteEndpoint` on
//! `StoreInternal`; cross-handle connection reuse is intentionally out of scope here.

use std::collections::HashMap;
use std::sync::Arc;

use lore_revision::lore::RepositoryId;
use lore_revision::protocol;
use lore_transport::Admin;
use lore_transport::Connection;
use lore_transport::ProtocolError;
use lore_transport::SessionPool;
use lore_transport::StorageSession;
use tokio::sync::Mutex;

/// Lazily-resolving handle to a peer storage service. One instance per open handle that has a
/// remote configured. Internally caches one `Arc<Connection>` per partition for both the
/// storage-session and admin paths so repeated ops against the same partition share the
/// connection.
pub(crate) struct RemoteEndpoint {
    remote_url: String,
    identity: Option<String>,
    /// Cached connections used for `StorageSession` ops.
    sessions: Mutex<HashMap<RepositoryId, Arc<Connection>>>,
    /// Cached connections used for admin ops (`obliterate`, etc.). Kept separate from
    /// `sessions` to mirror the existing transport-level split — admin connections may end up
    /// pinned to a different physical socket than session connections.
    admins: Mutex<HashMap<RepositoryId, Arc<Connection>>>,
}

impl RemoteEndpoint {
    pub(crate) fn new(remote_url: impl Into<String>, identity: Option<&str>) -> Self {
        Self {
            remote_url: remote_url.into(),
            identity: identity.map(str::to_string),
            sessions: Mutex::new(HashMap::new()),
            admins: Mutex::new(HashMap::new()),
        }
    }

    /// Get (or open) the connection used for storage-session ops on `partition`.
    pub(crate) async fn session_connection(
        &self,
        partition: RepositoryId,
    ) -> Result<Arc<Connection>, ProtocolError> {
        let mut lock = self.sessions.lock().await;
        if let Some(connection) = lock.get(&partition) {
            return Ok(connection.clone());
        }
        let connection = protocol::connect(
            self.remote_url.as_str(),
            self.identity.as_deref().unwrap_or_default(),
            partition,
        )
        .await?;
        lock.insert(partition, connection.clone());
        Ok(connection)
    }

    /// Get (or open) the connection used for admin ops on `partition`.
    async fn admin_connection(
        &self,
        partition: RepositoryId,
    ) -> Result<Arc<Connection>, ProtocolError> {
        let mut lock = self.admins.lock().await;
        if let Some(connection) = lock.get(&partition) {
            return Ok(connection.clone());
        }
        let connection = protocol::connect(
            self.remote_url.as_str(),
            self.identity.as_deref().unwrap_or_default(),
            partition,
        )
        .await?;
        lock.insert(partition, connection.clone());
        Ok(connection)
    }

    /// Resolve a `StorageSession` bound to `partition`. The session is created on first use;
    /// the underlying connection is shared with subsequent calls for the same partition.
    pub(crate) async fn session(
        &self,
        partition: RepositoryId,
    ) -> Result<Arc<StorageSession>, ProtocolError> {
        let connection = self.session_connection(partition).await?;
        let correlation_id = lore_revision::lore::execution_context()
            .globals()
            .correlation_id
            .to_string();
        connection.session(partition, &correlation_id).await
    }

    /// Live sessions for `partition` on the underlying connection, so a Copy on that
    /// connection may read it as its source. Hold the pool until the copies finish.
    pub(crate) async fn authorize_partition(
        &self,
        partition: RepositoryId,
    ) -> Result<Arc<SessionPool>, ProtocolError> {
        let connection = self.session_connection(partition).await?;
        let correlation_id = lore_revision::lore::execution_context()
            .globals()
            .correlation_id
            .to_string();
        connection
            .authorize_partition(partition, &correlation_id)
            .await
    }

    /// Resolve an `Admin` handle for `partition` — used by ops like obliterate that go
    /// through the admin verb rather than a per-session storage verb.
    pub(crate) async fn admin(
        &self,
        partition: RepositoryId,
    ) -> Result<Arc<dyn Admin>, ProtocolError> {
        let connection = self.admin_connection(partition).await?;
        connection.admin(partition).await
    }
}
