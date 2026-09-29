// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! A `lore-storage/0.4` Copy reads its source repository only through a session that the same
//! caller started on the connection, while that session is running and its token is unexpired.

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_revision::lore::RepositoryId;
use lore_transport::quic::storage_service::Command;
use zerocopy::IntoBytes;

use crate::protocol::attribute_map::AttributeMap;
use crate::protocol::storage::messages::MessageHandleError;
use crate::quic::QuicService;
use crate::quic::storage_service_v4::ParsedStorageRequestV4;
use crate::quic::storage_service_v4::StorageServiceV4;
use crate::store::test_store_create;
use crate::test_support::mint;
use crate::test_support::urc;
use crate::test_support::verifier;

/// One QUIC connection's storage service, with a fragment seeded in `source`.
struct Connection {
    service: StorageServiceV4,
    source: RepositoryId,
    destination: RepositoryId,
    address: Address,
}

impl Connection {
    async fn open() -> Self {
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("test stores");
        let source: RepositoryId = rand::random();
        let destination: RepositoryId = rand::random();
        let (fragment, address, payload) = lore_revision::fragment::generate_random();
        LORE_CONTEXT
            .scope(
                execution,
                immutable_store
                    .clone()
                    .put(source, address, fragment, Some(payload), false),
            )
            .await
            .expect("seed the source fragment");
        let service = StorageServiceV4::new(
            Arc::new(Some(verifier())),
            immutable_store.clone(),
            immutable_store,
            mutable_store,
        );
        Self {
            service,
            source,
            destination,
            address,
        }
    }

    async fn start(&self, repository: RepositoryId, token: &str) -> u32 {
        let response = self
            .service
            .run_request_handler(
                AttributeMap::default().into(),
                ParsedStorageRequestV4::AuthorizeStart {
                    repository,
                    correlation_id: String::new(),
                    auth_token: token.as_bytes().to_vec(),
                },
            )
            .await
            .expect("authorize a session");
        u32::from_le_bytes(response[0][..4].try_into().expect("a session id"))
    }

    async fn stop(&self, session_id: u32) {
        self.service
            .run_request_handler(
                AttributeMap::default().into(),
                ParsedStorageRequestV4::AuthorizeStop { session_id },
            )
            .await
            .expect("stop the session");
    }

    /// Copy the source fragment into the destination through `session_id`.
    async fn copy(&self, session_id: u32) -> Result<(), MessageHandleError> {
        let mut payload = BytesMut::with_capacity(80);
        payload.extend_from_slice(self.source.as_bytes());
        payload.extend_from_slice(self.address.hash.as_bytes());
        payload.extend_from_slice(self.address.context.as_bytes());
        payload.extend_from_slice(self.address.context.as_bytes());
        self.service
            .run_request_handler(
                AttributeMap::default().into(),
                ParsedStorageRequestV4::StorageCommand {
                    session_id,
                    opcode: Command::Copy as u8,
                    payload: payload.freeze(),
                },
            )
            .await
            .map(|_| ())
    }

    fn tokens(&self, source_user: &str, source_ttl: u64) -> (String, String) {
        let (source, _) = mint(source_user, &[(&urc(self.source), &["read"])], source_ttl);
        let (destination, _) = mint(
            "alice",
            &[(&urc(self.destination), &["read", "write"])],
            600,
        );
        (source, destination)
    }
}

fn assert_denied(result: Result<(), MessageHandleError>, why: &str) {
    match result {
        Err(MessageHandleError::AuthorizationFailure(_)) => {}
        other => panic!("{why}: expected an authorization failure, got {other:?}"),
    }
}

#[tokio::test]
async fn copy_reads_its_source_through_a_live_session() {
    let connection = Connection::open().await;
    let (source_token, destination_token) = connection.tokens("alice", 600);
    connection.start(connection.source, &source_token).await;
    let destination = connection
        .start(connection.destination, &destination_token)
        .await;

    connection
        .copy(destination)
        .await
        .expect("a live source session authorizes the copy");
}

#[tokio::test]
async fn copy_source_authorization_ends_when_its_session_stops() {
    let connection = Connection::open().await;
    let (source_token, destination_token) = connection.tokens("alice", 600);
    let source = connection.start(connection.source, &source_token).await;
    let destination = connection
        .start(connection.destination, &destination_token)
        .await;
    connection.copy(destination).await.expect("while live");

    connection.stop(source).await;
    assert_denied(
        connection.copy(destination).await,
        "a stopped session must not authorize a copy from its repository",
    );
}

#[tokio::test]
async fn copy_source_authorization_ends_when_its_token_expires() {
    let connection = Connection::open().await;
    let (source_token, destination_token) = connection.tokens("alice", 2);
    connection.start(connection.source, &source_token).await;
    let destination = connection
        .start(connection.destination, &destination_token)
        .await;
    connection.copy(destination).await.expect("while live");

    tokio::time::sleep(Duration::from_secs(3)).await;
    match connection.copy(destination).await {
        Err(MessageHandleError::SourceSessionExpired) => {}
        other => panic!("an expired source token must refuse the copy as expired: {other:?}"),
    }

    let (renewed, _) = connection.tokens("alice", 600);
    connection.start(connection.source, &renewed).await;
    connection
        .copy(destination)
        .await
        .expect("a renewed source session authorizes the copy again");
}

#[tokio::test]
async fn a_command_in_an_expired_session_is_refused_as_expired() {
    let connection = Connection::open().await;
    let (source_token, _) = connection.tokens("alice", 600);
    connection.start(connection.source, &source_token).await;
    let (destination_token, _) = mint(
        "alice",
        &[(&urc(connection.destination), &["read", "write"])],
        2,
    );
    let destination = connection
        .start(connection.destination, &destination_token)
        .await;
    connection.copy(destination).await.expect("while live");

    tokio::time::sleep(Duration::from_secs(3)).await;
    match connection.copy(destination).await {
        Err(MessageHandleError::SessionExpired) => {}
        other => panic!("a session past its token's expiry must refuse as expired: {other:?}"),
    }
}

#[tokio::test]
async fn copy_source_authorization_belongs_to_the_caller_that_started_it() {
    let connection = Connection::open().await;
    let (source_token, destination_token) = connection.tokens("bob", 600);
    connection.start(connection.source, &source_token).await;
    let destination = connection
        .start(connection.destination, &destination_token)
        .await;

    assert_denied(
        connection.copy(destination).await,
        "another caller's session must not authorize this caller's copy",
    );
}
