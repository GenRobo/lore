// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! A long-lived storage stream stops taking new work once the token that opened it expires.

use std::time::Duration;

use bytes::Bytes;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_proto::lore::storage::v1 as storage_v1;
use lore_proto::lore::storage::v1::storage_service_client::StorageServiceClient;
use lore_proto::lore::storage::v1::storage_service_server::StorageServiceServer;
use lore_revision::lore::RepositoryId;
use lore_transport::grpc::PARTITION_ID_KEY;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;
use tonic::Request;
use tonic::metadata::BinaryMetadataValue;
use tonic::transport::Channel;

use crate::grpc::storage_service::LoreStorageService;
use crate::store::test_store_create;
use crate::test_support::mint;
use crate::test_support::urc;

/// The token's `exp` has whole-second resolution; sleeping past it by a margin keeps the test
/// independent of where in the second it was minted.
const TTL_SECONDS: u64 = 2;

/// The registered storage service on a local port, holding one fragment in `repository` and in
/// `source`. Its interceptor admits every call with one token, as the JWT interceptor does for
/// a token it verified: `permission` on `repository`, read on `source`.
struct StorageOrigin {
    client: StorageServiceClient<Channel>,
    repository: RepositoryId,
    source: RepositoryId,
    address: Address,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

impl StorageOrigin {
    async fn start(permission: &[&str]) -> Self {
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("test stores");
        let repository: RepositoryId = rand::random();
        let source: RepositoryId = rand::random();
        let (fragment, address, payload) = lore_revision::fragment::generate_random();
        LORE_CONTEXT
            .scope(execution, async {
                for partition in [repository, source] {
                    immutable_store
                        .clone()
                        .put(partition, address, fragment, Some(payload.clone()), false)
                        .await
                        .expect("seed a fragment");
                }
            })
            .await;

        let (_, claims) = mint(
            "alice",
            &[(&urc(repository), permission), (&urc(source), &["read"])],
            TTL_SECONDS,
        );
        let service =
            LoreStorageService::new(immutable_store.clone(), immutable_store, mutable_store);
        let server =
            StorageServiceServer::with_interceptor(service, move |mut request: Request<()>| {
                request.extensions_mut().insert(claims.clone());
                Ok(request)
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind storage");
        let url = format!("http://{}", listener.local_addr().expect("storage address"));
        let (shutdown, stopped) = tokio::sync::oneshot::channel::<()>();
        let serve = tonic::transport::Server::builder()
            .add_service(server)
            .serve_with_incoming_shutdown(
                tonic::transport::server::TcpIncoming::from(listener),
                async {
                    let _ = stopped.await;
                },
            );
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(serve);
        let channel = tonic::transport::Endpoint::from_shared(url)
            .expect("storage url")
            .connect()
            .await
            .expect("connect to storage");
        Self {
            client: StorageServiceClient::new(channel),
            repository,
            source,
            address,
            _shutdown: shutdown,
        }
    }

    fn request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        request.metadata_mut().insert_bin(
            PARTITION_ID_KEY,
            BinaryMetadataValue::from_bytes(self.repository.data()),
        );
        request
    }

    fn put_request(&self) -> storage_v1::PutRequest {
        let (fragment, address, payload) = lore_revision::fragment::generate_random();
        storage_v1::PutRequest {
            address: Some(address.into()),
            fragment: Some(fragment.into()),
            payload: Some(payload),
        }
    }

    fn copy_request(&self) -> storage_v1::CopyRequest {
        storage_v1::CopyRequest {
            source_repository_id: self.source.into(),
            source_address: Some(self.address.into()),
            target_context: Bytes::new(),
        }
    }
}

async fn outlive_the_token() {
    tokio::time::sleep(Duration::from_secs(TTL_SECONDS + 1)).await;
}

#[tokio::test]
async fn a_get_stream_refuses_new_work_after_its_token_expires() {
    let origin = StorageOrigin::start(&["read"]).await;
    let (requests, stream) = mpsc::channel(4);
    let mut responses = origin
        .client
        .clone()
        .get(origin.request(ReceiverStream::new(stream)))
        .await
        .expect("open a Get stream")
        .into_inner();

    requests.send(origin.address.into()).await.expect("send");
    let live = responses
        .message()
        .await
        .expect("a Get while the token is live")
        .expect("a response");
    assert_eq!(live.address, Some(origin.address.into()));

    outlive_the_token().await;
    requests.send(origin.address.into()).await.expect("send");
    let status = responses
        .message()
        .await
        .expect_err("a Get sent after expiry must be refused");
    assert_eq!(status.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn a_get_metadata_stream_refuses_new_work_after_its_token_expires() {
    let origin = StorageOrigin::start(&["read"]).await;
    let (requests, stream) = mpsc::channel(4);
    let mut responses = origin
        .client
        .clone()
        .get_metadata(origin.request(ReceiverStream::new(stream)))
        .await
        .expect("open a GetMetadata stream")
        .into_inner();

    requests.send(origin.address.into()).await.expect("send");
    let live = responses
        .message()
        .await
        .expect("a GetMetadata while the token is live")
        .expect("a response");
    assert_eq!(live.address, Some(origin.address.into()));

    outlive_the_token().await;
    requests.send(origin.address.into()).await.expect("send");
    let status = responses
        .message()
        .await
        .expect_err("a GetMetadata sent after expiry must be refused");
    assert_eq!(status.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn a_put_stream_refuses_new_work_after_its_token_expires() {
    let origin = StorageOrigin::start(&["read", "write"]).await;
    let (requests, stream) = mpsc::channel(4);
    let mut responses = origin
        .client
        .clone()
        .put(origin.request(ReceiverStream::new(stream)))
        .await
        .expect("open a Put stream")
        .into_inner();

    requests.send(origin.put_request()).await.expect("send");
    responses
        .message()
        .await
        .expect("a Put while the token is live")
        .expect("a response");

    outlive_the_token().await;
    requests.send(origin.put_request()).await.expect("send");
    let status = responses
        .message()
        .await
        .expect_err("a Put sent after expiry must be refused");
    assert_eq!(status.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn a_copy_stream_refuses_new_work_after_its_token_expires() {
    let origin = StorageOrigin::start(&["read", "write"]).await;
    let (requests, stream) = mpsc::channel(4);
    let mut responses = origin
        .client
        .clone()
        .copy(origin.request(ReceiverStream::new(stream)))
        .await
        .expect("open a Copy stream")
        .into_inner();

    requests.send(origin.copy_request()).await.expect("send");
    responses
        .message()
        .await
        .expect("a Copy while the token is live")
        .expect("a response");

    outlive_the_token().await;
    requests.send(origin.copy_request()).await.expect("send");
    let status = responses
        .message()
        .await
        .expect_err("a Copy sent after expiry must be refused");
    assert_eq!(status.code(), Code::PermissionDenied);
}
