// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use lore_proto::lore::storage::v1 as storage_v1;
use lore_proto::lore::storage::v1::storage_service_server::StorageService as StorageServiceV1;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::Streaming;

use super::copy;
use super::copy::CopyResponseStream;
use super::get;
use super::get::GetResponseStream;
use super::get_metadata;
use super::mutable_compare_and_swap;
use super::mutable_load;
use super::mutable_store;
use super::put;
use super::put::PutResponseStream;
use super::query;
use super::verify;
use crate::grpc::storage_service::LoreStorageService;

#[tonic::async_trait]
impl StorageServiceV1 for LoreStorageService {
    type GetStream = GetResponseStream;

    async fn get(
        &self,
        request: Request<Streaming<lore_proto::lore::model::v1::Address>>,
    ) -> Result<Response<Self::GetStream>, Status> {
        get::handler(request, self.immutable_store().clone(), self).await
    }

    type GetMetadataStream = GetResponseStream;

    async fn get_metadata(
        &self,
        request: Request<Streaming<lore_proto::lore::model::v1::Address>>,
    ) -> Result<Response<Self::GetMetadataStream>, Status> {
        get_metadata::handler(request, self.immutable_store().clone(), self).await
    }

    type PutStream = PutResponseStream;

    async fn put(
        &self,
        request: Request<Streaming<storage_v1::PutRequest>>,
    ) -> Result<Response<Self::PutStream>, Status> {
        crate::grpc::verify_write(&request)?;
        put::handler(request, self.immutable_store().clone(), self).await
    }

    async fn query(
        &self,
        request: Request<storage_v1::QueryRequest>,
    ) -> Result<Response<storage_v1::QueryResponse>, Status> {
        query::handler(request, self.immutable_store().clone()).await
    }

    type CopyStream = CopyResponseStream;

    async fn copy(
        &self,
        request: Request<Streaming<storage_v1::CopyRequest>>,
    ) -> Result<Response<Self::CopyStream>, Status> {
        crate::grpc::verify_write(&request)?;
        copy::handler(request, self.immutable_store().clone(), self).await
    }

    async fn verify(
        &self,
        request: Request<storage_v1::VerifyRequest>,
    ) -> Result<Response<storage_v1::VerifyResponse>, Status> {
        verify::handler(request, self.local_immutable_store().clone()).await
    }

    async fn mutable_load(
        &self,
        request: Request<storage_v1::MutableLoadRequest>,
    ) -> Result<Response<storage_v1::MutableLoadResponse>, Status> {
        mutable_load::handler(request, self.mutable_store().clone()).await
    }

    async fn mutable_store(
        &self,
        request: Request<storage_v1::MutableStoreRequest>,
    ) -> Result<Response<storage_v1::MutableStoreResponse>, Status> {
        crate::grpc::verify_write(&request)?;
        let key_type = request.get_ref().key_type;
        crate::grpc::verify_raw_mutable_write(&request, key_type)?;
        mutable_store::handler(request, self.mutable_store().clone()).await
    }

    async fn mutable_compare_and_swap(
        &self,
        request: Request<storage_v1::MutableCompareAndSwapRequest>,
    ) -> Result<Response<storage_v1::MutableCompareAndSwapResponse>, Status> {
        crate::grpc::verify_write(&request)?;
        let key_type = request.get_ref().key_type;
        crate::grpc::verify_raw_mutable_write(&request, key_type)?;
        mutable_compare_and_swap::handler(request, self.mutable_store().clone()).await
    }
}

#[cfg(test)]
mod tests {
    use lore_base::runtime::LORE_CONTEXT;
    use lore_base::runtime::runtime;
    use lore_base::types::Context;
    use lore_base::types::Hash;
    use lore_base::types::KeyType;
    use lore_proto::lore::storage::v1 as storage_v1;
    use lore_proto::lore::storage::v1::storage_service_server::StorageService as StorageServiceV1;
    use lore_proto::lore::storage::v1::storage_service_server::StorageServiceServer;
    use rand::random;
    use tonic::Code;
    use zerocopy::IntoBytes;

    use crate::auth::jwt::AuthorizationToken;
    use crate::auth::jwt::ResourcePermission;
    use crate::grpc::storage::v1::test_utils::make_request_with_metadata;
    use crate::grpc::storage_service::LoreStorageService;
    use crate::store::test_store_create;

    fn bytes(hash: Hash) -> bytes::Bytes {
        bytes::Bytes::copy_from_slice(hash.as_bytes())
    }

    /// #3304: a writer must not move a branch head (or any record a checked RPC owns) through
    /// the raw store RPCs. With a write-capable token on the request, every checked key type
    /// is refused by `MutableStore` and `MutableCompareAndSwap`, and the protected value stays;
    /// writing the same key relabelled `Untyped` or `Instance` lands on a different stored key
    /// in the local store, so the protected value still stays. Untyped and instance keys stay
    /// writable.
    #[tokio::test]
    async fn raw_mutable_writes_of_checked_key_types_are_refused_under_auth() {
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create store");
        let repository = random::<Context>();
        // The local store keeps key types apart, as the server records at build.
        crate::auth::set_mutable_key_types_isolated(true);
        let protected = mutable_store.clone();

        runtime()
            .spawn(LORE_CONTEXT.scope(execution, async move {
                let service = LoreStorageService::new(
                    immutable_store.clone(),
                    immutable_store,
                    mutable_store,
                );
                let writer = AuthorizationToken {
                    user_id: "writer".to_string(),
                    resources: Some(vec![ResourcePermission {
                        resource_id: format!("urc-{repository}"),
                        permission: vec!["read".to_string(), "write".to_string()],
                    }]),
                    ..Default::default()
                };
                let load = |key: Hash, key_type: KeyType| {
                    let request = make_request_with_metadata(
                        storage_v1::MutableLoadRequest {
                            key: bytes(key),
                            key_type: key_type as u32,
                        },
                        repository,
                        "test-raw-mutable",
                    );
                    StorageServiceV1::mutable_load(&service, request)
                };
                let store = |key: Hash, value: Hash, key_type: KeyType| {
                    let mut request = make_request_with_metadata(
                        storage_v1::MutableStoreRequest {
                            key: bytes(key),
                            value: bytes(value),
                            key_type: key_type as u32,
                        },
                        repository,
                        "test-raw-mutable",
                    );
                    request.extensions_mut().insert(writer.clone());
                    request
                };
                let cas = |key: Hash, value: Hash, key_type: KeyType| {
                    let mut request = make_request_with_metadata(
                        storage_v1::MutableCompareAndSwapRequest {
                            key: bytes(key),
                            expected: bytes(Hash::default()),
                            value: bytes(value),
                            key_type: key_type as u32,
                        },
                        repository,
                        "test-raw-mutable",
                    );
                    request.extensions_mut().insert(writer.clone());
                    request
                };

                for key_type in [
                    KeyType::BranchLatestPointer,
                    KeyType::BranchMetadata,
                    KeyType::BranchId,
                    KeyType::RepositoryMetadata,
                    KeyType::RepositoryId,
                ] {
                    let key = random::<Hash>();
                    let original = random::<Hash>();
                    protected
                        .clone()
                        .store(repository.into(), key, original, key_type)
                        .await
                        .expect("seed the protected key");
                    let current = || async {
                        load(key, key_type)
                            .await
                            .expect("protected key")
                            .into_inner()
                            .value
                    };

                    let refused =
                        StorageServiceV1::mutable_store(&service, store(key, random(), key_type))
                            .await
                            .expect_err("raw store of a checked key type must be refused");
                    assert_eq!(refused.code(), Code::PermissionDenied, "{key_type:?}");

                    let refused = StorageServiceV1::mutable_compare_and_swap(
                        &service,
                        cas(key, random(), key_type),
                    )
                    .await
                    .expect_err("raw CAS of a checked key type must be refused");
                    assert_eq!(refused.code(), Code::PermissionDenied, "{key_type:?}");
                    assert_eq!(current().await.as_ref(), original.as_bytes(), "{key_type:?}");

                    // The same key relabelled as a type no checked RPC owns is accepted, and
                    // lands beside the protected key, never on it.
                    for alias in [KeyType::Untyped, KeyType::Instance] {
                        StorageServiceV1::mutable_store(&service, store(key, random(), alias))
                            .await
                            .expect("an unprotected type is writable");
                        assert_eq!(
                            current().await.as_ref(),
                            original.as_bytes(),
                            "{key_type:?} relabelled {alias:?}"
                        );
                    }
                }

                for key_type in [KeyType::Untyped, KeyType::Instance] {
                    let key = random::<Hash>();
                    let value = random::<Hash>();
                    StorageServiceV1::mutable_store(&service, store(key, value, key_type))
                        .await
                        .expect("a writer stores keys no checked RPC owns");
                    StorageServiceV1::mutable_compare_and_swap(
                        &service,
                        cas(random(), value, key_type),
                    )
                    .await
                    .expect("a writer swaps keys no checked RPC owns");
                    let loaded = load(key, key_type)
                        .await
                        .expect("stored")
                        .into_inner()
                        .value;
                    assert_eq!(loaded.as_ref(), value.as_bytes(), "{key_type:?}");
                }
            }))
            .await
            .expect("test task");
    }

    /// Compile-time check that `LoreStorageService` fully implements the generated
    /// `StorageService` trait — wrapping it in `StorageServiceServer` requires the
    /// trait bound to hold. Per-handler behavior is tested in each handler module.
    #[allow(dead_code)]
    fn assert_implements_trait(
        service: LoreStorageService,
    ) -> StorageServiceServer<LoreStorageService> {
        StorageServiceServer::new(service)
    }
}
