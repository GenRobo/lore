// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Tenant isolation through the registered `RepositoryService`: a caller learns nothing about a
//! repository its grants do not cover.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Hash;
use lore_proto::lore::repository::v1::RepositoryMetadataGetRequest;
use lore_proto::lore::repository::v1::RepositoryMetadataSetRequest;
use lore_proto::lore::repository::v1::repository_service_server::RepositoryService;
use lore_revision::environment::EnvironmentConfig;
use lore_revision::interface::ExecutionContext;
use lore_revision::lore::RepositoryId;
use lore_revision::repository;
use lore_revision::repository::RepositoryContext;
use lore_transport::Endpoint;
use tonic::Code;
use tonic::Status;

use super::service::LoreRepositoryV1Service;
use crate::auth::jwt::AuthorizationToken;
use crate::hooks::HookDispatcher;
use crate::store::test_store_create;
use crate::test_support::as_caller;
use crate::test_support::assert_same_status;
use crate::test_support::mint;
use crate::test_support::seed_repository;
use crate::test_support::urc;

/// Never dialed: the metadata handlers authorize from the caller's token.
const UNDIALED_BROKER: &str = "http://127.0.0.1:9";

type Caller = (String, AuthorizationToken);

/// An origin holding a hidden customer repository R and the caller's own repository A.
struct Origin {
    service: LoreRepositoryV1Service,
    execution: Arc<ExecutionContext>,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    hidden: RepositoryId,
    hidden_metadata: Hash,
    own: RepositoryId,
}

impl Origin {
    async fn new(auth_url: Option<&str>) -> Self {
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("test stores");
        let hidden: RepositoryId = rand::random();
        let own: RepositoryId = rand::random();
        let hidden_metadata = LORE_CONTEXT
            .scope(execution.clone(), async {
                seed_repository(
                    immutable_store.clone(),
                    mutable_store.clone(),
                    own,
                    "tenant-a",
                )
                .await;
                seed_repository(
                    immutable_store.clone(),
                    mutable_store.clone(),
                    hidden,
                    "customer-secret",
                )
                .await
            })
            .await;
        let environment = EnvironmentConfig {
            endpoint: auth_url.map(|url| Endpoint {
                auth_url: Some(url.to_string()),
                ..Default::default()
            }),
            config: None,
        };
        let service = LoreRepositoryV1Service::new(
            environment,
            immutable_store.clone(),
            mutable_store.clone(),
            Arc::new(HookDispatcher::empty()),
            Duration::from_secs(30),
        );
        Self {
            service,
            execution,
            immutable_store,
            mutable_store,
            hidden,
            hidden_metadata,
            own,
        }
    }

    async fn scoped<T>(&self, work: impl Future<Output = T>) -> T {
        LORE_CONTEXT.scope(self.execution.clone(), work).await
    }

    async fn stored_metadata(&self, id: RepositoryId) -> Option<Hash> {
        let repository = Arc::new(RepositoryContext::new_server_context(
            self.immutable_store.clone(),
            self.mutable_store.clone(),
            id,
        ));
        self.scoped(repository::metadata_hash(repository))
            .await
            .ok()
    }

    async fn metadata_get(
        &self,
        id: RepositoryId,
        caller: Option<&Caller>,
    ) -> Result<Hash, Status> {
        let request = as_caller(RepositoryMetadataGetRequest { id: id.into() }, caller);
        self.service
            .repository_metadata_get(request)
            .await
            .map(|response| response.into_inner().metadata.into())
    }

    async fn metadata_set(
        &self,
        id: RepositoryId,
        expected: Hash,
        updated: Hash,
        caller: Option<&Caller>,
    ) -> Result<Hash, Status> {
        let request = as_caller(
            RepositoryMetadataSetRequest {
                id: id.into(),
                expected: expected.into(),
                updated: updated.into(),
            },
            caller,
        );
        self.service
            .repository_metadata_set(request)
            .await
            .map(|response| response.into_inner().metadata.into())
    }
}

#[tokio::test]
async fn metadata_get_denies_a_foreign_repository_like_an_absent_one() {
    let origin = Origin::new(Some(UNDIALED_BROKER)).await;
    let caller = mint("alice", &[(&urc(origin.own), &["read", "write"])], 600);

    let hidden = origin.metadata_get(origin.hidden, Some(&caller)).await;
    let absent = origin.metadata_get(rand::random(), Some(&caller)).await;

    let hidden = hidden.expect_err("a token scoped to A must not read R's metadata pointer");
    let absent = absent.expect_err("an absent repository has no metadata");
    assert_eq!(hidden.code(), Code::PermissionDenied);
    assert_same_status(&hidden, &absent);
}

#[tokio::test]
async fn metadata_get_serves_a_granted_repository() {
    let origin = Origin::new(Some(UNDIALED_BROKER)).await;
    let caller = mint("bob", &[(&urc(origin.hidden), &["read"])], 600);

    let metadata = origin
        .metadata_get(origin.hidden, Some(&caller))
        .await
        .expect("a read grant on R reads R's metadata pointer");
    assert_eq!(metadata, origin.hidden_metadata);
}

#[tokio::test]
async fn metadata_set_requires_write_on_the_body_repository() {
    let origin = Origin::new(Some(UNDIALED_BROKER)).await;
    let foreign = mint("alice", &[(&urc(origin.own), &["read", "write"])], 600);
    let read_only = mint("bob", &[(&urc(origin.hidden), &["read"])], 600);

    let denied = origin
        .metadata_set(
            origin.hidden,
            Hash::default(),
            origin.hidden_metadata,
            Some(&foreign),
        )
        .await
        .expect_err("a token scoped to A must not reach R's metadata CAS");
    let absent = origin
        .metadata_set(
            rand::random(),
            Hash::default(),
            origin.hidden_metadata,
            Some(&foreign),
        )
        .await
        .expect_err("an absent repository has no metadata to set");
    assert_eq!(denied.code(), Code::PermissionDenied);
    assert_same_status(&denied, &absent);

    let read_only = origin
        .metadata_set(
            origin.hidden,
            Hash::default(),
            origin.hidden_metadata,
            Some(&read_only),
        )
        .await
        .expect_err("a read grant must not set metadata");
    assert_eq!(read_only.code(), Code::PermissionDenied);

    assert_eq!(
        origin.stored_metadata(origin.hidden).await,
        Some(origin.hidden_metadata),
        "R's metadata pointer must be untouched"
    );
}

#[tokio::test]
async fn metadata_set_allows_a_write_grant() {
    let origin = Origin::new(Some(UNDIALED_BROKER)).await;
    let caller = mint("carol", &[(&urc(origin.hidden), &["read", "write"])], 600);

    let metadata = origin
        .metadata_set(
            origin.hidden,
            origin.hidden_metadata,
            origin.hidden_metadata,
            Some(&caller),
        )
        .await
        .expect("a write grant on R sets R's metadata");
    assert_eq!(metadata, origin.hidden_metadata);
}

#[tokio::test]
async fn metadata_handlers_fail_closed_without_a_token_under_a_broker() {
    let origin = Origin::new(Some(UNDIALED_BROKER)).await;

    let get = origin
        .metadata_get(origin.hidden, None)
        .await
        .expect_err("a tokenless read must be refused when authorization is configured");
    let set = origin
        .metadata_set(origin.hidden, Hash::default(), origin.hidden_metadata, None)
        .await
        .expect_err("a tokenless write must be refused when authorization is configured");
    assert_eq!(get.code(), Code::PermissionDenied);
    assert_eq!(set.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn metadata_get_stays_open_on_an_auth_off_origin() {
    let origin = Origin::new(None).await;

    let metadata = origin
        .metadata_get(origin.hidden, None)
        .await
        .expect("an auth-off origin serves every client");
    assert_eq!(metadata, origin.hidden_metadata);
}
