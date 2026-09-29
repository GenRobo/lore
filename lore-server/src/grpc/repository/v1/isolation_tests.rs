// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Tenant isolation through the registered `RepositoryService`: a caller learns nothing about a
//! repository its grants do not cover.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Context;
use lore_base::types::Hash;
use lore_proto::lore::model::v1 as model;
use lore_proto::lore::repository::v1::RepositoryCreateRequest;
use lore_proto::lore::repository::v1::RepositoryDeleteRequest;
use lore_proto::lore::repository::v1::RepositoryGetRequest;
use lore_proto::lore::repository::v1::RepositoryListRequest;
use lore_proto::lore::repository::v1::RepositoryMetadataGetRequest;
use lore_proto::lore::repository::v1::RepositoryMetadataSetRequest;
use lore_proto::lore::repository::v1::repository_get_request::Query;
use lore_proto::lore::repository::v1::repository_service_server::RepositoryService;
use lore_revision::environment::EnvironmentConfig;
use lore_revision::interface::ExecutionContext;
use lore_revision::lore::RepositoryId;
use lore_revision::repository;
use lore_revision::repository::RepositoryContext;
use lore_transport::Endpoint;
use tokio_stream::StreamExt;
use tonic::Code;
use tonic::Status;

use super::service::LoreRepositoryV1Service;
use crate::auth::jwt::AuthorizationToken;
use crate::hooks::HookDispatcher;
use crate::store::test_store_create;
use crate::test_support::FakeBroker;
use crate::test_support::as_caller;
use crate::test_support::assert_same_status;
use crate::test_support::broker_grant;
use crate::test_support::mint;
use crate::test_support::seed_repository;
use crate::test_support::urc;

/// Never dialed: the metadata handlers authorize from the caller's token.
const UNDIALED_BROKER: &str = "http://127.0.0.1:9";

type Caller = (String, AuthorizationToken);

fn repository_id(repository: Option<model::Repository>) -> RepositoryId {
    Context::from(repository.expect("a repository record").id).into()
}

fn by_id(id: RepositoryId) -> Query {
    Query::Id(id.into())
}

/// What a caller gets for an id it may not see, identical to what an absent id gets.
fn assert_not_found(status: &Status, id: RepositoryId) {
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
    assert_eq!(status.message(), format!("Repository {id} not found"));
}

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

    /// An origin that delegates repository authorization to a broker double.
    async fn brokered() -> (Self, FakeBroker) {
        let broker = FakeBroker::start().await;
        (Self::new(Some(&broker.url)).await, broker)
    }

    async fn scoped<T>(&self, work: impl Future<Output = T>) -> T {
        LORE_CONTEXT.scope(self.execution.clone(), work).await
    }

    async fn get(&self, query: Query, caller: Option<&Caller>) -> Result<RepositoryId, Status> {
        let request = as_caller(RepositoryGetRequest { query: Some(query) }, caller);
        let response = self.service.repository_get(request).await?.into_inner();
        Ok(repository_id(response.repository))
    }

    async fn create(
        &self,
        id: RepositoryId,
        name: &str,
        caller: Option<&Caller>,
    ) -> Result<model::Repository, Status> {
        let request = as_caller(
            RepositoryCreateRequest {
                id: id.into(),
                name: name.to_string(),
                description: String::new(),
                default_branch_id: rand::random::<Context>().into(),
                default_branch_name: "main".to_string(),
                creator: None,
            },
            caller,
        );
        let response = self.service.repository_create(request).await?.into_inner();
        Ok(response.repository.expect("a repository record"))
    }

    async fn delete(
        &self,
        id: RepositoryId,
        caller: Option<&Caller>,
    ) -> Result<model::Repository, Status> {
        let request = as_caller(RepositoryDeleteRequest { id: id.into() }, caller);
        let response = self.service.repository_delete(request).await?.into_inner();
        Ok(response.repository.expect("a repository record"))
    }

    async fn is_deleted(&self, id: RepositoryId) -> bool {
        self.stored_metadata(id)
            .await
            .is_none_or(|metadata| metadata.is_zero())
    }

    async fn list(&self, caller: Option<&Caller>) -> Result<Vec<RepositoryId>, Status> {
        let request = as_caller(RepositoryListRequest { creator: None }, caller);
        let mut stream = self.service.repository_list(request).await?.into_inner();
        let mut listed = Vec::new();
        while let Some(item) = stream.next().await {
            listed.push(repository_id(item?.repository));
        }
        listed.sort_by_key(ToString::to_string);
        Ok(listed)
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

fn sorted(mut ids: Vec<RepositoryId>) -> Vec<RepositoryId> {
    ids.sort_by_key(ToString::to_string);
    ids
}

#[tokio::test]
async fn get_ignores_broker_entries_without_read_or_write() {
    let (origin, broker) = Origin::brokered().await;
    let caller = mint("alice", &[(&urc(origin.own), &["read"])], 600);

    for permission in [&[][..], &["obliterate"][..], &["owner", "migrate"][..]] {
        broker.state().check_answer = Some(vec![broker_grant(&urc(origin.hidden), permission)]);
        let status = origin
            .get(by_id(origin.hidden), Some(&caller))
            .await
            .expect_err("an entry without read or write must not reveal R");
        assert_not_found(&status, origin.hidden);
    }
}

#[tokio::test]
async fn get_matches_the_repository_entry_anywhere_in_the_answer() {
    let (origin, broker) = Origin::brokered().await;
    let caller = mint("alice", &[(&urc(origin.own), &["read"])], 600);
    broker.state().check_answer = Some(vec![
        broker_grant(&urc(rand::random()), &["read"]),
        broker_grant(&urc(origin.hidden), &["read"]),
    ]);

    let found = origin
        .get(by_id(origin.hidden), Some(&caller))
        .await
        .expect("a readable entry for R grants it wherever it appears");
    assert_eq!(found, origin.hidden);
}

#[tokio::test]
async fn get_hides_an_ungranted_repository_like_an_absent_one() {
    let (origin, _broker) = Origin::brokered().await;
    let caller = mint("alice", &[(&urc(origin.own), &["read"])], 600);
    let absent: RepositoryId = rand::random();

    let hidden = origin
        .get(by_id(origin.hidden), Some(&caller))
        .await
        .expect_err("R is outside the caller's grants");
    let missing = origin
        .get(by_id(absent), Some(&caller))
        .await
        .expect_err("an absent repository");
    assert_not_found(&hidden, origin.hidden);
    assert_not_found(&missing, absent);

    let own = origin
        .get(by_id(origin.own), Some(&caller))
        .await
        .expect("the caller's own repository");
    assert_eq!(own, origin.own);
}

#[tokio::test]
async fn list_expands_a_readable_wildcard_to_every_repository() {
    let (origin, _broker) = Origin::brokered().await;
    let staff = mint("staff", &[("urc-*", &["read"])], 600);

    let listed = origin.list(Some(&staff)).await.expect("wildcard listing");
    assert_eq!(listed, sorted(vec![origin.hidden, origin.own]));
}

#[tokio::test]
async fn list_shows_only_readable_well_formed_grants() {
    let (origin, broker) = Origin::brokered().await;
    let caller = mint("alice", &[(&urc(origin.own), &["read"])], 600);
    broker.state().lookup_answer = Some(vec![
        broker_grant(&urc(origin.hidden), &[]),
        broker_grant(&urc(origin.hidden), &["obliterate"]),
        broker_grant("urc-*", &[]),
        broker_grant("urc-*", &["migrate"]),
        broker_grant("urc-customer-secret", &["read"]),
        broker_grant(&urc(origin.own), &["read"]),
    ]);

    let listed = origin.list(Some(&caller)).await.expect("scoped listing");
    assert_eq!(listed, vec![origin.own]);
}

#[tokio::test]
async fn list_ignores_a_broker_wildcard_the_caller_does_not_hold() {
    let (origin, broker) = Origin::brokered().await;
    let caller = mint("alice", &[(&urc(origin.own), &["read"])], 600);

    // The pre-hotfix broker answered every caller with urc-* read,write.
    broker.state().lookup_answer = Some(vec![broker_grant("urc-*", &["read", "write"])]);
    let listed = origin.list(Some(&caller)).await.expect("scoped listing");
    assert!(
        listed.is_empty(),
        "a blanket urc-* must not list R: {listed:?}"
    );

    broker.state().lookup_answer = Some(vec![
        broker_grant("urc-*", &["read", "write"]),
        broker_grant(&urc(origin.own), &["read"]),
    ]);
    let listed = origin.list(Some(&caller)).await.expect("scoped listing");
    assert_eq!(listed, vec![origin.own]);
}

#[tokio::test]
async fn list_never_widens_an_empty_or_failed_answer() {
    let (origin, _broker) = Origin::brokered().await;

    let nothing = mint("nobody", &[], 600);
    let listed = origin.list(Some(&nothing)).await.expect("empty listing");
    assert!(listed.is_empty(), "no grants must list nothing: {listed:?}");

    let forged = ("not-a-token".to_string(), nothing.1.clone());
    let status = origin
        .list(Some(&forged))
        .await
        .expect_err("a broker that rejects the caller must not produce a listing");
    assert_eq!(status.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn create_denies_a_caller_without_write_before_any_lookup() {
    let (origin, broker) = Origin::brokered().await;
    let caller = mint("alice", &[(&urc(origin.own), &["read", "write"])], 600);
    let fresh: RepositoryId = rand::random();

    // A taken name, a taken id, both (a retry of R's create), and neither.
    let attempts = [
        (fresh, "customer-secret"),
        (origin.hidden, "probe-name"),
        (origin.hidden, "customer-secret"),
        (fresh, "fresh-name"),
    ];
    let mut denials = Vec::new();
    for (id, name) in attempts {
        let status = origin
            .create(id, name, Some(&caller))
            .await
            .expect_err("creating needs a write grant on the new id");
        assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
        denials.push(status);
    }
    for denial in &denials[1..] {
        assert_same_status(&denials[0], denial);
    }

    assert!(
        broker.state().created.is_empty(),
        "no broker resource created"
    );
    assert_eq!(origin.stored_metadata(fresh).await, None);
    assert_eq!(
        origin.stored_metadata(origin.hidden).await,
        Some(origin.hidden_metadata)
    );
}

#[tokio::test]
async fn create_collisions_never_name_another_repository() {
    let (origin, _broker) = Origin::brokered().await;
    let staff = mint("staff", &[("urc-*", &["read", "write"])], 600);
    let hidden_id = origin.hidden.to_string();

    let taken_name = origin
        .create(rand::random(), "customer-secret", Some(&staff))
        .await
        .expect_err("the name belongs to R");
    let taken_id = origin
        .create(origin.hidden, "probe-name", Some(&staff))
        .await
        .expect_err("the id belongs to R");
    for status in [&taken_name, &taken_id] {
        assert_eq!(status.code(), Code::AlreadyExists, "{status:?}");
        assert!(!status.message().contains(&hidden_id), "{status:?}");
        assert!(!status.message().contains("customer-secret"), "{status:?}");
    }
}

#[tokio::test]
async fn create_retry_returns_the_existing_record_only_to_its_writer() {
    let (origin, _broker) = Origin::brokered().await;
    let writer = mint("owner", &[(&urc(origin.hidden), &["read", "write"])], 600);
    let outsider = mint("alice", &[(&urc(origin.own), &["read", "write"])], 600);

    let record = origin
        .create(origin.hidden, "customer-secret", Some(&writer))
        .await
        .expect("an idempotent retry by R's writer");
    assert_eq!(repository_id(Some(record.clone())), origin.hidden);
    assert_eq!(record.name, "customer-secret");

    let status = origin
        .create(origin.hidden, "customer-secret", Some(&outsider))
        .await
        .expect_err("a retry must not hand R's record to a caller without write on R");
    assert_eq!(status.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn create_with_write_creates_the_repository() {
    let (origin, broker) = Origin::brokered().await;
    let fresh: RepositoryId = rand::random();
    let writer = mint("owner", &[(&urc(fresh), &["read", "write"])], 600);

    let record = origin
        .create(fresh, "fresh-name", Some(&writer))
        .await
        .expect("a write grant on the new id creates it");
    assert_eq!(repository_id(Some(record)), fresh);
    assert_eq!(broker.state().created, vec![urc(fresh)]);
    let found = origin
        .get(by_id(fresh), Some(&writer))
        .await
        .expect("the creator sees the new repository");
    assert_eq!(found, fresh);
}

#[tokio::test]
async fn delete_hides_an_invisible_repository_as_not_found() {
    let (origin, broker) = Origin::brokered().await;
    let absent: RepositoryId = rand::random();
    let outsider = mint("alice", &[(&urc(origin.own), &["read", "write"])], 600);
    let outside_admin = mint("root", &[(&urc(origin.own), &["read", "write"])], 600);
    broker.state().admins.insert("root".to_string());

    for caller in [&outsider, &outside_admin] {
        let hidden = origin
            .delete(origin.hidden, Some(caller))
            .await
            .expect_err("R is outside the caller's grants");
        let missing = origin
            .delete(absent, Some(caller))
            .await
            .expect_err("an absent repository");
        assert_not_found(&hidden, origin.hidden);
        assert_not_found(&missing, absent);
    }

    assert!(
        broker.state().deleted.is_empty(),
        "no broker delete without visibility"
    );
    assert_eq!(
        origin.stored_metadata(origin.hidden).await,
        Some(origin.hidden_metadata)
    );
}

#[tokio::test]
async fn delete_by_a_visible_non_admin_is_refused_by_the_broker() {
    let (origin, broker) = Origin::brokered().await;
    let reader = mint("bob", &[(&urc(origin.hidden), &["read", "write"])], 600);

    let status = origin
        .delete(origin.hidden, Some(&reader))
        .await
        .expect_err("deleting is for admins");
    assert_eq!(status.code(), Code::PermissionDenied);
    assert!(broker.state().deleted.is_empty());
    assert!(!origin.is_deleted(origin.hidden).await);
}

#[tokio::test]
async fn delete_by_a_visible_admin_deletes_and_returns_the_record() {
    let (origin, broker) = Origin::brokered().await;
    let admin = mint("carol", &[(&urc(origin.hidden), &["read"])], 600);
    broker.state().admins.insert("carol".to_string());

    let record = origin
        .delete(origin.hidden, Some(&admin))
        .await
        .expect("an admin that can see R deletes it");
    assert_eq!(repository_id(Some(record)), origin.hidden);
    assert_eq!(broker.state().deleted, vec![urc(origin.hidden)]);
    assert!(origin.is_deleted(origin.hidden).await);
}
