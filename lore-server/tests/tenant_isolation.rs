// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Tenant isolation end to end: requests carrying GRID-shaped tokens travel through the
//! production gRPC router and its JWT interceptors to the registered services, and a caller
//! learns nothing about a repository its own grants do not cover.

mod support;

use std::time::Duration;

use lore_base::types::Context;
use lore_base::types::Hash;
use lore_proto::lore::notification::SubscribeRequest;
use lore_proto::lore::notification::notification_service_client::NotificationServiceClient;
use lore_proto::lore::repository::v1::RepositoryCreateRequest;
use lore_proto::lore::repository::v1::RepositoryDeleteRequest;
use lore_proto::lore::repository::v1::RepositoryGetRequest;
use lore_proto::lore::repository::v1::RepositoryListRequest;
use lore_proto::lore::repository::v1::RepositoryMetadataGetRequest;
use lore_proto::lore::repository::v1::RepositoryMetadataSetRequest;
use lore_proto::lore::repository::v1::repository_get_request::Query;
use lore_proto::lore::repository::v1::repository_service_client::RepositoryServiceClient;
use lore_revision::lore::RepositoryId;
use lore_revision::notification::NotificationSender as _;
use lore_transport::grpc::PARTITION_ID_KEY;
use support::Origin;
use support::broker_grant;
use support::deploy;
use support::developer;
use support::grid_token;
use support::identity_without_resources;
use support::urc;
use tokio_stream::StreamExt;
use tonic::Code;
use tonic::Request;
use tonic::Status;
use tonic::metadata::BinaryMetadataValue;
use tonic::transport::Channel;

const CREATE_DENIED: &str = "Failed to create repository, permission denied";
const DELETE_DENIED: &str = "Delete resource denied";

fn signed<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("bearer header"),
    );
    request
}

fn assert_not_found(status: &Status, id: RepositoryId) {
    assert_eq!(status.code(), Code::NotFound, "{status:?}");
    assert_eq!(status.message(), format!("Repository {id} not found"));
}

/// What the pre-hotfix broker answered every caller: read and write on everything.
fn blanket(repositories: &[RepositoryId]) -> Vec<lore_proto::auth::ResourcePermission> {
    let mut answer = vec![broker_grant("urc-*", &["read", "write"])];
    answer.extend(
        repositories
            .iter()
            .map(|id| broker_grant(&urc(*id), &["read", "write"])),
    );
    answer
}

/// An origin holding a hidden customer repository R and tenant A's own repository.
struct Tenancy {
    origin: Origin,
    hidden: RepositoryId,
    hidden_metadata: Hash,
    own: RepositoryId,
    own_metadata: Hash,
}

impl Tenancy {
    async fn start() -> Self {
        let origin = Origin::start().await;
        let hidden: RepositoryId = rand::random();
        let own: RepositoryId = rand::random();
        let own_metadata = origin.seed(own, "tenant-a").await;
        let hidden_metadata = origin.seed(hidden, "customer-secret").await;
        Self {
            origin,
            hidden,
            hidden_metadata,
            own,
            own_metadata,
        }
    }

    async fn repositories(&self) -> RepositoryServiceClient<Channel> {
        RepositoryServiceClient::new(self.origin.channel().await)
    }

    async fn get(&self, query: Query, token: &str) -> Result<RepositoryId, Status> {
        let response = self
            .repositories()
            .await
            .repository_get(signed(RepositoryGetRequest { query: Some(query) }, token))
            .await?
            .into_inner();
        Ok(Context::from(response.repository.expect("a repository record").id).into())
    }

    async fn list(&self, token: &str) -> Result<Vec<RepositoryId>, Status> {
        let mut stream = self
            .repositories()
            .await
            .repository_list(signed(RepositoryListRequest { creator: None }, token))
            .await?
            .into_inner();
        let mut listed = Vec::new();
        while let Some(item) = stream.next().await {
            let repository = item?.repository.expect("a repository record");
            listed.push(RepositoryId::from(Context::from(repository.id)));
        }
        listed.sort_by_key(ToString::to_string);
        Ok(listed)
    }

    async fn create(&self, id: RepositoryId, name: &str, token: &str) -> Result<(), Status> {
        self.repositories()
            .await
            .repository_create(signed(
                RepositoryCreateRequest {
                    id: id.into(),
                    name: name.to_string(),
                    description: String::new(),
                    default_branch_id: rand::random::<Context>().into(),
                    default_branch_name: "main".to_string(),
                    creator: None,
                },
                token,
            ))
            .await
            .map(|_| ())
    }

    async fn delete(&self, id: RepositoryId, token: &str) -> Result<(), Status> {
        self.repositories()
            .await
            .repository_delete(signed(RepositoryDeleteRequest { id: id.into() }, token))
            .await
            .map(|_| ())
    }

    async fn metadata_get(&self, id: RepositoryId, token: &str) -> Result<Hash, Status> {
        self.repositories()
            .await
            .repository_metadata_get(signed(
                RepositoryMetadataGetRequest { id: id.into() },
                token,
            ))
            .await
            .map(|response| response.into_inner().metadata.into())
    }

    async fn metadata_set(
        &self,
        id: RepositoryId,
        metadata: Hash,
        token: &str,
    ) -> Result<(), Status> {
        self.repositories()
            .await
            .repository_metadata_set(signed(
                RepositoryMetadataSetRequest {
                    id: id.into(),
                    expected: metadata.into(),
                    updated: metadata.into(),
                },
                token,
            ))
            .await
            .map(|_| ())
    }
}

fn sorted(mut ids: Vec<RepositoryId>) -> Vec<RepositoryId> {
    ids.sort_by_key(ToString::to_string);
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_is_capped_by_the_callers_token() {
    let tenancy = Tenancy::start().await;
    let alice = grid_token(
        "alice",
        "github",
        false,
        &[(&urc(tenancy.own), &["read", "write"])],
        600,
    );
    tenancy.origin.broker.state().check_answer = Some(blanket(&[tenancy.hidden, tenancy.own]));

    let absent: RepositoryId = rand::random();
    let hidden = tenancy
        .get(Query::Id(tenancy.hidden.into()), &alice)
        .await
        .expect_err("R is outside alice's token, whatever the broker says");
    let missing = tenancy
        .get(Query::Id(absent.into()), &alice)
        .await
        .expect_err("an absent repository");
    assert_not_found(&hidden, tenancy.hidden);
    assert_not_found(&missing, absent);

    let by_name = tenancy
        .get(Query::Name("customer-secret".to_string()), &alice)
        .await
        .expect_err("nor by name");
    assert_eq!(by_name.code(), Code::NotFound);
    assert_eq!(by_name.message(), "Repository customer-secret not found");

    let own = tenancy
        .get(Query::Id(tenancy.own.into()), &alice)
        .await
        .expect("alice's own repository");
    assert_eq!(own, tenancy.own);

    let stranger = identity_without_resources("mallory");
    for id in [tenancy.hidden, tenancy.own] {
        let status = tenancy
            .get(Query::Id(id.into()), &stranger)
            .await
            .expect_err("a token without a resources claim grants nothing");
        assert_not_found(&status, id);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn list_shows_only_what_the_callers_token_grants() {
    let tenancy = Tenancy::start().await;

    let everything = tenancy
        .list(&developer("staff"))
        .await
        .expect("a developer's urc-* lists every repository");
    assert_eq!(everything, sorted(vec![tenancy.hidden, tenancy.own]));

    tenancy.origin.broker.state().lookup_answer = Some(blanket(&[tenancy.hidden, tenancy.own]));
    let alice = grid_token(
        "alice",
        "github",
        false,
        &[(&urc(tenancy.own), &["read"])],
        600,
    );
    let scoped = tenancy.list(&alice).await.expect("a scoped listing");
    assert_eq!(
        scoped,
        vec![tenancy.own],
        "a blanket broker answer must not list R"
    );

    let stranger = tenancy
        .list(&identity_without_resources("mallory"))
        .await
        .expect("an empty listing");
    assert!(
        stranger.is_empty(),
        "no resources claim lists nothing: {stranger:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_needs_a_writable_wildcard() {
    let tenancy = Tenancy::start().await;
    let fresh: RepositoryId = rand::random();
    let provisioner = grid_token(
        "provisioner",
        "grid-interim",
        true,
        &[(&urc(fresh), &["read", "write"])],
        600,
    );

    let taken = tenancy
        .create(fresh, "customer-secret", &provisioner)
        .await
        .expect_err("a scoped caller may not create");
    let free = tenancy
        .create(fresh, "fresh-name", &provisioner)
        .await
        .expect_err("a scoped caller may not create");
    for status in [&taken, &free] {
        assert_eq!(status.code(), Code::PermissionDenied, "{status:?}");
        assert_eq!(status.message(), CREATE_DENIED);
    }
    assert!(tenancy.origin.stored_metadata(fresh).await.is_none());

    tenancy
        .create(fresh, "fresh-name", &developer("staff"))
        .await
        .expect("a writable urc-* creates the repository");
    assert_eq!(tenancy.origin.broker.state().created, vec![urc(fresh)]);
    assert!(tenancy.origin.stored_metadata(fresh).await.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_needs_an_admin_with_write_on_the_repository() {
    let tenancy = Tenancy::start().await;
    {
        let mut broker = tenancy.origin.broker.state();
        broker.admins.insert("carol".to_string());
        broker.admins.insert("root".to_string());
    }

    let reader = grid_token(
        "carol",
        "github",
        false,
        &[(&urc(tenancy.hidden), &["read"])],
        600,
    );
    let status = tenancy
        .delete(tenancy.hidden, &reader)
        .await
        .expect_err("an admin needs write on R");
    assert_eq!(status.code(), Code::PermissionDenied);
    assert_eq!(status.message(), DELETE_DENIED);

    let outsider = grid_token(
        "root",
        "github",
        false,
        &[(&urc(tenancy.own), &["read", "write"])],
        600,
    );
    let status = tenancy
        .delete(tenancy.hidden, &outsider)
        .await
        .expect_err("admin membership does not reach outside the token's scope");
    assert_not_found(&status, tenancy.hidden);
    assert_eq!(
        tenancy.origin.stored_metadata(tenancy.hidden).await,
        Some(tenancy.hidden_metadata)
    );

    let writer = grid_token(
        "carol",
        "github",
        false,
        &[(&urc(tenancy.hidden), &["read", "write"])],
        600,
    );
    tenancy
        .delete(tenancy.hidden, &writer)
        .await
        .expect("an admin with write on R deletes it");
    assert_eq!(
        tenancy.origin.broker.state().deleted,
        vec![urc(tenancy.hidden)]
    );
    assert_eq!(tenancy.origin.stored_metadata(tenancy.hidden).await, None);
}

/// The repository client authenticates metadata calls with the stored login token (the
/// `AuthnInterceptor`'s `authentication_token`). GRID's logins carry `resources`, so they are
/// authorized from their own grants; a login without the claim is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_is_authorized_from_the_login_token() {
    let tenancy = Tenancy::start().await;

    let staff = developer("staff");
    assert_eq!(
        tenancy
            .metadata_get(tenancy.hidden, &staff)
            .await
            .expect("a developer login reads any repository's metadata"),
        tenancy.hidden_metadata
    );

    let runtime = deploy("grid-deploy", &[tenancy.own]);
    assert_eq!(
        tenancy
            .metadata_get(tenancy.own, &runtime)
            .await
            .expect("a deploy token reads its own repository's metadata"),
        tenancy.own_metadata
    );
    let hidden = tenancy
        .metadata_get(tenancy.hidden, &runtime)
        .await
        .expect_err("a deploy token scoped to A must not read R's metadata");
    let absent = tenancy
        .metadata_get(rand::random(), &runtime)
        .await
        .expect_err("an absent repository has no metadata");
    assert_eq!(hidden.code(), Code::PermissionDenied);
    assert_eq!(hidden.code(), absent.code());
    assert_eq!(hidden.message(), absent.message());
    let write = tenancy
        .metadata_set(tenancy.own, tenancy.own_metadata, &runtime)
        .await
        .expect_err("a read-only deploy token must not set metadata");
    assert_eq!(write.code(), Code::PermissionDenied);

    let stranger = identity_without_resources("mallory");
    for id in [tenancy.own, tenancy.hidden] {
        let status = tenancy
            .metadata_get(id, &stranger)
            .await
            .expect_err("a login without a resources claim is refused");
        assert_eq!(status.code(), Code::PermissionDenied);
    }
}

fn subscription(
    header: RepositoryId,
    body: RepositoryId,
    token: &str,
) -> Request<SubscribeRequest> {
    let mut request = signed(
        SubscribeRequest {
            repository: body.into(),
        },
        token,
    );
    request.metadata_mut().insert_bin(
        PARTITION_ID_KEY,
        BinaryMetadataValue::from_bytes(header.data()),
    );
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscribe_authorizes_the_repository_it_names() {
    let tenancy = Tenancy::start().await;
    let alice = grid_token(
        "alice",
        "github",
        false,
        &[(&urc(tenancy.own), &["read"])],
        600,
    );
    let mut notifications = NotificationServiceClient::new(tenancy.origin.channel().await);

    let status = notifications
        .subscribe(subscription(tenancy.own, tenancy.hidden, &alice))
        .await
        .expect_err("a token for A must not watch R, whatever the header names");
    assert_eq!(status.code(), Code::PermissionDenied);

    let stranger = identity_without_resources("mallory");
    let status = notifications
        .subscribe(subscription(tenancy.own, tenancy.own, &stranger))
        .await
        .expect_err("a token without a resources claim may not subscribe");
    assert_eq!(status.code(), Code::PermissionDenied);

    let mut events = notifications
        .subscribe(subscription(tenancy.own, tenancy.own, &alice))
        .await
        .expect("alice's own repository")
        .into_inner();
    tenancy
        .origin
        .notifications
        .branch_created(tenancy.own, rand::random())
        .await;
    let event = tokio::time::timeout(Duration::from_secs(10), events.message())
        .await
        .expect("an event for A")
        .expect("an open stream")
        .expect("an event, not the end");
    assert_eq!(
        RepositoryId::from(Context::from(event.repository)),
        tenancy.own
    );
}
