// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use lore_proto::lore::repository::v1::RepositoryCreateRequest;
use lore_proto::lore::repository::v1::RepositoryCreateResponse;
use lore_proto::lore::repository::v1::RepositoryDeleteRequest;
use lore_proto::lore::repository::v1::RepositoryDeleteResponse;
use lore_proto::lore::repository::v1::RepositoryGetRequest;
use lore_proto::lore::repository::v1::RepositoryGetResponse;
use lore_proto::lore::repository::v1::RepositoryListRequest;
use lore_proto::lore::repository::v1::RepositoryListResponse;
use lore_proto::lore::repository::v1::RepositoryMetadataGetRequest;
use lore_proto::lore::repository::v1::RepositoryMetadataGetResponse;
use lore_proto::lore::repository::v1::RepositoryMetadataSetRequest;
use lore_proto::lore::repository::v1::RepositoryMetadataSetResponse;
use lore_proto::lore::repository::v1::repository_service_server::RepositoryService;
use lore_revision::environment::EnvironmentConfig;
use lore_telemetry::InstrumentProvider;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::codegen::tokio_stream::Stream;

use super::repository_create;
use super::repository_delete;
use super::repository_get;
use super::repository_list;
use super::repository_metadata_get;
use super::repository_metadata_set;
use crate::grpc::timeout_grpc;
use crate::hooks::HookDispatcher;

type RepositoryListStream =
    Pin<Box<dyn Stream<Item = Result<RepositoryListResponse, Status>> + Send + 'static>>;

/// Zero-sized `InstrumentProvider` carrying the v1 service's metric
/// namespace. Standalone so the constructor can mint instruments before
/// `LoreRepositoryV1Service` exists.
#[derive(Clone)]
struct RepositoryServiceInstrumentProvider;

impl InstrumentProvider for RepositoryServiceInstrumentProvider {
    fn namespace(&self) -> &'static str {
        "lore.repository.v1.repository_service"
    }
}

/// Dispatch struct for `lore.repository.v1.RepositoryService`. Methods
/// delegate to the per-RPC handlers in sibling files; the struct itself
/// only holds the dependencies the handlers need.
#[derive(Clone)]
pub struct LoreRepositoryV1Service {
    environment: EnvironmentConfig,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    hook_dispatcher: Arc<HookDispatcher>,
    rpc_timeout: Duration,
    instrument_provider: RepositoryServiceInstrumentProvider,
}

impl LoreRepositoryV1Service {
    pub fn new(
        environment: EnvironmentConfig,
        immutable_store: Arc<dyn lore_storage::ImmutableStore>,
        mutable_store: Arc<dyn lore_storage::MutableStore>,
        hook_dispatcher: Arc<HookDispatcher>,
        rpc_timeout: Duration,
    ) -> Self {
        Self {
            environment,
            immutable_store,
            mutable_store,
            hook_dispatcher,
            rpc_timeout,
            instrument_provider: RepositoryServiceInstrumentProvider,
        }
    }

    /// Auth-service URL extracted from the environment, if configured.
    fn auth_url(&self) -> Option<String> {
        self.environment
            .endpoint
            .clone()
            .and_then(|endpoint| endpoint.auth_url)
            .filter(|url| !url.trim().is_empty())
    }

    /// Whether a handler reached without an authorization token must deny: JWT verification
    /// is on, or this origin delegates repository authorization to a broker.
    fn auth_required(&self) -> bool {
        crate::auth::auth_enabled() || self.auth_url().is_some()
    }

    /// The broker the create, delete, get and list handlers authorize against.
    fn broker(&self) -> Result<Option<String>, Status> {
        broker_url(self.auth_url(), crate::auth::auth_enabled())
    }
}

/// With JWT verification on, a missing broker URL would leave create, delete, get and list
/// unauthorized, so they fail closed instead.
fn broker_url(auth_url: Option<String>, auth_enabled: bool) -> Result<Option<String>, Status> {
    match auth_url {
        None if auth_enabled => Err(Status::failed_precondition(
            "Repository authorization is not configured on this server",
        )),
        auth_url => Ok(auth_url),
    }
}

#[tonic::async_trait]
impl RepositoryService for LoreRepositoryV1Service {
    async fn repository_create(
        &self,
        request: Request<RepositoryCreateRequest>,
    ) -> Result<Response<RepositoryCreateResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            repository_create::handler(
                request,
                self.broker()?,
                self.immutable_store.clone(),
                self.mutable_store.clone(),
                &self.hook_dispatcher,
                &self.instrument_provider,
            ),
        )
        .await
    }

    async fn repository_delete(
        &self,
        request: Request<RepositoryDeleteRequest>,
    ) -> Result<Response<RepositoryDeleteResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            repository_delete::handler(
                request,
                self.broker()?,
                self.immutable_store.clone(),
                self.mutable_store.clone(),
                &self.instrument_provider,
            ),
        )
        .await
    }

    async fn repository_get(
        &self,
        request: Request<RepositoryGetRequest>,
    ) -> Result<Response<RepositoryGetResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            repository_get::handler(
                request,
                self.broker()?,
                self.immutable_store.clone(),
                self.mutable_store.clone(),
            ),
        )
        .await
    }

    type RepositoryListStream = RepositoryListStream;

    async fn repository_list(
        &self,
        request: Request<RepositoryListRequest>,
    ) -> Result<Response<Self::RepositoryListStream>, Status> {
        repository_list::handler(
            request,
            self.broker()?,
            self.immutable_store.clone(),
            self.mutable_store.clone(),
        )
        .await
    }

    async fn repository_metadata_get(
        &self,
        request: Request<RepositoryMetadataGetRequest>,
    ) -> Result<Response<RepositoryMetadataGetResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            repository_metadata_get::handler(
                request,
                self.auth_required(),
                self.immutable_store.clone(),
                self.mutable_store.clone(),
            ),
        )
        .await
    }

    async fn repository_metadata_set(
        &self,
        request: Request<RepositoryMetadataSetRequest>,
    ) -> Result<Response<RepositoryMetadataSetResponse>, Status> {
        timeout_grpc(
            self.rpc_timeout,
            repository_metadata_set::handler(
                request,
                self.auth_required(),
                self.immutable_store.clone(),
                self.mutable_store.clone(),
            ),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use lore_proto::lore::repository::v1::repository_service_server::RepositoryServiceServer;

    use super::*;

    /// Compile-time check that `LoreRepositoryV1Service` fully implements
    /// the generated `RepositoryService` trait — wrapping it in
    /// `RepositoryServiceServer` requires the trait bound to hold.
    #[allow(dead_code)]
    fn assert_implements_trait(
        service: LoreRepositoryV1Service,
    ) -> RepositoryServiceServer<LoreRepositoryV1Service> {
        RepositoryServiceServer::new(service)
    }

    #[test]
    fn broker_handlers_fail_closed_under_jwt_without_a_broker() {
        let url = Some("https://auth.example".to_string());
        assert_eq!(broker_url(url.clone(), true).unwrap(), url);
        assert_eq!(broker_url(url.clone(), false).unwrap(), url);
        assert_eq!(broker_url(None, false).unwrap(), None);
        let status = broker_url(None, true).expect_err("JWT on without a broker must fail closed");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    }
}
