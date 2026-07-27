// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;
use std::time::Duration;

use lore_base::error::InvalidArguments;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::LockResource;
use lore_proto::LockService;
use lore_proto::lock::AdminLockRequest;
use lore_proto::lock::AdminLockResponse;
use lore_proto::lock::LockRequest;
use lore_proto::lock::LockResponse;
use lore_proto::lock::QueryRequest;
use lore_proto::lock::QueryResponse;
use lore_proto::lock::StatusRequest;
use lore_proto::lock::StatusResponse;
use lore_proto::lock::UnlockRequest;
use lore_proto::lock::UnlockResponse;
use lore_revision::lock::LockError;
use lore_revision::lock::LockQuery;
use lore_revision::lock::LockStore;
use lore_revision::lore::RepositoryId;
use lore_revision::notification::NotificationSender;
use lore_telemetry::InstrumentProvider;
use opentelemetry::metrics::Histogram;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::info;
use tracing::warn;

use super::extract_correlation_id;
use super::get_repository;
use super::get_user_id;
use super::is_owner_or_admin;
use super::timeout_grpc;
use crate::grpc::can_admin_lock;
use crate::util::setup_execution;

const STATUS_MAX_RESOURCE_LEN: usize = 100;

#[derive(Clone)]
struct LoreLockServiceInstrumentProvider {}

fn lock_query_from_request(
    repository: RepositoryId,
    request: &QueryRequest,
) -> Result<LockQuery, LockError> {
    match (&request.branch, &request.owner, &request.description) {
        // Repository
        (None, None, None) => Ok(LockQuery::Repository(repository)),
        // RepositoryBranch
        (Some(branch), None, None) => Ok(LockQuery::RepositoryBranch(repository, branch.into())),
        // RepositoryBranchDescription
        (Some(branch), None, Some(description)) => Ok(LockQuery::RepositoryBranchDescription(
            repository,
            branch.into(),
            description.clone(),
        )),
        // OwnerRepository
        (None, Some(owner), None) => Ok(LockQuery::OwnerRepository(owner.clone(), repository)),
        // OwnerRepositoryBranch
        (Some(branch), Some(owner), None) => Ok(LockQuery::OwnerRepositoryBranch(
            owner.clone(),
            repository,
            branch.into(),
        )),
        _ => Err(InvalidArguments {
            reason: "unsupported lock query combination".into(),
        }
        .into()),
    }
}

fn handle_lock_error(error: LockError) -> Status {
    match error {
        LockError::LockNotFound(_) => Status::not_found(error.to_string()),
        LockError::LockNotOwned(_) => Status::failed_precondition(error.to_string()),
        LockError::SlowDown(_) => Status::resource_exhausted(error.to_string()),
        LockError::InvalidArguments(_) => Status::invalid_argument(error.to_string()),
        LockError::Internal(_) => {
            warn!(error = ?error, "LockData operation failed");
            Status::internal(error.to_string())
        }
    }
}

#[derive(Clone)]
pub struct LoreLockService {
    lock_store: Arc<dyn LockStore>,
    notification: Arc<dyn NotificationSender>,
    rpc_timeout: Duration,

    instrument_provider: LoreLockServiceInstrumentProvider,
    locking_histogram: Histogram<u64>,
    status_histogram: Histogram<u64>,
}

impl LoreLockService {
    pub fn new(
        lock_store: Arc<dyn LockStore>,
        notification: Arc<dyn NotificationSender>,
        rpc_timeout: Duration,
    ) -> Self {
        let instrument_provider = LoreLockServiceInstrumentProvider {};

        Self {
            lock_store,
            notification,
            rpc_timeout,
            locking_histogram: instrument_provider.length_histogram(
                "locking.request.resources.length",
                vec![1., 5., 10., 25., 50., 75., 100., 200.],
            ),
            status_histogram: instrument_provider.length_histogram(
                "status.request.resources.length",
                vec![
                    1., 5., 10., 50., 100., 200., 300., 500., 2_500., 5_000., 10_000., 20_000.,
                    40_000., 60_000., 80_000.,
                ],
            ),
            instrument_provider,
        }
    }
}

impl InstrumentProvider for LoreLockServiceInstrumentProvider {
    fn namespace(&self) -> &'static str {
        "urc.lock_service"
    }
}

impl LoreLockService {
    async fn lock_as_user(
        &self,
        repository: RepositoryId,
        resources: Vec<lore_proto::lock::Resource>,
        owner_id: &str,
    ) -> Result<Vec<lore_proto::lock::Lock>, Status> {
        if resources.is_empty() {
            return Err(Status::invalid_argument("At least one resource needed"));
        }

        let lock_resources: Vec<LockResource> = resources.into_iter().map(Into::into).collect();

        let locks = self
            .lock_store
            .lock_resources(owner_id, repository, &lock_resources)
            .await
            .map_err(handle_lock_error)?;

        // TODO: UCS-13626 move branch out of individual resources into the main message
        // All resources are on the same branch and the lock call has to be made with at least 1 resource
        let branch = lock_resources[0].branch;
        let locked_resources: Vec<LockResource> =
            locks.iter().map(|lock| lock.resource.clone()).collect();

        self.notification
            .resource_locked(repository, branch, owner_id, &locked_resources)
            .await;

        let locks = locks.into_iter().map(Into::into).collect();

        Ok(locks)
    }
}

impl LoreLockService {
    async fn handle_lock(
        &self,
        request: Request<LockRequest>,
    ) -> Result<Response<LockResponse>, Status> {
        let repository = get_repository(request.metadata())?;
        let user_id = get_user_id(request.extensions());
        let correlation_id = extract_correlation_id(&request).unwrap_or_default();
        let lock_request = request.into_inner();

        self.locking_histogram.record(
            lock_request.resources.len() as u64,
            &self
                .instrument_provider
                .get_labels_for_operation_context("lock"),
        );

        if lock_request.resources.is_empty() {
            return Ok(Response::new(LockResponse { locks: vec![] }));
        }

        let resources = lock_request.resources;

        let execution = setup_execution(module_path!(), correlation_id, user_id.clone());

        LORE_CONTEXT
            .scope(execution, async move {
                self.lock_as_user(repository, resources, &user_id)
                    .await
                    .map(|locks| Response::new(LockResponse { locks }))
            })
            .await
    }

    async fn handle_query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let user_id = get_user_id(request.extensions());
        let repository = get_repository(request.metadata())?;
        let correlation_id = extract_correlation_id(&request).unwrap_or_default();
        let query_request = request.get_ref();

        let query =
            lock_query_from_request(repository, query_request).map_err(handle_lock_error)?;

        let execution = setup_execution(module_path!(), correlation_id, user_id.clone());

        LORE_CONTEXT
            .scope(execution, async move {
                self.lock_store
                    .query_locks(query)
                    .await
                    .map(|result| {
                        Response::new(QueryResponse {
                            result: result.into_iter().map(Into::into).collect(),
                        })
                    })
                    .map_err(handle_lock_error)
            })
            .await
    }

    async fn handle_status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let user_id = get_user_id(request.extensions());
        let correlation_id = extract_correlation_id(&request).unwrap_or_default();
        let repository = get_repository(request.metadata())?;
        let status_request = request.into_inner();

        if status_request.resources.len() > STATUS_MAX_RESOURCE_LEN {
            return Err(Status::invalid_argument("Resource count exceeds limit"));
        }

        self.status_histogram.record(
            status_request.resources.len() as u64,
            &self
                .instrument_provider
                .get_labels_for_operation_context("status"),
        );

        if status_request.resources.is_empty() {
            return Ok(Response::new(StatusResponse { locks: vec![] }));
        }

        info!(
            num_items = status_request.resources.len(),
            "Handling LockService::Status request"
        );

        let resources: Vec<LockResource> = status_request
            .resources
            .into_iter()
            .map(Into::into)
            .collect();

        let execution = setup_execution(module_path!(), correlation_id, user_id.clone());

        LORE_CONTEXT
            .scope(execution, async move {
                let locks = self
                    .lock_store
                    .check_locks_status(repository, &resources)
                    .await
                    .map_err(handle_lock_error)?;

                Ok(Response::new(StatusResponse {
                    locks: locks.into_iter().map(Into::into).collect(),
                }))
            })
            .await
    }

    async fn handle_unlock(
        &self,
        request: Request<UnlockRequest>,
    ) -> Result<Response<UnlockResponse>, Status> {
        let user_id = get_user_id(request.extensions());
        let correlation_id = extract_correlation_id(&request).unwrap_or_default();
        let repository = get_repository(request.metadata())?;
        let validate_user = !is_owner_or_admin(request.extensions(), repository);
        let unlock_request = request.into_inner();

        self.locking_histogram.record(
            unlock_request.resources.len() as u64,
            &self
                .instrument_provider
                .get_labels_for_operation_context("unlock"),
        );

        if unlock_request.resources.is_empty() {
            return Ok(Response::new(UnlockResponse { resources: vec![] }));
        }

        let resources: Vec<LockResource> =
            unlock_request.resources.iter().map(Into::into).collect();

        let execution = setup_execution(module_path!(), correlation_id, user_id.clone());

        LORE_CONTEXT
            .scope(execution, async move {
                let resources = self
                    .lock_store
                    .unlock_resources(user_id.as_str(), validate_user, repository, &resources)
                    .await
                    .map_err(handle_lock_error)?;

                // TODO: UCS-13626 move branch out of individual resources into the main message
                // All resources are on the same branch and the lock call has to be made with at least 1 resource
                if !resources.is_empty() {
                    self.notification
                        .resource_unlocked(repository, resources[0].branch, &user_id, &resources)
                        .await;
                }

                Ok(Response::new(UnlockResponse {
                    resources: resources.into_iter().map(Into::into).collect(),
                }))
            })
            .await
    }

    async fn handle_admin_lock(
        &self,
        request: Request<AdminLockRequest>,
    ) -> Result<Response<AdminLockResponse>, Status> {
        let correlation_id = extract_correlation_id(&request).unwrap_or_default();
        let repository = get_repository(request.metadata())?;
        let extensions = request.extensions().clone();

        let user_id = get_user_id(request.extensions());
        let lock_request = request.into_inner();

        self.locking_histogram.record(
            lock_request.resources.len() as u64,
            &self
                .instrument_provider
                .get_labels_for_operation_context("admin_lock"),
        );

        if lock_request.resources.is_empty() {
            return Ok(Response::new(AdminLockResponse { locks: vec![] }));
        }

        let resources = lock_request.resources;
        let owner = lock_request.owner;

        let execution = setup_execution(module_path!(), correlation_id, user_id.clone());

        LORE_CONTEXT
            .scope(execution, async move {
                if !can_admin_lock(&extensions, repository) {
                    warn!("Attempt to apply admin locks, but user does not have the correct permissions");
                    return Err(Status::permission_denied("Permission denied"));
                }

                self.lock_as_user(repository, resources, &owner)
                    .await
                    .map(|locks| Response::new(AdminLockResponse { locks }))
            })
            .await
    }
}

#[tonic::async_trait]
impl LockService for LoreLockService {
    #[tracing::instrument(name = "LoreLockService::lock", skip_all)]
    async fn lock(&self, request: Request<LockRequest>) -> Result<Response<LockResponse>, Status> {
        crate::grpc::verify_write(&request)?;
        timeout_grpc(self.rpc_timeout, self.handle_lock(request)).await
    }

    #[tracing::instrument(name = "LoreLockService::query", skip_all)]
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        timeout_grpc(self.rpc_timeout, self.handle_query(request)).await
    }

    #[tracing::instrument(name = "LoreLockService::status", skip_all)]
    async fn status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        timeout_grpc(self.rpc_timeout, self.handle_status(request)).await
    }

    #[tracing::instrument(name = "LoreLockService::unlock", skip_all)]
    async fn unlock(
        &self,
        request: Request<UnlockRequest>,
    ) -> Result<Response<UnlockResponse>, Status> {
        crate::grpc::verify_write(&request)?;
        timeout_grpc(self.rpc_timeout, self.handle_unlock(request)).await
    }

    #[tracing::instrument(name = "LoreLockService::admin_lock", skip_all)]
    async fn admin_lock(
        &self,
        request: Request<AdminLockRequest>,
    ) -> Result<Response<AdminLockResponse>, Status> {
        crate::grpc::verify_write(&request)?;
        timeout_grpc(self.rpc_timeout, self.handle_admin_lock(request)).await
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::time::Duration;

    use lore_proto::LockService;
    use lore_revision::lore::RepositoryId;
    use lore_transport::grpc::REPOSITORY_ID_KEY;
    use rand::random;
    use tonic::Code;
    use tonic::Request;

    use crate::grpc::lock_service::LoreLockService;

    mod store {
        use async_trait::async_trait;
        use lore_base::types::LockData;
        use lore_base::types::LockResource;
        use lore_revision::lock::LockError;
        use lore_revision::lock::LockQuery;
        use lore_revision::lock::LockStore;
        use lore_revision::lore::RepositoryId;

        mockall::mock! {
             pub MockLockStore {}

             #[async_trait]
             impl LockStore for MockLockStore {

                async fn lock_resources(
                    &self,
                    owner_id: &str,
                    repository: RepositoryId,
                    resources: &[LockResource],
                ) -> Result<Vec<LockData>, LockError>;

                async fn query_locks(&self, query: LockQuery) -> Result<Vec<LockData>, LockError>;

                async fn check_locks_status(
                    &self,
                    repository: RepositoryId,
                    resources: &[LockResource],
                ) -> Result<Vec<LockData>, LockError>;


                async fn unlock_resources(
                    &self,
                    owner_id: &str,
                    validate_user: bool,
                    repository: RepositoryId,
                    resources: &[LockResource],
                ) -> Result<Vec<LockResource>, LockError>;
            }
        }
    }

    mod status {
        use lore_proto::lock::Resource;
        use lore_proto::lock::StatusRequest;

        use super::*;
        use crate::notification::local::NotificationSender;

        #[tokio::test]
        async fn resource_count_exceeds_limit() {
            let lock_store = super::store::MockMockLockStore::new();

            let notification_sender = Arc::new(NotificationSender::default());
            let lock_service = LoreLockService::new(
                Arc::new(lock_store),
                notification_sender,
                Duration::from_secs(60),
            );

            let resources: Vec<Resource> = (0..101)
                .map(|_| Resource {
                    branch: Default::default(),
                    hash: Default::default(),
                    description: "".to_string(),
                })
                .collect();

            let mut request = Request::new(StatusRequest { resources });
            let repository = random::<RepositoryId>();
            request.metadata_mut().insert_bin(
                REPOSITORY_ID_KEY,
                tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
            );

            let error_status = lock_service
                .status(request)
                .await
                .expect_err("Status should fail when resource count exceeds limit");

            assert_eq!(error_status.code(), Code::InvalidArgument);
        }

        #[tokio::test]
        async fn resource_count_at_limit() {
            let mut lock_store = super::store::MockMockLockStore::new();
            lock_store
                .expect_check_locks_status()
                .return_once(|_, _| Ok(vec![]));

            let notification_sender = Arc::new(NotificationSender::default());
            let lock_service = LoreLockService::new(
                Arc::new(lock_store),
                notification_sender,
                Duration::from_secs(60),
            );

            let resources: Vec<Resource> = (0..100)
                .map(|_| Resource {
                    branch: Default::default(),
                    hash: Default::default(),
                    description: "".to_string(),
                })
                .collect();

            let mut request = Request::new(StatusRequest { resources });
            let repository = random::<RepositoryId>();
            request.metadata_mut().insert_bin(
                REPOSITORY_ID_KEY,
                tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
            );

            let _ = lock_service
                .status(request)
                .await
                .expect("Status should succeed when resource count is at limit");
        }
    }

    mod unlock {
        use lore_proto::lock::AdminLockRequest;
        use lore_proto::lock::LockRequest;
        use lore_proto::lock::Resource;
        use lore_proto::lock::StatusRequest;
        use lore_proto::lock::UnlockRequest;

        use super::*;
        use crate::notification::local::NotificationSender;

        #[tokio::test]
        async fn lock_zero_resources() {
            let lock_store = super::store::MockMockLockStore::new();

            let notification_sender = Arc::new(NotificationSender::default());
            let lock_service = LoreLockService::new(
                Arc::new(lock_store),
                notification_sender,
                Duration::from_secs(60),
            );

            let mut request = Request::new(LockRequest { resources: vec![] });
            let repository = random::<RepositoryId>();
            request.metadata_mut().insert_bin(
                REPOSITORY_ID_KEY,
                tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
            );

            let _ = lock_service
                .lock(request)
                .await
                .expect("LockData did not return ok status");
        }

        #[tokio::test]
        async fn unlock_zero_resources() {
            let lock_store = super::store::MockMockLockStore::new();

            let notification_sender = Arc::new(NotificationSender::default());
            let lock_service = LoreLockService::new(
                Arc::new(lock_store),
                notification_sender,
                Duration::from_secs(60),
            );

            let mut request = Request::new(UnlockRequest { resources: vec![] });
            let repository = random::<RepositoryId>();
            request.metadata_mut().insert_bin(
                REPOSITORY_ID_KEY,
                tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
            );

            let _ = lock_service
                .unlock(request)
                .await
                .expect("Unlock did not return ok status");
        }

        #[tokio::test]
        async fn status_zero_resources() {
            let lock_store = super::store::MockMockLockStore::new();

            let notification_sender = Arc::new(NotificationSender::default());
            let lock_service = LoreLockService::new(
                Arc::new(lock_store),
                notification_sender,
                Duration::from_secs(60),
            );

            let mut request = Request::new(StatusRequest { resources: vec![] });
            let repository = random::<RepositoryId>();
            request.metadata_mut().insert_bin(
                REPOSITORY_ID_KEY,
                tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
            );

            let _ = lock_service
                .status(request)
                .await
                .expect("Status did not return ok status");
        }

        #[tokio::test]
        async fn admin_unlock_zero_resources() {
            let lock_store = super::store::MockMockLockStore::new();

            let notification_sender = Arc::new(NotificationSender::default());
            let lock_service = LoreLockService::new(
                Arc::new(lock_store),
                notification_sender,
                Duration::from_secs(60),
            );

            let mut request = Request::new(AdminLockRequest {
                resources: vec![],
                owner: "".to_string(),
            });
            let repository = random::<RepositoryId>();
            request.metadata_mut().insert_bin(
                REPOSITORY_ID_KEY,
                tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
            );

            let _ = lock_service
                .admin_lock(request)
                .await
                .expect("Admin lock did not return ok status");
        }

        #[tokio::test]
        async fn unlock_fails_for_other_owner() {
            let mut lock_store = super::store::MockMockLockStore::new();
            lock_store
                .expect_unlock_resources()
                .return_once(|_, _, _, _| Err(lore_base::error::LockNotOwned.into()));

            let notification_sender = Arc::new(NotificationSender::default());
            let lock_service = LoreLockService::new(
                Arc::new(lock_store),
                notification_sender,
                Duration::from_secs(60),
            );

            let mut request = Request::new(UnlockRequest {
                resources: vec![Resource {
                    branch: Default::default(),
                    hash: Default::default(),
                    description: "".to_string(),
                }],
            });
            let repository = random::<RepositoryId>();
            request.metadata_mut().insert_bin(
                REPOSITORY_ID_KEY,
                tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
            );

            let error_status = lock_service
                .unlock(request)
                .await
                .expect_err("Unlock did not return error status");

            assert_eq!(error_status.code(), Code::FailedPrecondition);
        }
    }
}
