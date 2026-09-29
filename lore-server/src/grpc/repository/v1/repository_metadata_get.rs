// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Context;
use lore_proto::lore::repository::v1::RepositoryMetadataGetRequest;
use lore_proto::lore::repository::v1::RepositoryMetadataGetResponse;
use lore_revision::lore::RepositoryId;
use lore_revision::repository;
use lore_revision::repository::RepositoryContext;
use tonic::Request;
use tonic::Response;
use tonic::Status;

use crate::auth::Access;
use crate::auth::jwt::AuthorizationToken;
use crate::grpc::authorize_body_repository;
use crate::grpc::extract_correlation_id;
use crate::grpc::get_user_id;
use crate::util::setup_execution;

/// `lore.repository.v1.RepositoryService.RepositoryMetadataGet` handler.
///
/// Cheap hash-only read of the repository's metadata pointer. Returns the
/// hash unchanged from `repository::metadata_hash`; callers wanting the
/// deserialised metadata blob fetch the addressed content separately.
#[tracing::instrument(name = "RepositoryMetadataGet::v1::handle", skip_all)]
pub async fn handler(
    request: Request<RepositoryMetadataGetRequest>,
    auth_required: bool,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
) -> Result<Response<RepositoryMetadataGetResponse>, Status> {
    let authorization = request.extensions().get::<AuthorizationToken>().cloned();
    let user_id = get_user_id(request.extensions());
    let correlation_id = extract_correlation_id(&request).unwrap_or_default();
    let req = request.into_inner();

    let repository_id: Context = req.id.into();
    if repository_id == Context::default() {
        return Err(Status::invalid_argument("Missing repository id"));
    }
    let repository_id: RepositoryId = repository_id.into();
    authorize_body_repository(
        authorization.as_ref(),
        auth_required,
        repository_id,
        Access::Read,
    )?;

    let execution = setup_execution(module_path!(), correlation_id, user_id);
    let repository = Arc::new(RepositoryContext::new_server_context(
        immutable_store,
        mutable_store,
        repository_id,
    ));

    LORE_CONTEXT
        .scope(execution, async move {
            let metadata_hash = repository::metadata_hash(repository)
                .await
                .map_err(|err| Status::not_found(err.to_string()))?;

            Ok(Response::new(RepositoryMetadataGetResponse {
                metadata: metadata_hash.into(),
            }))
        })
        .await
}
