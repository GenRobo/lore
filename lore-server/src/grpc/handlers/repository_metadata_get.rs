// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Context;
use lore_proto::RepositoryMetadataGetRequest;
use lore_proto::RepositoryMetadataGetResponse;
use lore_revision::repository;
use lore_revision::repository::RepositoryContext;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::warn;

use crate::auth::jwt::verify_authorization;
use crate::grpc::extract_correlation_id;
use crate::grpc::get_authorization;
use crate::grpc::get_user_id;
use crate::util::setup_execution;

#[tracing::instrument(name = "RepositoryMetadataGet::handle", skip_all)]
pub async fn handler(
    request: Request<RepositoryMetadataGetRequest>,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
) -> Result<Response<RepositoryMetadataGetResponse>, Status> {
    // AZ-1: the RepositoryService interceptor only authenticates the caller (UCS-13506), so
    // authorize the repository named in the request body here — otherwise a token valid for
    // one repository can read another's metadata. No token means auth is off, so skip.
    let authorization = get_authorization(request.extensions()).ok();
    let user_id = get_user_id(request.extensions());
    let correlation_id = extract_correlation_id(&request).unwrap_or_default();
    let req = request.into_inner();

    let repository_id: Context = req.repository_id.into();
    if repository_id == Context::default() {
        return Err(Status::invalid_argument("Missing repository ID"));
    }

    let repository_id: lore_revision::lore::RepositoryId = repository_id.into();
    if let Some(token) = authorization.as_ref()
        && verify_authorization(token, repository_id).is_err()
    {
        return Err(Status::permission_denied("Not authorized to access repository"));
    }

    let execution = setup_execution(module_path!(), correlation_id, user_id);
    let repository = Arc::new(RepositoryContext::new_server_context(
        immutable_store,
        mutable_store,
        repository_id,
    ));

    LORE_CONTEXT
        .scope(execution, async move {
            let metadata_hash = repository::metadata_hash(repository).await.map_err(|err| {
                warn!(%err, "Failed to load repository metadata hash");
                Status::not_found(err.to_string())
            })?;

            Ok(Response::new(RepositoryMetadataGetResponse {
                metadata_hash: metadata_hash.into(),
            }))
        })
        .await
}
