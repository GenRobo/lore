// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::Arc;

use axum::Extension;
use axum::Json;
use axum::body::Body;
use axum::body::Bytes;
use axum::extract::Path;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::HeaderValue;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Context;
use lore_revision::immutable;
use lore_revision::repository::RepositoryContext;
use lore_storage::options::WriteOptions;
use lore_transport::grpc::CORRELATION_ID_HEADER;
use serde::Serialize;
use tracing::debug;
use tracing::info;

use crate::auth::jwt::AuthorizationToken;
use crate::auth::jwt::verify_write_authorization;
use crate::http::server::ServerState;
use crate::util::get_user_id_from_token;
use crate::util::setup_execution;

#[derive(Serialize)]
struct ResponseData {
    address: String,
}

#[derive(Serialize)]
struct ResponseSuccess {
    data: ResponseData,
}

pub async fn handler(
    State(state): State<Arc<ServerState>>,
    Path(repository_id): Path<String>,
    Extension(user_info): Extension<Option<AuthorizationToken>>,
    headers: HeaderMap,
    data: Bytes,
) -> Response {
    info!("Put repository {} data {} bytes", repository_id, data.len());
    info!("User info: {:?}", user_info);

    let mut header_error = HeaderMap::new();
    header_error.insert("content-type", HeaderValue::from_str("text/plain").unwrap());

    let correlation_id = headers
        .get(CORRELATION_ID_HEADER)
        .and_then(|header_value| header_value.to_str().map(str::to_string).ok())
        .unwrap_or_default();

    // AZ-1: this route mutates the repository, so it needs a write grant. The auth
    // middleware verifies read on the path repository; when auth is off it inserts None
    // here and the request is allowed, matching the gRPC and QUIC write paths. A resource
    // that parses is checked; a malformed one falls through to the BAD_REQUEST below.
    let write_denied = matches!(user_info.as_ref(), Some(token)
        if repository_id
            .parse::<Context>()
            .map(|repository| verify_write_authorization(token, repository.into()).is_err())
            .unwrap_or(false));

    let user_id = get_user_id_from_token(user_info);
    let execution = setup_execution(module_path!(), correlation_id, user_id);
    LORE_CONTEXT
        .scope(execution, async move {
            if write_denied {
                return (
                    StatusCode::FORBIDDEN,
                    header_error,
                    Body::from("Write access to the repository is required for this operation"),
                )
                    .into_response();
            }
            let repository = match repository_id.parse::<Context>() {
                Ok(repository_id) => Arc::new(RepositoryContext::new_server_context(
                    state.immutable_store.clone(),
                    state.mutable_store.clone(),
                    repository_id.into(),
                )),
                Err(error) => {
                    debug!("Error parsing the repository ID {}", error);
                    return (
                        StatusCode::BAD_REQUEST,
                        header_error,
                        Body::from("Wrong repository"),
                    )
                        .into_response();
                }
            };

            let context = uuid::Uuid::now_v7().into();
            let (address, _fragment) = match immutable::write(
                repository.clone(),
                context,
                data,
                WriteOptions::default().with_remote_write(),
            )
            .await
            {
                Ok(result) => result,
                Err(error) => {
                    debug!("Failed to write into immutable store. {}", error);
                    return (
                        StatusCode::BAD_REQUEST,
                        header_error,
                        Body::from("Malformed data"),
                    )
                        .into_response();
                }
            };

            (
                StatusCode::OK,
                Json(ResponseSuccess {
                    data: ResponseData {
                        address: format!("{address}"),
                    },
                }),
            )
                .into_response()
        })
        .await
}
