// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Fixtures for authorization tests: signed test tokens and seeded repositories.

use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use jsonwebtoken::Algorithm;
use jsonwebtoken::EncodingKey;
use jsonwebtoken::Header;
use lore_base::types::Hash;
use lore_revision::lore::RepositoryId;
use lore_revision::repository;
use lore_revision::repository::RepositoryContext;
use lore_revision::repository::RepositoryMetadata;
use tonic::Request;
use tonic::Status;

use crate::auth::jwt::AuthorizationToken;
use crate::auth::jwt::ResourcePermission;

const SECRET: &[u8] = b"lore-authorization-test-secret";
const AUDIENCE: &str = "lore-test";

pub(crate) fn urc(repository: RepositoryId) -> String {
    format!("urc-{repository}")
}

pub(crate) fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs()
}

/// A signed token for `user` carrying `grants` as (resource id, permissions), valid for `ttl`
/// seconds. Returns the encoded JWT and its claims.
pub(crate) fn mint(
    user: &str,
    grants: &[(&str, &[&str])],
    ttl: u64,
) -> (String, AuthorizationToken) {
    let now = unix_now();
    let claims = AuthorizationToken {
        user_id: user.to_string(),
        issuer: "lore-test".to_string(),
        issued_at: now,
        expires: now + ttl,
        audience: vec![AUDIENCE.to_string()],
        env: "test".to_string(),
        name: user.to_string(),
        preferred_username: user.to_string(),
        resources: Some(
            grants
                .iter()
                .map(|(resource_id, permission)| ResourcePermission {
                    resource_id: (*resource_id).to_string(),
                    permission: permission.iter().map(|p| (*p).to_string()).collect(),
                })
                .collect(),
        ),
        groups: None,
        is_service_account: Some(false),
        idp: "test".to_string(),
    };
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some("test".to_string());
    let token = jsonwebtoken::encode(&header, &claims, &EncodingKey::from_secret(SECRET))
        .expect("encode test token");
    (token, claims)
}

/// A request as the JWT interceptors leave it: the bearer header forwarded to the broker and
/// the verified claims in the extensions.
pub(crate) fn as_caller<T>(
    message: T,
    caller: Option<&(String, AuthorizationToken)>,
) -> Request<T> {
    let mut request = Request::new(message);
    if let Some((bearer, claims)) = caller {
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {bearer}").parse().expect("bearer header"),
        );
        request.extensions_mut().insert(claims.clone());
    }
    request
}

/// Store a repository the way `RepositoryCreate` does, returning its metadata pointer.
/// Call inside a `LORE_CONTEXT` scope.
pub(crate) async fn seed_repository(
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    id: RepositoryId,
    name: &str,
) -> Hash {
    let repository = Arc::new(RepositoryContext::new_server_context(
        immutable_store,
        mutable_store,
        id,
    ));
    let metadata = RepositoryMetadata {
        name: name.to_string(),
        description: format!("{name} description"),
        default_branch: rand::random(),
        default_branch_name: "main".to_string(),
        creator: format!("{name}-owner"),
        created: 1,
    };
    let hash = repository::metadata_store(repository.clone(), metadata)
        .await
        .expect("store metadata");
    repository::metadata_store_hash(repository.clone(), hash)
        .await
        .expect("store metadata pointer");
    repository::store_name_to_id(repository, name, id)
        .await
        .expect("store name mapping");
    hash
}

/// Two statuses a caller cannot tell apart.
pub(crate) fn assert_same_status(left: &Status, right: &Status) {
    assert_eq!(
        left.code(),
        right.code(),
        "status codes differ: {left:?} vs {right:?}"
    );
    assert_eq!(left.message(), right.message(), "status messages differ");
    assert_eq!(left.details(), right.details(), "status details differ");
}
