// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Fixtures for authorization tests: signed test tokens, seeded repositories, and an
//! in-process ucs-auth broker.

use std::collections::HashSet;
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::task::Context as TaskContext;
use std::task::Poll;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use jsonwebtoken::Algorithm;
use jsonwebtoken::EncodingKey;
use jsonwebtoken::Header;
use lore_base::types::Hash;
use lore_proto::auth;
use lore_proto::rebac;
use lore_revision::lore::RepositoryId;
use lore_revision::repository;
use lore_revision::repository::RepositoryContext;
use lore_revision::repository::RepositoryMetadata;
use tonic::Request;
use tonic::Response;
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

/// Claims of a token from [`mint`], or `None` when it is not one of ours or has expired.
fn verify_minted(token: &str) -> Option<AuthorizationToken> {
    let mut validation = jsonwebtoken::Validation::new(Algorithm::HS256);
    validation.set_audience(&[AUDIENCE]);
    validation.leeway = 0;
    jsonwebtoken::decode::<AuthorizationToken>(
        token,
        &jsonwebtoken::DecodingKey::from_secret(SECRET),
        &validation,
    )
    .ok()
    .map(|data| data.claims)
}

/// What the broker double answers, and what it was asked.
#[derive(Default)]
pub(crate) struct BrokerState {
    /// Callers (`sub`) that may delete, as with `LORE_AUTH_ADMINS`.
    pub admins: HashSet<String>,
    /// Replaces the computed CheckUserPermission `allowed_resource_permission`.
    pub check_answer: Option<Vec<auth::ResourcePermission>>,
    /// Replaces the computed LookupUserPermissions `resource_permission`.
    pub lookup_answer: Option<Vec<auth::ResourcePermission>>,
    pub checked: Vec<String>,
    pub created: Vec<String>,
    pub deleted: Vec<String>,
}

/// An in-process ucs-auth broker. Like GRID's, it verifies the caller's bearer and answers from
/// that token's own grants: Check returns each requested id the grants cover, exactly or via
/// `urc-*`, with the caller's real permissions; CreateResource needs `write`; DeleteResource
/// needs an admin caller.
pub(crate) struct FakeBroker {
    pub url: String,
    state: Arc<Mutex<BrokerState>>,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

impl FakeBroker {
    pub(crate) async fn start() -> Self {
        let state = Arc::new(Mutex::new(BrokerState::default()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind broker");
        let url = format!("http://{}", listener.local_addr().expect("broker address"));
        let (shutdown, stopped) = tokio::sync::oneshot::channel::<()>();
        let server = tonic::transport::Server::builder()
            .add_service(AuthApi(state.clone()))
            .add_service(RebacApi(state.clone()))
            .serve_with_incoming_shutdown(
                tonic::transport::server::TcpIncoming::from(listener),
                async {
                    let _ = stopped.await;
                },
            );
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(server);
        Self {
            url,
            state,
            _shutdown: shutdown,
        }
    }

    pub(crate) fn state(&self) -> MutexGuard<'_, BrokerState> {
        self.state.lock().expect("broker state")
    }
}

pub(crate) fn broker_grant(resource_id: &str, permission: &[&str]) -> auth::ResourcePermission {
    auth::ResourcePermission {
        resource_id: resource_id.to_string(),
        permission: permission.iter().map(|p| (*p).to_string()).collect(),
    }
}

fn caller<T>(request: &Request<T>) -> Result<AuthorizationToken, Status> {
    request
        .metadata()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .and_then(verify_minted)
        .ok_or_else(|| Status::unauthenticated("caller token invalid"))
}

fn granted(claims: &AuthorizationToken, resource_id: &str) -> Vec<String> {
    let mut permission: Vec<String> = Vec::new();
    for grant in claims.resources.iter().flatten() {
        if grant.resource_id == resource_id || grant.resource_id == "urc-*" {
            for held in &grant.permission {
                if !permission.contains(held) {
                    permission.push(held.clone());
                }
            }
        }
    }
    permission
}

fn check(
    state: &Mutex<BrokerState>,
    request: Request<auth::CheckUserPermissionRequest>,
) -> Result<Response<auth::CheckUserPermissionResponse>, Status> {
    let claims = caller(&request)?;
    let mut state = state.lock().expect("broker state");
    let requested = request.into_inner().resource_id;
    state.checked.extend(requested.iter().cloned());
    let mut allowed = Vec::new();
    let mut denied = Vec::new();
    for resource_id in requested {
        let permission = granted(&claims, &resource_id);
        let entry = auth::ResourcePermission {
            resource_id,
            permission,
        };
        if entry.permission.is_empty() {
            denied.push(entry);
        } else {
            allowed.push(entry);
        }
    }
    Ok(Response::new(auth::CheckUserPermissionResponse {
        allowed_resource_permission: state.check_answer.clone().unwrap_or(allowed),
        denied_resource_permission: denied,
    }))
}

fn lookup(
    state: &Mutex<BrokerState>,
    request: Request<auth::LookupUserPermissionsRequest>,
) -> Result<Response<auth::LookupUserPermissionsResponse>, Status> {
    let claims = caller(&request)?;
    let own = claims
        .resources
        .unwrap_or_default()
        .into_iter()
        .map(|grant| auth::ResourcePermission {
            resource_id: grant.resource_id,
            permission: grant.permission,
        })
        .collect();
    let state = state.lock().expect("broker state");
    Ok(Response::new(auth::LookupUserPermissionsResponse {
        resource_permission: state.lookup_answer.clone().unwrap_or(own),
        next_page_token: None,
    }))
}

fn create(
    state: &Mutex<BrokerState>,
    request: Request<rebac::CreateResourceRequest>,
) -> Result<Response<rebac::CreateResourceResponse>, Status> {
    let claims = caller(&request)?;
    let resource_id = request.into_inner().resource_id;
    if !granted(&claims, &resource_id).iter().any(|p| p == "write") {
        return Err(Status::permission_denied("creating needs write access"));
    }
    state
        .lock()
        .expect("broker state")
        .created
        .push(resource_id);
    Ok(Response::new(rebac::CreateResourceResponse {}))
}

fn delete(
    state: &Mutex<BrokerState>,
    request: Request<rebac::DeleteResourceRequest>,
) -> Result<Response<rebac::DeleteResourceResponse>, Status> {
    let claims = caller(&request)?;
    let mut state = state.lock().expect("broker state");
    if !state.admins.contains(&claims.user_id) {
        return Err(Status::permission_denied(
            "deleting is restricted to admins",
        ));
    }
    state.deleted.push(request.into_inner().resource_id);
    Ok(Response::new(rebac::DeleteResourceResponse {}))
}

type HttpResponse = http::Response<tonic::body::Body>;
type HttpFuture = Pin<Box<dyn Future<Output = Result<HttpResponse, Infallible>> + Send>>;

struct Unary<F>(F);

impl<Req, Resp, F> tonic::server::UnaryService<Req> for Unary<F>
where
    F: FnMut(Request<Req>) -> Result<Response<Resp>, Status>,
{
    type Response = Resp;
    type Future = std::future::Ready<Result<Response<Resp>, Status>>;

    fn call(&mut self, request: Request<Req>) -> Self::Future {
        std::future::ready((self.0)(request))
    }
}

fn unary<Req, Resp, F>(request: http::Request<tonic::body::Body>, handler: F) -> HttpFuture
where
    Req: tonic_prost::prost::Message + Default + Send + 'static,
    Resp: tonic_prost::prost::Message + Send + 'static,
    F: FnMut(Request<Req>) -> Result<Response<Resp>, Status> + Send + 'static,
{
    Box::pin(async move {
        let mut grpc = tonic::server::Grpc::new(tonic_prost::ProstCodec::<Resp, Req>::default());
        Ok(grpc.unary(Unary(handler), request).await)
    })
}

fn unimplemented() -> HttpFuture {
    Box::pin(async { Ok(Status::unimplemented("not part of the broker double").into_http()) })
}

#[derive(Clone)]
struct AuthApi(Arc<Mutex<BrokerState>>);

impl tonic::server::NamedService for AuthApi {
    const NAME: &'static str = "epic_urc.UrcAuthApi";
}

impl tonic::codegen::Service<http::Request<tonic::body::Body>> for AuthApi {
    type Response = HttpResponse;
    type Error = Infallible;
    type Future = HttpFuture;

    fn poll_ready(&mut self, _: &mut TaskContext<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> HttpFuture {
        let state = self.0.clone();
        match request.uri().path() {
            "/epic_urc.UrcAuthApi/CheckUserPermission" => {
                unary(request, move |request| check(&state, request))
            }
            "/epic_urc.UrcAuthApi/LookupUserPermissions" => {
                unary(request, move |request| lookup(&state, request))
            }
            _ => unimplemented(),
        }
    }
}

#[derive(Clone)]
struct RebacApi(Arc<Mutex<BrokerState>>);

impl tonic::server::NamedService for RebacApi {
    const NAME: &'static str = "ucs.auth.RebacApi";
}

impl tonic::codegen::Service<http::Request<tonic::body::Body>> for RebacApi {
    type Response = HttpResponse;
    type Error = Infallible;
    type Future = HttpFuture;

    fn poll_ready(&mut self, _: &mut TaskContext<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> HttpFuture {
        let state = self.0.clone();
        match request.uri().path() {
            "/ucs.auth.RebacApi/CreateResource" => {
                unary(request, move |request| create(&state, request))
            }
            "/ucs.auth.RebacApi/DeleteResource" => {
                unary(request, move |request| delete(&state, request))
            }
            _ => unimplemented(),
        }
    }
}
