// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Fixtures for the end-to-end tests: GRID-shaped signed tokens, a ucs-auth broker double, a
//! token exchange for the Lore client, and an origin serving the production gRPC router, and on
//! request the public QUIC endpoint, with JWT verification on.
#![allow(dead_code)]

use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::OnceLock;
use std::task::Context as TaskContext;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use async_trait::async_trait;
use jsonwebtoken::Algorithm;
use jsonwebtoken::DecodingKey;
use jsonwebtoken::EncodingKey;
use jsonwebtoken::Header;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Hash;
use lore_proto::auth;
use lore_proto::rebac;
use lore_revision::environment::EnvironmentConfig;
use lore_revision::event::LoreEvent;
use lore_revision::filter::Filter;
use lore_revision::instance::InstanceId;
use lore_revision::interface::ExecutionContext;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::lore::BranchId;
use lore_revision::lore::RepositoryId;
use lore_revision::notification::NotificationSender as _;
use lore_revision::relay::EventDispatcher;
use lore_revision::repository;
use lore_revision::repository::RepositoryContext;
use lore_revision::repository::RepositoryFormat;
use lore_revision::repository::RepositoryMetadata;
use lore_server::auth::jwk::JWKService;
use lore_server::auth::jwk::JWKServiceError;
use lore_server::auth::jwt::JwtVerifier;
use lore_server::grpc::notification_service::NotificationService;
use lore_server::grpc::server::FeatureSettings;
use lore_server::grpc::server::GrpcServerBuilder;
use lore_server::grpc::tower::build_guard::BuildGuardConfig;
use lore_server::hooks::HookDispatcher;
use lore_server::notification::local::NotificationSender;
use lore_server::quic::quinn::QuinnConfigBuilder;
use lore_server::quic::quinn::QuinnServer;
use lore_server::server::QuicPublicStreamHandler;
use lore_storage::ImmutableStore;
use lore_storage::MutableStore;
use lore_telemetry::user_agent_filter::UserAgentFilter;
use lore_transport::Connection;
use lore_transport::Endpoint;
use lore_transport::ProtocolError;
use lore_transport::traits::Authentication;
use lore_transport::types::AuthSession;
use lore_transport::types::AuthenticationToken;
use lore_transport::types::AuthorizationToken;
use lore_transport::types::ResolvedUser;
use serde_json::Value;
use serde_json::json;
use tonic::Request;
use tonic::Response;
use tonic::Status;

pub const SECRET: &[u8] = b"lore-end-to-end-test-secret";
pub const ISSUER: &str = "lore-e2e";
pub const AUDIENCE: &str = "lore-e2e";
/// The domain the Lore client sends tokens to; every token lists it as an audience.
pub const LOOPBACK: &str = "127.0.0.1";

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs()
}

pub fn urc(repository: RepositoryId) -> String {
    format!("urc-{repository}")
}

/// Sign `claims` with the key the origin's verifier trusts.
pub fn sign(claims: &Value) -> String {
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some("e2e".to_string());
    jsonwebtoken::encode(&header, claims, &EncodingKey::from_secret(SECRET)).expect("sign token")
}

fn resources(grants: &[(&str, &[&str])]) -> Value {
    Value::Array(
        grants
            .iter()
            .map(|(resource_id, permission)| {
                json!({ "resource_id": resource_id, "permission": permission })
            })
            .collect(),
    )
}

/// A token shaped like the ones GRID mints (`ucs_auth_service.py` `_mint`, `authtool.py`
/// `_base_claims`): an `idp` and a `resources` claim listing the caller's grants.
pub fn grid_token(
    sub: &str,
    idp: &str,
    service_account: bool,
    grants: &[(&str, &[&str])],
    ttl: u64,
) -> String {
    let now = unix_now();
    sign(&json!({
        "sub": sub,
        "iss": ISSUER,
        "iat": now,
        "exp": now + ttl,
        "aud": [AUDIENCE, LOOPBACK],
        "env": "dev",
        "name": sub,
        "preferred_username": sub,
        "idp": idp,
        "is_service_account": service_account,
        "resources": resources(grants),
    }))
}

/// A developer's browser-login identity: `urc-*` read and write, as the broker mints for
/// members of the GitHub organization.
pub fn developer(sub: &str) -> String {
    grid_token(sub, "github", false, &[("urc-*", &["read", "write"])], 3600)
}

/// A runtime deployment token from `authtool.py mint-deploy`: read on the given repositories.
pub fn deploy(sub: &str, repositories: &[RepositoryId]) -> String {
    let ids: Vec<String> = repositories.iter().map(|id| urc(*id)).collect();
    let grants: Vec<(&str, &[&str])> = ids.iter().map(|id| (id.as_str(), &["read"][..])).collect();
    grid_token(sub, "grid-interim", true, &grants, 3600)
}

/// A token with every identity claim but no `resources`, as another issuer might mint.
pub fn identity_without_resources(sub: &str) -> String {
    let now = unix_now();
    sign(&json!({
        "sub": sub,
        "iss": ISSUER,
        "iat": now,
        "exp": now + 3600,
        "aud": [AUDIENCE, LOOPBACK],
        "env": "dev",
        "name": sub,
        "preferred_username": sub,
        "is_service_account": false,
    }))
}

/// The claims of a token signed with [`SECRET`] that is valid now.
pub fn verify(token: &str) -> Option<Value> {
    let mut validation = jsonwebtoken::Validation::new(Algorithm::HS256);
    validation.set_audience(&[AUDIENCE]);
    validation.leeway = 0;
    jsonwebtoken::decode::<Value>(token, &DecodingKey::from_secret(SECRET), &validation)
        .ok()
        .map(|data| data.claims)
}

/// The permissions `claims` grants on `resource_id`, exactly or through `urc-*`.
pub fn granted(claims: &Value, resource_id: &str) -> Vec<String> {
    let mut permission: Vec<String> = Vec::new();
    for grant in claims["resources"].as_array().into_iter().flatten() {
        let id = grant["resource_id"].as_str().unwrap_or_default();
        if id == resource_id || id == "urc-*" {
            for held in grant["permission"].as_array().into_iter().flatten() {
                let held = held.as_str().unwrap_or_default().to_string();
                if !permission.contains(&held) {
                    permission.push(held);
                }
            }
        }
    }
    permission
}

fn writes(claims: &Value, resource_id: &str) -> bool {
    granted(claims, resource_id)
        .iter()
        .any(|permission| matches!(permission.as_str(), "write" | "owner" | "admin"))
}

struct Jwks;

#[async_trait]
impl JWKService for Jwks {
    async fn get_key(&self, _kid: &str) -> Result<(DecodingKey, Algorithm), JWKServiceError> {
        Ok((DecodingKey::from_secret(SECRET), Algorithm::HS256))
    }
}

/// The verifier an origin in tenant mode is configured with.
pub fn verifier() -> JwtVerifier {
    JwtVerifier {
        jwk_service: Arc::new(Jwks),
        jwt_issuer: Some(ISSUER.to_string()),
        jwt_audience: Some(vec![AUDIENCE.to_string()]),
    }
}

/// What the broker double answers, and what it was asked.
#[derive(Default)]
pub struct BrokerState {
    /// Callers (`sub`) that may delete, as with `LORE_AUTH_ADMINS`.
    pub admins: HashSet<String>,
    /// Replaces the computed CheckUserPermission answer.
    pub check_answer: Option<Vec<auth::ResourcePermission>>,
    /// Replaces the computed LookupUserPermissions answer.
    pub lookup_answer: Option<Vec<auth::ResourcePermission>>,
    pub checked: Vec<String>,
    pub created: Vec<String>,
    pub deleted: Vec<String>,
}

/// A ucs-auth broker double answering like GRID's hotfixed broker (`225da2718`): from the
/// caller's own verified grants, with read and write only; CreateResource needs write on the
/// id; DeleteResource needs an admin caller holding write on it.
pub struct Broker {
    pub url: String,
    state: Arc<Mutex<BrokerState>>,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

impl Broker {
    pub async fn start() -> Self {
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

    pub fn state(&self) -> MutexGuard<'_, BrokerState> {
        self.state.lock().expect("broker state")
    }
}

pub fn broker_grant(resource_id: &str, permission: &[&str]) -> auth::ResourcePermission {
    auth::ResourcePermission {
        resource_id: resource_id.to_string(),
        permission: permission.iter().map(|p| (*p).to_string()).collect(),
    }
}

fn caller<T>(request: &Request<T>) -> Result<Value, Status> {
    request
        .metadata()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .and_then(verify)
        .ok_or_else(|| Status::unauthenticated("caller token invalid"))
}

fn check(
    state: &Mutex<BrokerState>,
    request: Request<auth::CheckUserPermissionRequest>,
) -> Result<Response<auth::CheckUserPermissionResponse>, Status> {
    let claims = caller(&request)?;
    let mut state = state.lock().expect("broker state");
    let requested = request.into_inner().resource_id;
    state.checked.extend(requested.iter().cloned());
    let allowed = requested
        .into_iter()
        .filter_map(|resource_id| {
            let permission: Vec<String> = granted(&claims, &resource_id)
                .into_iter()
                .filter(|permission| permission == "read" || permission == "write")
                .collect();
            (!permission.is_empty()).then_some(auth::ResourcePermission {
                resource_id,
                permission,
            })
        })
        .collect();
    Ok(Response::new(auth::CheckUserPermissionResponse {
        allowed_resource_permission: state.check_answer.clone().unwrap_or(allowed),
        denied_resource_permission: Vec::new(),
    }))
}

fn lookup(
    state: &Mutex<BrokerState>,
    request: Request<auth::LookupUserPermissionsRequest>,
) -> Result<Response<auth::LookupUserPermissionsResponse>, Status> {
    let claims = caller(&request)?;
    let own = claims["resources"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|grant| auth::ResourcePermission {
            resource_id: grant["resource_id"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            permission: grant["permission"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|permission| permission.as_str().map(str::to_string))
                .collect(),
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
    if !writes(&claims, &resource_id) {
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
    let resource_id = request.into_inner().resource_id;
    let sub = claims["sub"].as_str().unwrap_or_default().to_string();
    let mut state = state.lock().expect("broker state");
    if !state.admins.contains(&sub) || !writes(&claims, &resource_id) {
        return Err(Status::permission_denied(
            "deleting a repository needs an origin admin with write access to it",
        ));
    }
    state.deleted.push(resource_id);
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

/// How the client-side token exchange answers for one auth URL.
#[derive(Clone, Default)]
pub struct ExchangePolicy {
    /// Lifetime of each session token, in seconds.
    pub ttl: u64,
    /// Refuse every exchange, as when the identity's grant has been revoked.
    pub refuse: bool,
    /// Wait this long before answering each exchange, as a slow or stalled broker would.
    pub delay: Duration,
    /// Exchanges asked for so far, answered or not.
    pub requested: usize,
    /// Session tokens issued so far.
    pub issued: usize,
}

fn exchange_policies() -> &'static Mutex<HashMap<String, ExchangePolicy>> {
    static POLICIES: OnceLock<Mutex<HashMap<String, ExchangePolicy>>> = OnceLock::new();
    POLICIES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn exchange_policy(auth_url: &str) -> ExchangePolicy {
    exchange_policies()
        .lock()
        .expect("exchange policies")
        .get(auth_url)
        .cloned()
        .unwrap_or_default()
}

pub fn set_exchange_policy(auth_url: &str, update: impl FnOnce(&mut ExchangePolicy)) {
    let mut policies = exchange_policies().lock().expect("exchange policies");
    update(policies.entry(auth_url.to_string()).or_default());
}

/// The Lore client's token exchange for the test broker's URL: like the broker's
/// ExchangeUserTokenForMultiresourceToken, it verifies the identity and issues a session token
/// for one repository with the identity's own permission on it, never outliving the identity.
struct SessionExchange;

fn not_authorized() -> ProtocolError {
    ProtocolError::from(lore_base::error::NotAuthorized)
}

fn unsupported() -> ProtocolError {
    ProtocolError::internal("not part of the test exchange")
}

#[async_trait]
impl Authentication for SessionExchange {
    async fn start_auth_session(
        &self,
        _auth_url: &str,
        _client_state: &str,
        _correlation_id: &str,
    ) -> Result<AuthSession, ProtocolError> {
        Err(unsupported())
    }

    async fn poll_auth_session(
        &self,
        _auth_url: &str,
        _client_state: &str,
        _session_code: &str,
        _correlation_id: &str,
    ) -> Result<Option<AuthenticationToken>, ProtocolError> {
        Err(unsupported())
    }

    async fn exchange_external_token(
        &self,
        _auth_url: &str,
        _token: &str,
        _token_type: &str,
        _correlation_id: &str,
    ) -> Result<AuthenticationToken, ProtocolError> {
        Err(unsupported())
    }

    async fn refresh_authentication(
        &self,
        _auth_url: &str,
        _refresh_token: &str,
        _correlation_id: &str,
    ) -> Result<AuthenticationToken, ProtocolError> {
        Err(unsupported())
    }

    async fn exchange_for_repository(
        &self,
        auth_url: &str,
        authn_token: &str,
        repository: RepositoryId,
        correlation_id: &str,
    ) -> Result<AuthorizationToken, ProtocolError> {
        self.exchange_for_custom_resource(auth_url, authn_token, &urc(repository), correlation_id)
            .await
    }

    async fn exchange_for_custom_resource(
        &self,
        auth_url: &str,
        authn_token: &str,
        resource_id: &str,
        _correlation_id: &str,
    ) -> Result<AuthorizationToken, ProtocolError> {
        let identity = verify(authn_token).ok_or_else(not_authorized)?;
        let permission = granted(&identity, resource_id);
        let delay = {
            let mut policies = exchange_policies().lock().expect("exchange policies");
            let policy = policies.entry(auth_url.to_string()).or_default();
            policy.requested += 1;
            policy.delay
        };
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let ttl = {
            let mut policies = exchange_policies().lock().expect("exchange policies");
            let policy = policies.entry(auth_url.to_string()).or_default();
            if policy.refuse || permission.is_empty() {
                return Err(not_authorized());
            }
            policy.issued += 1;
            policy.ttl
        };
        let now = unix_now();
        let expires = (now + ttl).min(identity["exp"].as_u64().unwrap_or(now));
        let token = sign(&json!({
            "sub": identity["sub"],
            "iss": ISSUER,
            "iat": now,
            "exp": expires,
            "aud": [AUDIENCE, LOOPBACK],
            "env": "dev",
            "name": identity["name"],
            "preferred_username": identity["preferred_username"],
            "idp": identity["idp"],
            "is_service_account": false,
            "token_use": "session",
            "resources": [{ "resource_id": resource_id, "permission": permission }],
        }));
        Ok(AuthorizationToken {
            token,
            expires_ms: expires * 1000,
            acceptable_root_domains: Vec::new(),
        })
    }

    async fn get_user_info(
        &self,
        _auth_url: &str,
        _authz_token: &str,
        _repository: RepositoryId,
        _user_ids: &[String],
        _correlation_id: &str,
    ) -> Result<Vec<ResolvedUser>, ProtocolError> {
        Err(unsupported())
    }

    async fn get_user_id(
        &self,
        _auth_url: &str,
        _authz_token: &str,
        _repository: RepositoryId,
        _display_name: &str,
        _correlation_id: &str,
    ) -> Result<Option<ResolvedUser>, ProtocolError> {
        Err(unsupported())
    }
}

/// Prepare this process to act as a Lore client: a private credential store, the test token
/// exchange for plain-http auth URLs, and the notification client.
pub fn client_environment() {
    static READY: OnceLock<PathBuf> = OnceLock::new();
    READY.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!(
            "lore-e2e-credentials-{}-{}",
            std::process::id(),
            rand::random::<u32>()
        ));
        std::fs::create_dir_all(&dir).expect("credential store directory");
        // SAFETY: set once, before any test in this binary reads the credential store.
        unsafe {
            std::env::set_var("LORE_AUTH_PATH", &dir);
            std::env::set_var("LORE_AUTH_STORE", "fallback");
        }
        lore_transport::auth::authentication::add("http", Arc::new(SessionExchange))
            .expect("register the test exchange");
        lore_notification::initialize();
        dir
    });
}

/// Store `token` as `identity`'s login for `auth_url`, as `lore auth login` would.
pub async fn store_identity(auth_url: &str, identity: &str, token: &str) {
    lore_credential::token_store::store_user_token(
        auth_url,
        identity,
        token,
        vec![LOOPBACK.to_string()],
    )
    .await
    .expect("store identity token");
}

/// A Lore client's execution context, recording the branches its notifications announce.
pub fn client(correlation_id: &str) -> (Arc<ExecutionContext>, Arc<Mutex<Vec<BranchId>>>) {
    let announced = Arc::new(Mutex::new(Vec::new()));
    let record = announced.clone();
    let dispatcher = EventDispatcher::new(Some(Box::new(move |event: &LoreEvent| {
        if let LoreEvent::NotificationBranchCreated(data) = event {
            record.lock().expect("announced branches").push(data.branch);
        }
    })));
    let globals = LoreGlobalArgs {
        correlation_id: correlation_id.to_string().into(),
        max_connections: 1,
        ..Default::default()
    };
    (
        Arc::new(ExecutionContext::new_client(globals, dispatcher)),
        announced,
    )
}

/// Log `identity` in on `origin` with a developer login, and have the exchange issue session
/// tokens that last `ttl` seconds.
pub async fn log_in(origin: &Origin, identity: &str, ttl: u64) {
    set_exchange_policy(&origin.broker.url, |policy| policy.ttl = ttl);
    store_identity(&origin.broker.url, identity, &developer(identity)).await;
}

pub async fn connect(remote: &str, identity: &str, repository: RepositoryId) -> Arc<Connection> {
    lore_transport::connect(remote, identity, repository, 1)
        .await
        .expect("connect to the origin")
}

/// Publish a new branch of `repository` until the client announces it, or give up after `within`.
pub async fn announced_in_time(
    origin: &Origin,
    repository: RepositoryId,
    announced: &Mutex<Vec<BranchId>>,
    within: Duration,
) -> bool {
    let branch: BranchId = rand::random();
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        origin
            .notifications
            .branch_created(repository, branch)
            .await;
        tokio::time::sleep(Duration::from_millis(250)).await;
        if announced
            .lock()
            .expect("announced branches")
            .contains(&branch)
        {
            return true;
        }
    }
    false
}

/// Poll `condition` every 250 ms until it holds, or give up after `within`.
pub async fn eventually(within: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    condition()
}

/// Whether `token` is a JWT whose expiry has not passed.
pub fn is_live(token: &str) -> bool {
    lore_credential::user_info_from_token(token.to_string()).is_some_and(|info| {
        u128::from(info.expires)
            > SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_millis()
    })
}

pub async fn repository_with_remote(
    connection: Arc<Connection>,
    id: RepositoryId,
) -> Arc<RepositoryContext> {
    let (immutable_store, mutable_store) = client_stores().await;
    Arc::new(RepositoryContext::new(
        None,
        immutable_store,
        mutable_store,
        id,
        InstanceId::default(),
        Ok(connection),
        Arc::new(Filter::default()),
        RepositoryFormat::Lore,
    ))
}

/// An origin in tenant mode: the production gRPC router with JWT verification and a broker
/// double, in-memory stores, and optionally the public QUIC endpoint on the same port.
pub struct Origin {
    pub port: u16,
    pub broker: Broker,
    pub immutable_store: Arc<dyn ImmutableStore>,
    pub mutable_store: Arc<dyn MutableStore>,
    pub execution: Arc<ExecutionContext>,
    pub notifications: Arc<NotificationSender>,
    quic: Option<QuinnServer>,
    _shutdown: tokio::sync::oneshot::Sender<()>,
}

impl Origin {
    pub async fn start() -> Self {
        Self::start_with(false).await
    }

    pub async fn start_with_quic() -> Self {
        Self::start_with(true).await
    }

    async fn start_with(quic: bool) -> Self {
        let broker = Broker::start().await;
        let execution = lore_server::util::setup_execution("e2e", String::new(), String::new());
        let (immutable_store, mutable_store) =
            LORE_CONTEXT.scope(execution.clone(), stores()).await;

        let (port, quic) = loop {
            let port = std::net::TcpListener::bind("127.0.0.1:0")
                .and_then(|listener| listener.local_addr())
                .expect("free port")
                .port();
            if !quic {
                break (port, None);
            }
            let started = LORE_CONTEXT.sync_scope(execution.clone(), || {
                start_quic(port, &immutable_store, &mutable_store)
            });
            if let Ok(server) = started {
                break (port, Some(server));
            }
        };

        let environment = EnvironmentConfig {
            endpoint: Some(Endpoint {
                auth_url: Some(broker.url.clone()),
                ..Default::default()
            }),
            config: None,
        };
        let notifications = Arc::new(NotificationSender::default());
        let router = GrpcServerBuilder::new()
            .with_environment(environment)
            .with_feature(FeatureSettings::default())
            .with_immutable_store(immutable_store.clone(), immutable_store.clone())
            .with_mutable_store(mutable_store.clone())
            .with_lock_store(None)
            .with_notification(
                notifications.clone(),
                Some(NotificationService::new(notifications.clone())),
            )
            .with_hook_dispatcher(Arc::new(HookDispatcher::empty()))
            .with_tls_config(None, None, None)
            .expect("no TLS")
            .with_admin_endpoints(HashMap::new(), vec![])
            .with_http2_config(
                None,
                None,
                Duration::from_secs(30),
                None,
                Arc::new(UserAgentFilter::default()),
                None,
                BuildGuardConfig {
                    enforce: false,
                    allow_client_override: false,
                },
            )
            .with_jwt_verifier(Some(verifier()))
            .expect("production router");

        let (shutdown, stopped) = tokio::sync::oneshot::channel::<()>();
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(LORE_CONTEXT.scope(execution.clone(), async move {
            router
                .serve(address, async {
                    let _ = stopped.await;
                })
                .await
                .expect("serve the router");
        }));
        for _ in 0..100 {
            if tokio::net::TcpStream::connect(address).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        Self {
            port,
            broker,
            immutable_store,
            mutable_store,
            execution,
            notifications,
            quic,
            _shutdown: shutdown,
        }
    }

    /// Where a tonic client reaches the router.
    pub fn grpc_endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// A tonic channel to the router.
    pub async fn channel(&self) -> tonic::transport::Channel {
        tonic::transport::Channel::from_shared(self.grpc_endpoint())
            .expect("endpoint")
            .connect()
            .await
            .expect("connect to the router")
    }

    /// Store a repository the way RepositoryCreate does, returning its metadata pointer.
    pub async fn seed(&self, id: RepositoryId, name: &str) -> Hash {
        let repository = Arc::new(RepositoryContext::new_server_context(
            self.immutable_store.clone(),
            self.mutable_store.clone(),
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
        LORE_CONTEXT
            .scope(self.execution.clone(), async move {
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
            })
            .await
    }

    /// The repository's metadata pointer, if it has one.
    pub async fn stored_metadata(&self, id: RepositoryId) -> Option<Hash> {
        let repository = Arc::new(RepositoryContext::new_server_context(
            self.immutable_store.clone(),
            self.mutable_store.clone(),
            id,
        ));
        LORE_CONTEXT
            .scope(
                self.execution.clone(),
                repository::metadata_hash(repository),
            )
            .await
            .ok()
            .filter(|hash| !hash.is_zero())
    }
}

impl Drop for Origin {
    fn drop(&mut self) {
        if let Some(quic) = self.quic.take()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            #[allow(clippy::disallowed_methods)]
            runtime.spawn(async move { quic.close().await });
        }
    }
}

/// In-memory stores, as a client's local stores.
pub async fn client_stores() -> (Arc<dyn ImmutableStore>, Arc<dyn MutableStore>) {
    stores().await
}

async fn stores() -> (Arc<dyn ImmutableStore>, Arc<dyn MutableStore>) {
    let immutable = lore_storage::local::immutable_store::create(
        None::<&str>,
        lore_storage::local::immutable_store::ImmutableStoreCreateOptions::none(),
        false,
        lore_storage::local::immutable_store::ImmutableStoreSettings::default(),
    )
    .await
    .expect("immutable store");
    let mutable: Arc<dyn MutableStore> = lore_storage::local::mutable_store::create(
        None::<&str>,
        lore_storage::MutableStoreSettings::default(),
        immutable.clone(),
    )
    .await
    .expect("mutable store");
    (immutable, mutable)
}

fn start_quic(
    port: u16,
    immutable_store: &Arc<dyn ImmutableStore>,
    mutable_store: &Arc<dyn MutableStore>,
) -> anyhow::Result<QuinnServer> {
    let certificates = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join("protocol")
        .join("test_data");
    let config = QuinnConfigBuilder::new()
        .server_metrics_name("e2e")
        .address(SocketAddr::from(([127, 0, 0, 1], port)))
        .cert_file(certificates.join("test_cert.pem"))
        .pkey_file(certificates.join("test_key.pem"))
        .cert_chain(Some(certificates.join("test_ca.pem")))
        .num_listeners(1)
        .stream_handler_factory(Box::new(QuicPublicStreamHandler::new(
            immutable_store.clone(),
            immutable_store.clone(),
            mutable_store.clone(),
            Some(verifier()),
            500,
            None,
        )))
        .build()?;
    QuinnServer::start(config)
}
