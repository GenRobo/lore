// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Refuses requests from a client built at a different commit than this server.
//!
//! Applied to the whole router rather than per-service, because service
//! interceptors are only attached when auth is enabled and an auth-off server
//! needs the same guard.
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use http::HeaderValue;
use http::Request;
use http::Response;
use lore_base::version::lore_build_sha_is_unknown;
use lore_base::version::lore_build_sha_short;
use lore_transport::grpc::build_sha_from_user_agent;
use lore_transport::grpc::user_agent_requests_override;
use tonic::Code;
use tonic::body::Body;
use tower::Layer;
use tower::Service;

const GRPC_STATUS_HEADER: &str = "grpc-status";
const GRPC_MESSAGE_HEADER: &str = "grpc-message";
const CONTENT_TYPE_GRPC: &str = "application/grpc";

#[derive(Clone, Copy, Debug)]
pub struct BuildGuardConfig {
    /// Require the client's build commit to equal this server's.
    pub enforce: bool,
    /// Honour a client that explicitly asks to be admitted despite a mismatch.
    pub allow_client_override: bool,
}

impl Default for BuildGuardConfig {
    fn default() -> Self {
        Self {
            enforce: true,
            allow_client_override: true,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    Allow,
    /// Mismatched, but the client asked for it and the server permits that.
    AllowOverridden { client: String },
    Deny { message: String },
}

/// Decide whether a peer may proceed, given its user agent.
pub(crate) fn evaluate(config: &BuildGuardConfig, user_agent: Option<&str>) -> Verdict {
    if !config.enforce {
        return Verdict::Allow;
    }
    // A server that cannot name its own commit has nothing to compare against.
    // Refusing every client here would turn a build-provenance gap into a total
    // outage, so it degrades to permissive.
    if lore_build_sha_is_unknown() {
        return Verdict::Allow;
    }

    let server = lore_build_sha_short();
    let client = user_agent.and_then(build_sha_from_user_agent);
    if client == Some(server) {
        return Verdict::Allow;
    }

    let client_desc = client.unwrap_or("unknown").to_string();
    if config.allow_client_override && user_agent.is_some_and(user_agent_requests_override) {
        return Verdict::AllowOverridden {
            client: client_desc,
        };
    }
    Verdict::Deny {
        message: format!(
            "lore build mismatch: client {client_desc}, server {server}. \
             Deploy matching builds, or set LORE_ALLOW_BUILD_MISMATCH=1 on the client to override."
        ),
    }
}

/// A trailers-only gRPC error response, which is how a rejection before the
/// service is reached must be reported.
fn deny_response(message: &str) -> Response<Body> {
    let mut response = Response::new(Body::empty());
    let headers = response.headers_mut();
    headers.insert(
        http::header::CONTENT_TYPE,
        HeaderValue::from_static(CONTENT_TYPE_GRPC),
    );
    headers.insert(
        GRPC_STATUS_HEADER,
        HeaderValue::from(Code::FailedPrecondition as i32),
    );
    if let Ok(value) = HeaderValue::from_str(message) {
        headers.insert(GRPC_MESSAGE_HEADER, value);
    }
    response
}

#[derive(Clone, Copy, Debug)]
pub struct BuildGuardLayer {
    config: BuildGuardConfig,
}

impl BuildGuardLayer {
    pub fn new(config: BuildGuardConfig) -> Self {
        Self { config }
    }
}

impl<S> Layer<S> for BuildGuardLayer {
    type Service = BuildGuardService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        BuildGuardService {
            inner,
            config: self.config,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BuildGuardService<S> {
    inner: S,
    config: BuildGuardConfig,
}

impl<S> Service<Request<Body>> for BuildGuardService<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<Body>) -> Self::Future {
        let user_agent = request
            .headers()
            .get(http::header::USER_AGENT)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);

        match evaluate(&self.config, user_agent.as_deref()) {
            Verdict::Allow => Box::pin(self.inner.call(request)),
            Verdict::AllowOverridden { client } => {
                tracing::warn!(
                    client_build = %client,
                    server_build = %lore_build_sha_short(),
                    "admitting a build-mismatched client on its own request (LORE_ALLOW_BUILD_MISMATCH)"
                );
                Box::pin(self.inner.call(request))
            }
            Verdict::Deny { message } => {
                tracing::warn!("{message}");
                Box::pin(async move { Ok(deny_response(&message)) })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ua_for(sha: &str) -> String {
        format!("lore-transport/0.8.6-nightly+0 build/{sha}")
    }

    #[test]
    fn matching_build_is_allowed() {
        let ua = ua_for(lore_build_sha_short());
        assert_eq!(
            evaluate(&BuildGuardConfig::default(), Some(&ua)),
            Verdict::Allow
        );
    }

    #[test]
    fn mismatched_build_is_denied_with_both_sides_named() {
        let ua = ua_for("000000000000");
        let verdict = evaluate(&BuildGuardConfig::default(), Some(&ua));
        let Verdict::Deny { message } = verdict else {
            panic!("expected denial, got {verdict:?}");
        };
        assert!(message.contains("000000000000"), "{message}");
        assert!(message.contains(lore_build_sha_short()), "{message}");
        assert!(message.contains("LORE_ALLOW_BUILD_MISMATCH"), "{message}");
    }

    #[test]
    fn a_client_advertising_no_build_is_denied() {
        // Pre-handshake clients cannot prove a match, so they must not be
        // silently treated as matching.
        let verdict = evaluate(
            &BuildGuardConfig::default(),
            Some("lore-transport/0.8.6-nightly+0"),
        );
        assert!(matches!(verdict, Verdict::Deny { .. }), "{verdict:?}");
        assert!(matches!(
            evaluate(&BuildGuardConfig::default(), None),
            Verdict::Deny { .. }
        ));
    }

    #[test]
    fn client_override_is_honoured_when_the_server_permits_it() {
        let ua = format!("{} build-override/1", ua_for("000000000000"));
        assert_eq!(
            evaluate(&BuildGuardConfig::default(), Some(&ua)),
            Verdict::AllowOverridden {
                client: "000000000000".to_string()
            }
        );
    }

    #[test]
    fn client_override_is_refused_when_the_server_forbids_it() {
        let ua = format!("{} build-override/1", ua_for("000000000000"));
        let config = BuildGuardConfig {
            enforce: true,
            allow_client_override: false,
        };
        assert!(matches!(
            evaluate(&config, Some(&ua)),
            Verdict::Deny { .. }
        ));
    }

    #[test]
    fn enforcement_off_allows_anything() {
        let config = BuildGuardConfig {
            enforce: false,
            allow_client_override: false,
        };
        assert_eq!(evaluate(&config, Some(&ua_for("000000000000"))), Verdict::Allow);
        assert_eq!(evaluate(&config, None), Verdict::Allow);
    }

    #[test]
    fn denial_carries_a_failed_precondition_status() {
        let response = deny_response("nope");
        assert_eq!(
            response.headers().get(GRPC_STATUS_HEADER).unwrap(),
            &HeaderValue::from(Code::FailedPrecondition as i32)
        );
        assert_eq!(response.headers().get(GRPC_MESSAGE_HEADER).unwrap(), "nope");
    }
}
