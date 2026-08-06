// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use jsonwebtoken::DecodingKey;
use jsonwebtoken::jwk::Jwk;
use jsonwebtoken::jwk::JwkSet;
use lore_telemetry::InstrumentProvider;
use lore_telemetry::LabelArray;
use lore_telemetry::METRICS_OPERATION_LATENCY_METRIC_NAME;
use lore_telemetry::timed;
use lore_telemetry::timer::TimedResult;
use lore_transport::grpc::user_agent;
use opentelemetry::KeyValue;
use serde::Deserialize;
use smallvec::SmallVec;
use thiserror::Error;
use tracing::warn;

#[derive(Clone)]
struct JWKServiceKey {
    #[allow(dead_code)]
    jwk: Jwk,
    decoding_key: DecodingKey,
    algorithm: jsonwebtoken::Algorithm,
}

#[derive(Clone, Default, Deserialize, Debug)]
pub struct JWKServiceSettings {
    pub endpoint: String,
}

#[derive(Error, Debug)]
pub enum JWKServiceError {
    #[error("Internal Error")]
    InternalError,
    #[error("Could not parse jwks endpoint response")]
    ParseError(#[from] serde_json::Error),
    #[error("Could not decode jwk key")]
    DecodingError(#[from] jsonwebtoken::errors::Error),
    #[error("Key for kid not found")]
    NotFound,
}

#[async_trait]
pub trait JWKService: Send + Sync {
    /// Get the public key for the specified key id. Note: this may potentially result in a network
    /// call if the key for key id is not already cached locally by the implementer of this trait.
    async fn get_key(
        &self,
        kid: &str,
    ) -> Result<(DecodingKey, jsonwebtoken::Algorithm), JWKServiceError>;
}

// Bounds a JWKS refresh; without it a hung endpoint pins the refresh lock until
// TCP gives up, and every verification that misses the cache queues behind it.
const JWKS_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Clone, Default)]
pub struct JwkServiceImpl {
    // allow to be refetched from different threads if needed
    cached_set: Arc<tokio::sync::RwLock<HashMap<String, JWKServiceKey>>>,
    // Single-flights refreshes: concurrent misses queue here instead of racing
    // duplicate fetches, and the fetch itself runs holding only this lock —
    // never `cached_set`, which every JWT verification reads.
    refresh_lock: Arc<tokio::sync::Mutex<()>>,
    #[allow(dead_code)]
    settings: JWKServiceSettings,
}

impl JwkServiceImpl {
    pub fn new(settings: JWKServiceSettings) -> Self {
        JwkServiceImpl {
            cached_set: Default::default(),
            refresh_lock: Default::default(),
            settings,
        }
    }

    async fn get_cached_key(
        &self,
        kid: &str,
    ) -> Result<(DecodingKey, jsonwebtoken::Algorithm), JWKServiceError> {
        let keys = self.cached_set.read().await;
        let res = keys.get(kid).ok_or(JWKServiceError::NotFound)?;
        Ok((res.decoding_key.clone(), res.algorithm))
    }

    /// Fetch the latest keys and replace the local cache. If `desired` is not-`None`,
    /// short-circuits if the key id is already present in the local cache.
    pub async fn fetch_new_keys(&self, desired: Option<&str>) -> Result<(), JWKServiceError> {
        // The shared cache is deliberately NOT locked across the fetch:
        // `get_cached_key` takes `cached_set.read()` on every JWT verification,
        // so holding its write guard through a network round trip would stall
        // all token verification for the duration — indefinitely, for a hung
        // endpoint. The write guard is taken only at the end, to install the
        // result.
        let _refresh = self.refresh_lock.lock().await;

        // Check to see if the desired key was fetched while we waited for the lock.
        if let Some(d) = desired {
            if self.cached_set.read().await.contains_key(d) {
                return Ok(());
            }
        }

        let endpoint = reqwest::Url::parse(&self.settings.endpoint).map_err(|e| {
            warn!("failed to parse JWKS endpoint as a URL: {e:?}");
            JWKServiceError::InternalError
        })?;

        let is_file = endpoint.scheme() == "file";

        let response_body = if is_file {
            let path = endpoint.to_file_path().map_err(|_err| {
                warn!("failed to resolve JWKS file:// endpoint to a path: {endpoint}");
                JWKServiceError::InternalError
            })?;

            tokio::fs::read_to_string(&path).await.map_err(|e| {
                warn!("failed to read JWKS file at {}: {e:?}", path.display());
                JWKServiceError::InternalError
            })?
        } else {
            let client = reqwest::Client::builder()
                .user_agent(user_agent())
                .timeout(JWKS_FETCH_TIMEOUT)
                .build()
                .map_err(|e| {
                    warn!("failed to construct HTTP client: {e:?}");
                    JWKServiceError::InternalError
                })?;

            let response = timed!(
                self.latency_histogram_ms(METRICS_OPERATION_LATENCY_METRIC_NAME),
                &self.get_labels_for_operation_context("get_keys"),
                {
                    client.get(endpoint).send().await.map_err(|e| {
                        warn!("failed to fetch JWKS endpoint: {e:?}");
                        JWKServiceError::InternalError
                    })
                }
            )
            .result?;

            let status = response.status();
            let body = response.text().await.map_err(|e| {
                warn!("failed to get response body from JWKS endpoint result: {e:?}");
                JWKServiceError::InternalError
            })?;

            if !status.is_success() {
                warn!("JWKS endpoint returned error. Status: {status}, response: {body}");

                return Err(JWKServiceError::InternalError);
            }

            body
        };

        let new_jwks: JwkSet = serde_json::from_str(response_body.as_str()).map_err(|e| {
            if is_file {
                warn!("invalid JWKS file contents: {response_body}");
            } else {
                warn!("failed to parse JWKS response: {response_body}");
            }
            JWKServiceError::ParseError(e)
        })?;

        let mut new_set = HashMap::new();

        for jwk in new_jwks.keys {
            let kid = jwk
                .common
                .key_id
                .as_ref()
                .ok_or(JWKServiceError::InternalError)?;

            let algorithm = jwk
                .common
                .key_algorithm
                .ok_or(JWKServiceError::InternalError)?;
            let algorithm = jsonwebtoken::Algorithm::from_str(&algorithm.to_string())
                .map_err(JWKServiceError::DecodingError)?;

            new_set.insert(
                kid.clone(),
                JWKServiceKey {
                    decoding_key: DecodingKey::from_jwk(&jwk)
                        .map_err(JWKServiceError::DecodingError)?,
                    jwk,
                    algorithm,
                },
            );
        }

        *self.cached_set.write().await = new_set;

        Ok(())
    }
}

#[async_trait]
impl JWKService for JwkServiceImpl {
    async fn get_key(
        &self,
        kid: &str,
    ) -> Result<(DecodingKey, jsonwebtoken::Algorithm), JWKServiceError> {
        let key = self.get_cached_key(kid).await;

        match key {
            Ok(_) => key,
            Err(JWKServiceError::NotFound) => {
                // one more try after fetch
                self.fetch_new_keys(Some(kid)).await?;
                self.get_cached_key(kid).await
            }
            Err(e) => Err(e),
        }
    }
}

impl InstrumentProvider for JwkServiceImpl {
    fn namespace(&self) -> &'static str {
        "urc.auth.jwk_service"
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use axum::Json;
    use axum::Router;
    use axum::extract::State;
    use axum::routing::get;
    use serde_json::json;
    use tokio::net::TcpListener;

    use super::*;

    async fn jwks_handler(State(requests): State<Arc<AtomicUsize>>) -> Json<serde_json::Value> {
        let kid = if requests.fetch_add(1, Ordering::SeqCst) == 0 {
            "old-kid"
        } else {
            "new-kid"
        };

        Json(json!({
            "keys": [{
                "kty": "oct",
                "use": "sig",
                "kid": kid,
                "alg": "HS256",
                "k": "c2VjcmV0"
            }]
        }))
    }

    async fn spawn_jwks_server(requests: Arc<AtomicUsize>) -> SocketAddr {
        let app = Router::new()
            .route("/jwks", get(jwks_handler))
            .with_state(requests);
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test jwks server");
        let address = listener.local_addr().expect("get test jwks server address");

        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve test jwks server");
        });

        address
    }

    #[tokio::test]
    async fn fetch_new_keys_refreshes_when_desired_key_is_missing() {
        let requests = Arc::new(AtomicUsize::new(0));
        let address = spawn_jwks_server(requests.clone()).await;
        let service = JwkServiceImpl::new(JWKServiceSettings {
            endpoint: format!("http://{address}/jwks"),
        });

        service
            .fetch_new_keys(None)
            .await
            .expect("initial key fetch should succeed");

        service
            .get_key("new-kid")
            .await
            .expect("missing desired key should trigger a refresh");

        assert_eq!(requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn loads_keys_from_file_url() {
        let temp_dir = std::env::temp_dir();
        let jwks_path = temp_dir.join("jwk_test_loads_keys_from_file_url.json");

        std::fs::write(
            &jwks_path,
            r#"{"keys":[{"kty":"EC","crv":"P-256","x":"MKBCTNIcKUSDii11ySs3526iDZ8AiTo7Tu6KPAqv7D4","y":"4Etl6SRW2YiLUrN5vfvVHuhp7x8PxltmWWlbbM4IFGI","alg":"ES256","kid":"test-key-1"}]}"#,
        )
        .unwrap();

        let endpoint = reqwest::Url::from_file_path(&jwks_path)
            .unwrap()
            .to_string();
        let settings = JWKServiceSettings { endpoint };
        let service = JwkServiceImpl::new(settings);

        let result = service.fetch_new_keys(None).await;
        assert!(result.is_ok(), "{result:?}");

        let (_, algorithm) = service
            .get_key("test-key-1")
            .await
            .expect("key should be cached after loading from file");
        assert_eq!(algorithm, jsonwebtoken::Algorithm::ES256);

        std::fs::remove_file(&jwks_path).ok();
    }

    #[tokio::test]
    async fn cached_keys_stay_readable_while_a_refresh_is_stuck() {
        // An endpoint that accepts connections but never answers: the refresh
        // blocks until the client timeout.
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stall endpoint");
        let address = listener.local_addr().expect("stall endpoint addr");
        let _listener = listener;

        let service = JwkServiceImpl::new(JWKServiceSettings {
            endpoint: format!("http://{address}/jwks"),
        });

        let jwk: Jwk = serde_json::from_value(json!({
            "kty": "oct",
            "use": "sig",
            "kid": "cached-kid",
            "alg": "HS256",
            "k": "c2VjcmV0"
        }))
        .expect("test jwk");
        service.cached_set.write().await.insert(
            "cached-kid".to_string(),
            JWKServiceKey {
                decoding_key: DecodingKey::from_jwk(&jwk).expect("decoding key"),
                jwk,
                algorithm: jsonwebtoken::Algorithm::HS256,
            },
        );

        let stuck = {
            let service = service.clone();
            tokio::spawn(async move { service.get_key("missing-kid").await })
        };
        // Give the refresh a moment to reach the endpoint and block on it.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // The regression: with the cache write guard held across the fetch this
        // read times out; with the fetch outside the cache lock it answers
        // immediately.
        tokio::time::timeout(
            std::time::Duration::from_millis(500),
            service.get_key("cached-kid"),
        )
        .await
        .expect("cached key must stay readable while a refresh is in flight")
        .expect("cached key resolves");

        stuck.abort();
    }

    #[tokio::test]
    async fn file_url_missing_file_returns_error() {
        let settings = JWKServiceSettings {
            endpoint: "file:///tmp/jwk_test_file_that_does_not_exist.json".to_string(),
        };
        let service = JwkServiceImpl::new(settings);

        let result = service.fetch_new_keys(None).await;

        assert!(result.is_err());
    }
}
