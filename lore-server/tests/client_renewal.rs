// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The Lore client against an origin in tenant mode, with authorization on end to end: long-lived
//! gRPC streams and QUIC sessions outlive the session tokens that opened them, and a copy reads
//! its source through a session the client holds on the copying connection, for that copy alone.

mod support;

use std::sync::Arc;
use std::time::Duration;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_revision::fragment::generate_random;
use lore_revision::lore::RepositoryId;
use lore_revision::store::remote::RemoteImmutableStore;
use lore_storage::ImmutableStore as _;
use lore_storage::StoreMatch;
use lore_transport::StorageSession;
use lore_transport::grpc::GRPCAuth;
use support::Origin;
use support::announced_in_time;
use support::client;
use support::client_environment;
use support::connect;
use support::deploy;
use support::eventually;
use support::exchange_policy;
use support::is_live;
use support::log_in;
use support::repository_with_remote;
use support::set_exchange_policy;
use support::store_identity;

/// Lifetime of the session tokens the exchange issues in the renewal tests.
const SESSION_TTL: u64 = 3;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subscription_continues_across_a_session_token_expiry() {
    client_environment();
    let origin = Origin::start().await;
    let identity = "notification-renewal";
    log_in(&origin, identity, SESSION_TTL).await;
    let repository: RepositoryId = rand::random();
    let remote = format!("grpc://127.0.0.1:{}", origin.port);
    let (execution, announced) = client("notification-renewal");

    LORE_CONTEXT
        .scope(execution, async {
            let connection = connect(&remote, identity, repository).await;
            let context = repository_with_remote(connection, repository).await;
            lore_revision::notification::subscribe(context)
                .await
                .expect("subscribe");
            assert!(
                announced_in_time(&origin, repository, &announced, Duration::from_secs(10)).await,
                "events flow while the session token is live"
            );

            tokio::time::sleep(Duration::from_secs(SESSION_TTL + 1)).await;
            assert!(
                announced_in_time(&origin, repository, &announced, Duration::from_secs(20)).await,
                "the subscription must be renewed once its session token expires"
            );
            assert!(
                exchange_policy(&origin.broker.url).issued >= 2,
                "the expired session token was replaced"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lapsed_subscription_stops_and_subscribing_again_reopens_it() {
    client_environment();
    let origin = Origin::start().await;
    let identity = "notification-lapse";
    log_in(&origin, identity, SESSION_TTL).await;
    let repository: RepositoryId = rand::random();
    let remote = format!("grpc://127.0.0.1:{}", origin.port);
    let (execution, announced) = client("notification-lapse");

    LORE_CONTEXT
        .scope(execution, async {
            let connection = connect(&remote, identity, repository).await;
            let context = repository_with_remote(connection, repository).await;
            lore_revision::notification::subscribe(context.clone())
                .await
                .expect("subscribe");
            assert!(
                announced_in_time(&origin, repository, &announced, Duration::from_secs(10)).await,
                "events flow while the session token is live"
            );

            // The grant is revoked: no session token can be renewed.
            set_exchange_policy(&origin.broker.url, |policy| policy.refuse = true);
            tokio::time::sleep(Duration::from_secs(SESSION_TTL + 1)).await;
            assert!(
                !announced_in_time(&origin, repository, &announced, Duration::from_secs(8)).await,
                "no events once the authorization has lapsed"
            );

            set_exchange_policy(&origin.broker.url, |policy| policy.refuse = false);
            lore_revision::notification::subscribe(context)
                .await
                .expect("subscribe again");
            assert!(
                announced_in_time(&origin, repository, &announced, Duration::from_secs(20)).await,
                "subscribing again must replace the ended subscription"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_storage_streams_continue_across_a_session_token_expiry() {
    client_environment();
    let origin = Origin::start().await;
    let identity = "storage-renewal";
    log_in(&origin, identity, SESSION_TTL).await;
    let repository: RepositoryId = rand::random();
    let remote = format!("grpc://127.0.0.1:{}", origin.port);
    let (execution, _) = client("storage-renewal");

    LORE_CONTEXT
        .scope(execution, async {
            let connection = connect(&remote, identity, repository).await;
            let session = connection
                .session(repository, "storage-renewal")
                .await
                .expect("storage session");

            let (fragment, first, payload) = generate_random();
            session
                .put(first, fragment, Some(payload))
                .await
                .expect("put while the session token is live");
            session
                .get(&first)
                .await
                .expect("get while the session token is live");

            // The open put and get streams outlive the session token that opened them.
            tokio::time::sleep(Duration::from_secs(SESSION_TTL + 1)).await;
            let (fragment, second, payload) = generate_random();
            session
                .put(second, fragment, Some(payload))
                .await
                .expect("a put after the token expired renews its stream");
            let (_, payload) = session
                .get(&second)
                .await
                .expect("a get after the token expired renews its stream");
            assert_eq!(payload.len(), fragment.size_payload as usize);
            assert!(
                exchange_policy(&origin.broker.url).issued >= 2,
                "the expired session token was replaced"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_grpc_storage_stream_is_refused_once_its_authorization_lapses() {
    client_environment();
    let origin = Origin::start().await;
    let identity = "storage-lapse";
    log_in(&origin, identity, SESSION_TTL).await;
    let repository: RepositoryId = rand::random();
    let remote = format!("grpc://127.0.0.1:{}", origin.port);
    let (execution, _) = client("storage-lapse");

    LORE_CONTEXT
        .scope(execution, async {
            let connection = connect(&remote, identity, repository).await;
            let session = connection
                .session(repository, "storage-lapse")
                .await
                .expect("storage session");
            let (fragment, first, payload) = generate_random();
            session
                .put(first, fragment, Some(payload))
                .await
                .expect("put while the session token is live");

            set_exchange_policy(&origin.broker.url, |policy| policy.refuse = true);
            tokio::time::sleep(Duration::from_secs(SESSION_TTL + 1)).await;
            let (fragment, second, payload) = generate_random();
            let refused = tokio::time::timeout(
                Duration::from_secs(60),
                session.put(second, fragment, Some(payload)),
            )
            .await
            .expect("a lapsed authorization fails the put rather than hanging");
            assert!(refused.is_err(), "no put without a live authorization");
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_remote_copy_reads_its_source_after_the_source_session_stopped() {
    client_environment();
    let origin = Origin::start_with_quic().await;
    let identity = "remote-copy";
    log_in(&origin, identity, 600).await;
    let source: RepositoryId = rand::random();
    let destination: RepositoryId = rand::random();
    let remote = format!("lore://127.0.0.1:{}", origin.port);
    let (execution, _) = client("remote-copy");

    LORE_CONTEXT
        .scope(execution, async {
            let store = Arc::new(RemoteImmutableStore::new(&remote, Some(identity)));
            let (fragment, address, payload) = generate_random();
            store
                .clone()
                .put(source, address, fragment, Some(payload), false)
                .await
                .expect("put into the source repository");

            // The content now lives only on the server. Stop the source session that put it.
            connect(&remote, identity, source)
                .await
                .release_session(source, "remote-copy");
            tokio::time::sleep(Duration::from_secs(2)).await;

            let context: Context = rand::random();
            store
                .clone()
                .copy(source, address, destination, context, true)
                .await
                .expect("a caller entitled to both repositories copies between them");
            let copied = Address {
                hash: address.hash,
                context,
            };
            assert_eq!(
                store
                    .clone()
                    .exist(destination, copied, StoreMatch::MatchFull)
                    .await
                    .expect("query the destination"),
                StoreMatch::MatchFull
            );
        })
        .await;
}

/// Put fresh content through `session` from `writers` concurrent writers, `each` puts apiece,
/// returning every failure.
async fn concurrent_puts(
    session: &Arc<StorageSession>,
    writers: usize,
    each: usize,
) -> Vec<String> {
    let writes = (0..writers).map(|_| async {
        let mut failures = Vec::new();
        for _ in 0..each {
            let (fragment, address, payload) = generate_random();
            if let Err(err) = session.put(address, fragment, Some(payload)).await {
                failures.push(err.to_string());
            }
        }
        failures
    });
    futures::future::join_all(writes)
        .await
        .into_iter()
        .flatten()
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subscription_whose_renewals_expire_at_once_stops() {
    client_environment();
    let origin = Origin::start().await;
    let identity = "notification-quick-expiry";
    // Every session token expires within a second of being issued, so each renewed subscription
    // ends almost as soon as it opens.
    log_in(&origin, identity, 1).await;
    let repository: RepositoryId = rand::random();
    let remote = format!("grpc://127.0.0.1:{}", origin.port);
    let (execution, _) = client("notification-quick-expiry");

    LORE_CONTEXT
        .scope(execution, async {
            let connection = connect(&remote, identity, repository).await;
            let context = repository_with_remote(connection, repository).await;
            lore_revision::notification::subscribe(context)
                .await
                .expect("subscribe");

            let mut last = exchange_policy(&origin.broker.url).requested;
            let mut quiet = 0;
            let settled = eventually(Duration::from_secs(30), || {
                let requested = exchange_policy(&origin.broker.url).requested;
                quiet = if requested == last { quiet + 1 } else { 0 };
                last = requested;
                quiet >= 16
            })
            .await;
            assert!(
                settled,
                "a subscription whose renewed tokens keep expiring at once must stop renewing"
            );
            assert!(
                exchange_policy(&origin.broker.url).issued >= 3,
                "the subscription renewed before it gave up"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_grpc_writes_across_a_token_expiry_all_succeed() {
    client_environment();
    let origin = Origin::start().await;
    let identity = "grpc-concurrent-renewal";
    log_in(&origin, identity, SESSION_TTL).await;
    let repository: RepositoryId = rand::random();
    let remote = format!("grpc://127.0.0.1:{}", origin.port);
    let (execution, _) = client("grpc-concurrent-renewal");

    LORE_CONTEXT
        .scope(execution, async {
            let connection = connect(&remote, identity, repository).await;
            let session = connection
                .session(repository, "grpc-concurrent-renewal")
                .await
                .expect("storage session");
            let (fragment, address, payload) = generate_random();
            session
                .put(address, fragment, Some(payload))
                .await
                .expect("put while the session token is live");

            tokio::time::sleep(Duration::from_secs(SESSION_TTL + 1)).await;
            // A slow exchange keeps the renewal open while writes pile up behind it.
            set_exchange_policy(&origin.broker.url, |policy| {
                policy.delay = Duration::from_millis(500)
            });
            let failures = concurrent_puts(&session, 16, 3).await;
            assert!(
                failures.is_empty(),
                "every write across the expiry succeeds: {failures:?}"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn renewing_past_a_replacement_token_that_has_expired_gets_a_live_one() {
    client_environment();
    let origin = Origin::start().await;
    let identity = "replacement-expiry";
    log_in(&origin, identity, SESSION_TTL).await;
    let repository: RepositoryId = rand::random();
    let remote = format!("grpc://127.0.0.1:{}", origin.port);
    let (execution, _) = client("replacement-expiry");

    LORE_CONTEXT
        .scope(execution, async {
            let connection = connect(&remote, identity, repository).await;
            let grpc = lore_transport::grpc::connect(Arc::downgrade(&connection), &remote, true)
                .await
                .expect("gRPC connection");
            let auth = grpc
                .repository_authz(&origin.broker.url, identity, repository)
                .await;
            // The token an idle stream was opened with.
            let opened_with = auth.read().authorization_token.clone();

            // Once it expires another holder installs a replacement, as the credential
            // refresher does, and the replacement expires in turn.
            tokio::time::sleep(Duration::from_secs(SESSION_TTL + 1)).await;
            GRPCAuth::renew(&auth, &opened_with)
                .await
                .expect("replace the expired token");
            let replacement = auth.read().authorization_token.clone();
            assert_ne!(replacement, opened_with);
            tokio::time::sleep(Duration::from_secs(SESSION_TTL + 1)).await;

            // The idle stream renews the token it was opened with.
            GRPCAuth::renew(&auth, &opened_with)
                .await
                .expect("renew the idle stream's token");
            let renewed = auth.read().authorization_token.clone();
            assert!(
                is_live(&renewed),
                "a renewal must not settle for a replacement token that has expired"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quic_session_keeps_hydrating_across_token_expiries() {
    client_environment();
    let origin = Origin::start_with_quic().await;
    let identity = "quic-renewal";
    log_in(&origin, identity, SESSION_TTL).await;
    let repository: RepositoryId = rand::random();
    let remote = format!("lore://127.0.0.1:{}", origin.port);
    let (execution, _) = client("quic-renewal");

    LORE_CONTEXT
        .scope(execution, async {
            let connection = connect(&remote, identity, repository).await;
            let session = connection
                .session(repository, "quic-renewal")
                .await
                .expect("storage session");
            let (fragment, first, payload) = generate_random();
            session
                .put(first, fragment, Some(payload))
                .await
                .expect("put while the session token is live");

            // One long-lived session outlives several of the tokens that authorize it.
            for expiry in 1..=2 {
                tokio::time::sleep(Duration::from_secs(SESSION_TTL + 1)).await;
                let (fragment, address, payload) = generate_random();
                session
                    .put(address, fragment, Some(payload))
                    .await
                    .unwrap_or_else(|err| {
                        panic!("a put after token expiry {expiry} renews the session: {err}")
                    });
                let (_, read) = session
                    .get(&address)
                    .await
                    .expect("get under the renewed session");
                assert_eq!(read.len(), fragment.size_payload as usize);
                session
                    .get(&first)
                    .await
                    .expect("content from before the renewal stays readable");
            }
            assert!(
                exchange_policy(&origin.broker.url).issued >= 3,
                "each expired session token was replaced"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_quic_writes_across_a_token_expiry_all_succeed() {
    client_environment();
    let origin = Origin::start_with_quic().await;
    let identity = "quic-concurrent-renewal";
    log_in(&origin, identity, SESSION_TTL).await;
    let repository: RepositoryId = rand::random();
    let remote = format!("lore://127.0.0.1:{}", origin.port);
    let (execution, _) = client("quic-concurrent-renewal");

    LORE_CONTEXT
        .scope(execution, async {
            let connection = connect(&remote, identity, repository).await;
            let session = connection
                .session(repository, "quic-concurrent-renewal")
                .await
                .expect("storage session");
            let (fragment, address, payload) = generate_random();
            session
                .put(address, fragment, Some(payload))
                .await
                .expect("put while the session token is live");

            tokio::time::sleep(Duration::from_secs(SESSION_TTL + 1)).await;
            set_exchange_policy(&origin.broker.url, |policy| {
                policy.delay = Duration::from_millis(500)
            });
            let failures = concurrent_puts(&session, 16, 3).await;
            assert!(
                failures.is_empty(),
                "every write across the expiry succeeds: {failures:?}"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quic_session_stops_when_its_renewal_is_refused() {
    client_environment();
    let origin = Origin::start_with_quic().await;
    let identity = "quic-refused-renewal";
    log_in(&origin, identity, SESSION_TTL).await;
    let repository: RepositoryId = rand::random();
    let remote = format!("lore://127.0.0.1:{}", origin.port);
    let (execution, _) = client("quic-refused-renewal");

    LORE_CONTEXT
        .scope(execution, async {
            let connection = connect(&remote, identity, repository).await;
            let session = connection
                .session(repository, "quic-refused-renewal")
                .await
                .expect("storage session");
            let (fragment, address, payload) = generate_random();
            session
                .put(address, fragment, Some(payload))
                .await
                .expect("put while the session token is live");

            // The grant is revoked: the exchange refuses every renewal.
            set_exchange_policy(&origin.broker.url, |policy| policy.refuse = true);
            tokio::time::sleep(Duration::from_secs(SESSION_TTL + 1)).await;
            let asked = exchange_policy(&origin.broker.url).requested;
            let (fragment, address, payload) = generate_random();
            let refused = tokio::time::timeout(
                Duration::from_secs(15),
                session.put(address, fragment, Some(payload)),
            )
            .await
            .expect("a refused renewal fails the put rather than retrying");
            assert!(refused.is_err(), "no put without a live authorization");
            assert!(
                exchange_policy(&origin.broker.url).requested <= asked + 1,
                "a refused exchange ends the renewal"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quic_refusal_other_than_expiry_is_not_renewed() {
    client_environment();
    let origin = Origin::start_with_quic().await;
    let identity = "quic-read-only";
    let repository: RepositoryId = rand::random();
    set_exchange_policy(&origin.broker.url, |policy| policy.ttl = 600);
    store_identity(
        &origin.broker.url,
        identity,
        &deploy(identity, &[repository]),
    )
    .await;
    let remote = format!("lore://127.0.0.1:{}", origin.port);
    let (execution, _) = client("quic-read-only");

    LORE_CONTEXT
        .scope(execution, async {
            let connection = connect(&remote, identity, repository).await;
            let session = connection
                .session(repository, "quic-read-only")
                .await
                .expect("read-only storage session");
            let asked = exchange_policy(&origin.broker.url).requested;
            let (fragment, address, payload) = generate_random();
            let refused = session.put(address, fragment, Some(payload)).await;
            assert!(refused.is_err(), "a read-only session cannot put");
            assert_eq!(
                exchange_policy(&origin.broker.url).requested,
                asked,
                "only an expired session is renewed"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quic_copy_renews_its_expired_source_session() {
    client_environment();
    let origin = Origin::start_with_quic().await;
    let identity = "copy-source-renewal";
    log_in(&origin, identity, SESSION_TTL).await;
    let source: RepositoryId = rand::random();
    let destination: RepositoryId = rand::random();
    let remote = format!("lore://127.0.0.1:{}", origin.port);
    let (execution, _) = client("copy-source-renewal");

    LORE_CONTEXT
        .scope(execution, async {
            let store = Arc::new(RemoteImmutableStore::new(&remote, Some(identity)));
            let (fragment, address, payload) = generate_random();
            store
                .clone()
                .put(source, address, fragment, Some(payload), false)
                .await
                .expect("put into the source repository");

            // The session that put the content is still held, but its token expires.
            tokio::time::sleep(Duration::from_secs(SESSION_TTL + 1)).await;
            let context: Context = rand::random();
            store
                .clone()
                .copy(source, address, destination, context, true)
                .await
                .expect("a copy renews the expired session that authorizes its source");
            let copied = Address {
                hash: address.hash,
                context,
            };
            assert_eq!(
                store
                    .clone()
                    .exist(destination, copied, StoreMatch::MatchFull)
                    .await
                    .expect("query the destination"),
                StoreMatch::MatchFull
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_remote_copies_release_their_source_sessions() {
    client_environment();
    let origin = Origin::start_with_quic().await;
    let identity = "repeated-copies";
    log_in(&origin, identity, 600).await;
    let source: RepositoryId = rand::random();
    let destination: RepositoryId = rand::random();
    let remote = format!("lore://127.0.0.1:{}", origin.port);
    let store = Arc::new(RemoteImmutableStore::new(&remote, Some(identity)));
    let (fragment, address, payload) = generate_random();

    let (execution, _) = client("repeated-copies-put");
    let connection = LORE_CONTEXT
        .scope(execution, async {
            store
                .clone()
                .put(source, address, fragment, Some(payload), false)
                .await
                .expect("put into the source repository");
            let connection = connect(&remote, identity, destination).await;
            // As a command's cleanup does, release the session the put used.
            connection.release_session(source, "repeated-copies-put");
            connection
        })
        .await;
    let before = connection.pinned_session_count();

    for copy in 0..8 {
        // Each command has its own correlation ID.
        let correlation_id = format!("repeated-copies-{copy}");
        let (execution, _) = client(&correlation_id);
        LORE_CONTEXT
            .scope(execution, async {
                let context: Context = rand::random();
                store
                    .clone()
                    .copy(source, address, destination, context, true)
                    .await
                    .expect("copy between two granted repositories");
            })
            .await;
        connection.release_session(destination, &correlation_id);
    }
    assert_eq!(
        connection.pinned_session_count(),
        before,
        "a copy must not leave its source session pinned after it completes"
    );
}
