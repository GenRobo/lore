// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! The Lore client against an origin in tenant mode, with authorization on end to end: long-lived
//! streams outlive the session tokens that opened them, and a copy reads its source through a
//! session the client holds on the copying connection.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Context;
use lore_revision::event::LoreEvent;
use lore_revision::filter::Filter;
use lore_revision::fragment::generate_random;
use lore_revision::instance::InstanceId;
use lore_revision::interface::ExecutionContext;
use lore_revision::interface::LoreGlobalArgs;
use lore_revision::lore::BranchId;
use lore_revision::lore::RepositoryId;
use lore_revision::notification::NotificationSender as _;
use lore_revision::relay::EventDispatcher;
use lore_revision::repository::RepositoryContext;
use lore_revision::repository::RepositoryFormat;
use lore_revision::store::remote::RemoteImmutableStore;
use lore_storage::ImmutableStore as _;
use lore_storage::StoreMatch;
use lore_transport::Connection;
use support::Origin;
use support::client_environment;
use support::developer;
use support::exchange_policy;
use support::set_exchange_policy;
use support::store_identity;

/// Lifetime of the session tokens the exchange issues in the renewal tests.
const SESSION_TTL: u64 = 3;

/// A Lore client's execution context, recording the branches its notifications announce.
fn client(correlation_id: &str) -> (Arc<ExecutionContext>, Arc<Mutex<Vec<BranchId>>>) {
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
async fn log_in(origin: &Origin, identity: &str, ttl: u64) {
    set_exchange_policy(&origin.broker.url, |policy| policy.ttl = ttl);
    store_identity(&origin.broker.url, identity, &developer(identity)).await;
}

async fn connect(remote: &str, identity: &str, repository: RepositoryId) -> Arc<Connection> {
    lore_transport::connect(remote, identity, repository, 1)
        .await
        .expect("connect to the origin")
}

/// Publish a new branch of `repository` until the client announces it, or give up after `within`.
async fn announced_in_time(
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

async fn repository_with_remote(
    connection: Arc<Connection>,
    id: RepositoryId,
) -> Arc<RepositoryContext> {
    let (immutable_store, mutable_store) = support::client_stores().await;
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
