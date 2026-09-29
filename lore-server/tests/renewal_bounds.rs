// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Renewal against a token exchange that stalls: the client gives up in bounded time rather than
//! holding up the operation that waits on it, and cancelling a subscription interrupts a renewal
//! in progress.

mod support;

use std::time::Duration;
use std::time::Instant;

use lore_base::runtime::LORE_CONTEXT;
use lore_revision::fragment::generate_random;
use lore_revision::lore::RepositoryId;
use support::Origin;
use support::announced_in_time;
use support::client;
use support::client_environment;
use support::connect;
use support::eventually;
use support::exchange_policy;
use support::log_in;
use support::repository_with_remote;
use support::set_exchange_policy;

/// Lifetime of the session tokens the exchange issues.
const SESSION_TTL: u64 = 3;

/// The client bounds a renewal at 20 seconds; this allows for the exchanges around it.
const RENEWAL_GIVES_UP_WITHIN: Duration = Duration::from_secs(40);

/// How long a stalled exchange takes to answer: longer than any bound the client applies.
const STALL: Duration = Duration::from_secs(300);

/// With the exchange stalled once a session token has expired, `writers` concurrent puts on one
/// session each fail within the renewal bound: the ones queued behind the stalled renewal fail
/// with it rather than each waiting out a renewal of its own.
async fn a_stalled_renewal_fails_in_time(
    remote_scheme: &str,
    quic: bool,
    identity: &str,
    writers: usize,
) {
    client_environment();
    let origin = if quic {
        Origin::start_with_quic().await
    } else {
        Origin::start().await
    };
    log_in(&origin, identity, SESSION_TTL).await;
    let repository: RepositoryId = rand::random();
    let remote = format!("{remote_scheme}://127.0.0.1:{}", origin.port);
    let (execution, _) = client(identity);

    LORE_CONTEXT
        .scope(execution, async {
            let connection = connect(&remote, identity, repository).await;
            let session = connection
                .session(repository, identity)
                .await
                .expect("storage session");
            let (fragment, address, payload) = generate_random();
            session
                .put(address, fragment, Some(payload))
                .await
                .expect("put while the session token is live");

            set_exchange_policy(&origin.broker.url, |policy| policy.delay = STALL);
            tokio::time::sleep(Duration::from_secs(SESSION_TTL + 1)).await;
            let puts = (0..writers).map(|_| async {
                let (fragment, address, payload) = generate_random();
                session.put(address, fragment, Some(payload)).await
            });
            let outcomes = tokio::time::timeout(
                RENEWAL_GIVES_UP_WITHIN,
                futures::future::join_all(puts),
            )
            .await
            .expect("a stalled exchange must fail the renewal, and every put waiting on it, in bounded time");
            assert!(
                outcomes.iter().all(Result::is_err),
                "no put without a renewed authorization"
            );
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_grpc_stream_renewal_against_a_stalled_exchange_fails_in_time() {
    a_stalled_renewal_fails_in_time("grpc", false, "grpc-stalled-renewal", 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quic_session_renewal_against_a_stalled_exchange_fails_in_time() {
    a_stalled_renewal_fails_in_time("lore", true, "quic-stalled-renewal", 1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_requests_queued_behind_a_stalled_renewal_fail_with_it() {
    a_stalled_renewal_fails_in_time("grpc", false, "grpc-queued-stalled-renewal", 4).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quic_commands_queued_behind_a_stalled_renewal_fail_with_it() {
    a_stalled_renewal_fails_in_time("lore", true, "quic-queued-stalled-renewal", 4).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsubscribing_interrupts_a_stalled_renewal() {
    client_environment();
    let origin = Origin::start().await;
    let identity = "notification-stalled-renewal";
    log_in(&origin, identity, SESSION_TTL).await;
    let repository: RepositoryId = rand::random();
    let remote = format!("grpc://127.0.0.1:{}", origin.port);
    let (execution, announced) = client(identity);

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

            set_exchange_policy(&origin.broker.url, |policy| policy.delay = STALL);
            let asked = exchange_policy(&origin.broker.url).requested;
            assert!(
                eventually(Duration::from_secs(SESSION_TTL + 10), || {
                    exchange_policy(&origin.broker.url).requested > asked
                })
                .await,
                "the subscription renews once its token expires"
            );

            let started = Instant::now();
            tokio::time::timeout(
                Duration::from_secs(60),
                lore_revision::notification::unsubscribe(context),
            )
            .await
            .expect("unsubscribing returns")
            .expect("unsubscribe");
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "unsubscribing must not wait for the stalled renewal (took {:?})",
                started.elapsed()
            );
        })
        .await;
}
