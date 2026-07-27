// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
pub mod jwk;
pub mod jwt;
pub mod jwt_axum_middleware;
pub mod jwt_interceptor;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

/// Whether this process is serving with authorization enabled. Set once at server build
/// from the presence of a JWT verifier. Used as a fail-closed backstop: a mutating handler
/// reached without an authorization token denies the write when auth is enabled (rather than
/// trusting the interceptor to have run), and only passes through when auth is genuinely
/// disabled server-wide.
static AUTH_ENABLED: AtomicBool = AtomicBool::new(false);

/// Record whether authorization is enabled for this server process.
pub fn set_auth_enabled(enabled: bool) {
    AUTH_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Whether authorization is enabled for this server process.
pub fn auth_enabled() -> bool {
    AUTH_ENABLED.load(Ordering::Relaxed)
}
