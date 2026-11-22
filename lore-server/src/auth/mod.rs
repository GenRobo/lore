// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
pub mod jwk;
pub mod jwt;
pub mod jwt_axum_middleware;
pub mod jwt_interceptor;

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use lore_base::types::KeyType;

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

/// Whether a mutable key of this type is changed only through a checked RPC: branch heads
/// by `BranchPush` (protection, locks, parent check), branch records by `BranchCreate`,
/// `BranchDelete`, `BranchProtect`/`BranchUnprotect` and `BranchMetadataSet`, repository
/// records by `RepositoryCreate` and `RepositoryMetadataSet`.
/// Under auth, the raw `MutableStore` / `MutableCompareAndSwap` RPCs refuse these types, so a
/// writer cannot move a head past those checks. The stock client never writes them through
/// the raw RPCs: it keeps its own copies in its local store.
pub fn is_checked_key_type(key_type: KeyType) -> bool {
    match key_type {
        KeyType::BranchMetadata
        | KeyType::BranchId
        | KeyType::BranchLatestPointer
        | KeyType::RepositoryMetadata
        | KeyType::RepositoryId => true,
        KeyType::Untyped | KeyType::Instance => false,
    }
}

/// Whether the server's mutable store keeps key types apart, so a key written under one type
/// can never be read back as another. Set once at server build; unset (the default) fails
/// closed. Only the local store is known to: `LocalMutableStore` stamps the key type into the
/// stored key (`Key::make_typed`) and lists by it, so relabelling a branch head `Untyped` or
/// `Instance` writes a different key. A remote store forwards to another server and a plugin
/// store's namespacing is unknown, so neither is trusted.
static MUTABLE_KEY_TYPES_ISOLATED: AtomicBool = AtomicBool::new(false);

/// Record whether the configured mutable store keeps key types apart.
pub fn set_mutable_key_types_isolated(isolated: bool) {
    MUTABLE_KEY_TYPES_ISOLATED.store(isolated, Ordering::Relaxed);
}

/// Under auth, whether a raw `MutableStore` / `MutableCompareAndSwap` may write a key of this
/// type: never one a checked RPC owns, and nothing at all on a store that cannot keep key
/// types apart (a relabelled key could alias a protected one there).
pub fn raw_mutable_write_allowed(key_type: KeyType) -> bool {
    MUTABLE_KEY_TYPES_ISOLATED.load(Ordering::Relaxed) && !is_checked_key_type(key_type)
}

/// The access a request needs on a repository.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    Read,
    Write,
}
