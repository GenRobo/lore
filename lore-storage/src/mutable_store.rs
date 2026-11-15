// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;

use crate::Hash;
use crate::Partition;
use crate::immutable_store::StoreError;
use crate::store_types::KeyType;
use crate::store_types::KeyValueStream;

#[async_trait]
pub trait MutableStore: Send + Sync {
    /// Returns the mutable value stored for `key` with the type `key_type` within `partition`.
    ///
    /// Returns `StoreError::AddressNotFound` if no value of the given type is stored for the given key.
    /// Other errors indicate store inconsistency or inability to perform the load.
    async fn load(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError>;

    /// Store a new mutable value for `key` of type `key_type` within `partition`.
    ///
    /// Storing a null hash (`Hash::default()`) removes the key.
    /// Errors indicate store inconsistency or inability to perform the store.
    async fn store(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<(), StoreError>;

    /// Compare and swap a mutable value for `key` with the type `key_type` within `partition`.
    ///
    /// Updates the value for the given key if the current value matches `expected`
    /// or the key does not exist. Returns the previous value of the key (equal to
    /// `expected` if the swap succeeded).
    /// Errors indicate store inconsistency or inability to perform the operation.
    async fn compare_and_swap(
        self: Arc<Self>,
        partition: Partition,
        key: Hash,
        expected: Hash,
        value: Hash,
        key_type: KeyType,
    ) -> Result<Hash, StoreError>;

    /// List all the key-value pairs of a given type for the given partition.
    /// If the partition is null, it must match all partitions the user have access to
    async fn list(
        self: Arc<Self>,
        partition: Partition,
        key_type: KeyType,
    ) -> Result<KeyValueStream, StoreError>;

    /// Flush any pending writes to durable storage.
    ///
    /// When `sync_data` is true, everything stored before the call has reached the storage
    /// media (fsync) when this returns `Ok`; an `Err` means it may not have. When false, the
    /// writes are handed to the operating system and survive the process, not the machine.
    async fn flush(self: Arc<Self>, sync_data: bool) -> Result<(), StoreError>;

    /// Stop taking changes, for good: once this returns, `store` and `compare_and_swap` fail,
    /// and a change that was in progress when it was called has completed.
    ///
    /// A process that is shutting down seals the store before the last flush of the stores
    /// its values point into, so that no value can come to point at something flushed too
    /// late. The default is for stores that have nothing of their own to flush.
    async fn seal(self: Arc<Self>) -> Result<(), StoreError> {
        Ok(())
    }
}

impl Debug for dyn MutableStore {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "MutableStore")
    }
}
