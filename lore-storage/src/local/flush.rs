// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Flush bookkeeping shared by `LocalImmutableStore` and `LocalMutableStore`.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use tokio::sync::Mutex;
use tokio::sync::MutexGuard;

use crate::local::fan_out::FAN_OUT_LEVEL_MAX;

/// How far the write of one bucket index file goes towards durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BucketSync {
    /// Replace the file, fsync nothing.
    None,
    /// Fsync the file before it replaces the old one. The caller fsyncs the directory once for
    /// all the buckets it writes there.
    File,
    /// Fsync the file before it replaces the old one and the directory afterwards.
    FileAndDirectory,
}

impl BucketSync {
    /// The mode of a write on its own, outside a flush pass over the whole group.
    pub fn standalone(sync_data: bool) -> Self {
        if sync_data {
            BucketSync::FileAndDirectory
        } else {
            BucketSync::None
        }
    }

    /// The mode of a write that is part of a flush pass over the whole group.
    pub fn in_pass(sync_data: bool) -> Self {
        if sync_data {
            BucketSync::File
        } else {
            BucketSync::None
        }
    }
}

/// What a group's index files still owe the storage media, kept next to the group's `dirty`
/// flags (memory differs from the file) so a flush finds its work with lock-free loads.
///
/// `dirty` alone cannot answer "is everything durable": a write that does not fsync clears it.
pub struct GroupDurability {
    /// Per bucket: the index file was replaced without an fsync since it was last durable, or
    /// a write of it is in progress.
    unsynced: [AtomicBool; FAN_OUT_LEVEL_MAX],
    /// A file was published in, or removed from, the group's directory and the directory has
    /// not been fsynced since.
    directory_unsynced: AtomicBool,
    /// The group's directory was created and the directories above it have not been fsynced
    /// since.
    directory_created: AtomicBool,
}

impl Default for GroupDurability {
    fn default() -> Self {
        Self {
            unsynced: std::array::from_fn(|_| AtomicBool::new(false)),
            directory_unsynced: AtomicBool::new(false),
            directory_created: AtomicBool::new(false),
        }
    }
}

impl GroupDurability {
    /// Whether a write of the bucket in mode `sync` has anything to do.
    pub fn needs_serialize(
        &self,
        dirty: &[AtomicBool],
        bucket_index: usize,
        sync: BucketSync,
    ) -> bool {
        dirty[bucket_index].load(Ordering::Acquire)
            || (sync != BucketSync::None && self.unsynced[bucket_index].load(Ordering::Acquire))
    }

    /// Whether a flush of the group in mode `sync` has anything to do in its index files.
    pub fn needs_flush(&self, dirty: &[AtomicBool], sync: BucketSync) -> bool {
        dirty.iter().any(|flag| flag.load(Ordering::Acquire))
            || (sync != BucketSync::None
                && (self.directory_unsynced.load(Ordering::Acquire)
                    || self
                        .unsynced
                        .iter()
                        .any(|flag| flag.load(Ordering::Acquire))))
    }

    /// Take over the bucket's pending state before writing its file. Call with the bucket's
    /// serialize lock held and its entries locked against writers; a failed write has to set
    /// `dirty` again.
    ///
    /// `unsynced` is set before `dirty` is cleared: from the moment a bucket is modified until
    /// its file is durable, a scan of the two flags finds at least one of them set, so a
    /// durable flush never skips a bucket whose write is still on its way.
    pub fn claim(&self, dirty: &[AtomicBool], bucket_index: usize) {
        self.unsynced[bucket_index].store(true, Ordering::Release);
        dirty[bucket_index].store(false, Ordering::Release);
    }

    /// Record that the file of a claimed bucket was written in mode `sync`. Call after
    /// [`Self::directory_changed`], so the scan described at [`Self::claim`] holds for the
    /// directory too.
    pub fn written(&self, bucket_index: usize, sync: BucketSync) {
        if sync != BucketSync::None {
            self.unsynced[bucket_index].store(false, Ordering::Release);
        }
    }

    /// Record that the group's directory was created.
    pub fn directory_created(&self) {
        self.directory_created.store(true, Ordering::Release);
        self.directory_unsynced.store(true, Ordering::Release);
    }

    /// Record a file published in, or removed from, the group's directory.
    pub fn directory_changed(&self) {
        self.directory_unsynced.store(true, Ordering::Release);
    }

    /// Fsync the group's directory if it changed since it was last synced, and the two
    /// directories above it (`index` and the store's own) if the group's directory is new.
    pub fn sync_directory(&self, group_path: &std::path::Path) -> std::io::Result<()> {
        if !self.directory_unsynced.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        let created = self.directory_created.swap(false, Ordering::AcqRel);
        group_path
            .ancestors()
            .take(if created { 3 } else { 1 })
            .filter(|directory| !directory.as_os_str().is_empty())
            .try_for_each(crate::fs_util::sync_dir)
            .inspect_err(|_| {
                if created {
                    self.directory_created.store(true, Ordering::Release);
                }
                self.directory_unsynced.store(true, Ordering::Release);
            })
    }
}

/// Lets one flush pass at a time walk the groups of a store, and lets the durable flushes that
/// arrive while a pass runs share the next one instead of each running its own.
///
/// A pass rewrites level markers and renames bucket files group by group; two of them
/// interleaving in the same group would publish a mix of their snapshots.
#[derive(Default)]
pub struct FlushGate {
    /// Held for the duration of a pass.
    pass: Mutex<()>,
    /// Number of durable passes started so far. Only changed with `pass` held.
    started: AtomicU64,
    /// Number of the last durable pass that completed without an error. Only changed with
    /// `pass` held.
    completed: AtomicU64,
}

impl FlushGate {
    /// The durable pass a flush requested now has to see completed: the first one that starts
    /// from here on. A pass that is already running may have scanned past the state the caller
    /// wants on storage, so it does not count.
    pub fn ticket(&self) -> u64 {
        self.started.load(Ordering::SeqCst) + 1
    }

    /// Make the store durable for the holder of `ticket`: wait for the running pass, then run
    /// `pass` unless a pass that started after the ticket was taken has already succeeded.
    ///
    /// An error is only reported to the caller whose pass failed; the callers queued behind it
    /// find no successful pass and run their own.
    pub async fn durable<F, Fut, E>(&self, ticket: u64, pass: F) -> Result<(), E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let _pass = self.pass.lock().await;
        if self.completed.load(Ordering::SeqCst) >= ticket {
            return Ok(());
        }
        let number = self.started.fetch_add(1, Ordering::SeqCst) + 1;
        pass().await?;
        self.completed.store(number, Ordering::SeqCst);
        Ok(())
    }

    /// Wait for the running pass and keep the next one out until the guard is dropped. For
    /// passes that make nothing durable and so have nothing to share.
    pub async fn exclusive(&self) -> MutexGuard<'_, ()> {
        self.pass.lock().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use super::*;

    #[tokio::test]
    async fn durable_flushes_waiting_behind_a_pass_share_the_next_one() {
        let gate = Arc::new(FlushGate::default());
        let passes = Arc::new(AtomicUsize::new(0));

        // Hold the gate like a running pass does, then queue several durable flushes behind it
        let running = gate.exclusive().await;
        let mut waiting = Vec::new();
        for _ in 0..8 {
            let gate = gate.clone();
            let passes = passes.clone();
            let ticket = gate.ticket();
            waiting.push(tokio::spawn(async move {
                gate.durable(ticket, || async {
                    passes.fetch_add(1, Ordering::SeqCst);
                    Ok::<(), ()>(())
                })
                .await
            }));
        }
        tokio::task::yield_now().await;
        assert_eq!(passes.load(Ordering::SeqCst), 0);

        drop(running);
        for flush in waiting {
            flush.await.unwrap().unwrap();
        }
        assert_eq!(passes.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_pass_that_started_before_the_request_does_not_satisfy_it() {
        let gate = FlushGate::default();
        let passes = AtomicUsize::new(0);
        let pass = || async {
            passes.fetch_add(1, Ordering::SeqCst);
            Ok::<(), ()>(())
        };

        gate.durable(gate.ticket(), pass).await.unwrap();
        // A later request has written since: the completed pass cannot have covered it
        gate.durable(gate.ticket(), pass).await.unwrap();
        assert_eq!(passes.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_failed_pass_is_not_shared() {
        let gate = FlushGate::default();
        let ticket = gate.ticket();

        assert_eq!(
            gate.durable(ticket, || async { Err("disk") }).await,
            Err("disk")
        );

        let passes = AtomicUsize::new(0);
        gate.durable(ticket, || async {
            passes.fetch_add(1, Ordering::SeqCst);
            Ok::<(), &str>(())
        })
        .await
        .unwrap();
        assert_eq!(passes.load(Ordering::SeqCst), 1);
    }
}
