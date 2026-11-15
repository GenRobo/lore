// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Enforcement of file locks when a branch head moves.
//!
//! A file lock is keyed by repository, branch and the hash of the repository-relative path
//! (`lore_revision::lock::util::assemble_resource_for_path`). A push is refused when a path
//! that differs between the branch head and the state being published carries such a lock
//! whose owner is not the caller.
use std::fmt::Write;
use std::sync::Arc;

use lore_base::error::LockNotOwned;
use lore_base::types::Hash;
use lore_base::types::LockData;
use lore_revision::change::FileAction;
use lore_revision::change::NodeChange;
use lore_revision::change::NodeChangeState;
use lore_revision::filter::FilterMode;
use lore_revision::lock::LockError;
use lore_revision::lock::LockQuery;
use lore_revision::lock::LockStore;
use lore_revision::lore::BranchId;
use lore_revision::node::NodeID;
use lore_revision::node::NodeIDExt;
use lore_revision::repository::RepositoryContext;
use lore_revision::state;
use lore_revision::state::ChangeSink;
use lore_revision::state::State;
use lore_revision::state::StateError;
use lore_revision::state::StateNodeChildrenWithNameIterator;
use lore_revision::util::path::RelativePath;
use lore_storage::hash::hash_slice;
use lore_telemetry::tracing::fields::BRANCH_ID;
use tokio::sync::mpsc;
use tonic::Status;
use tracing::info;

use crate::grpc::warn_error_to_status;

/// Locked paths named in a refusal. The diff stops once this many are found.
const MAX_REPORTED_LOCKS: usize = 16;

/// Changes buffered between the diff and the lock lookup.
const DIFF_CHANNEL_CAPACITY: usize = 256;

/// What a push needs to honour file locks: the lock store, when the server has one, and the
/// identity the push is made as.
#[derive(Clone, Copy)]
pub struct PushLocks<'a> {
    store: Option<&'a dyn LockStore>,
    caller: &'a str,
}

impl<'a> PushLocks<'a> {
    pub fn new(store: Option<&'a dyn LockStore>, caller: &'a str) -> Self {
        Self { store, caller }
    }

    /// No lock store: nothing is enforced.
    pub fn disabled() -> Self {
        Self {
            store: None,
            caller: "",
        }
    }

    /// Refuse to move `branch` from `head` to `candidate` when a path that differs between the
    /// two is locked on that branch by somebody other than the caller.
    ///
    /// Costs one lock query per call. The states are only diffed when the branch has a lock
    /// the caller does not own. `loaded` is a state the caller holds already: when it is the
    /// head, as the parent of a fast-forward push is, the head is not loaded a second time.
    /// A failed query or diff refuses the push: a push is never accepted because the locks
    /// could not be read.
    ///
    /// The check and the compare-and-swap of the branch head that follows it are not atomic:
    /// a lock acquired after the query (or one the lock store does not return yet, where its
    /// branch query is eventually consistent) does not stop the push in flight. Whoever takes
    /// a lock syncs before editing and sees that push.
    pub async fn ensure_unlocked(
        self,
        repository: &Arc<RepositoryContext>,
        branch: BranchId,
        head: Hash,
        loaded: Option<&Arc<State>>,
        candidate: &Arc<State>,
    ) -> Result<(), Status> {
        let Some(store) = self.store else {
            return Ok(());
        };

        let mut foreign = store
            .query_locks(LockQuery::RepositoryBranch(repository.id, branch))
            .await
            .map_err(lock_query_error)?;
        // Without authentication every lock and every caller is the same `<unknown>` owner.
        foreign.retain(|lock| lock.owner != self.caller);
        if foreign.is_empty() {
            return Ok(());
        }
        foreign.sort_unstable_by_key(|lock| lock.resource.hash);

        let head_state = match loaded.filter(|state| state.revision() == head) {
            Some(state) => state.clone(),
            None => State::deserialize(repository.clone(), head)
                .await
                .map_err(lock_diff_error)?,
        };
        let locked = locked_changes(repository, head_state, candidate.clone(), &foreign).await?;
        if locked.is_empty() {
            return Ok(());
        }

        let mut message = LockNotOwned.to_string();
        for (index, (path, lock)) in locked.iter().enumerate() {
            message.push_str(if index == 0 { ": " } else { ", " });
            let _ = write!(message, "'{path}' (locked by {})", foreign[*lock].owner);
        }
        if locked.len() == MAX_REPORTED_LOCKS {
            message.push_str(", and possibly more");
        }
        info!(
            {BRANCH_ID} = %branch,
            caller = self.caller,
            "Branch push refused: {message}",
        );
        Err(Status::failed_precondition(message))
    }
}

fn lock_query_error(error: LockError) -> Status {
    if matches!(error, LockError::SlowDown(_)) {
        return Status::resource_exhausted(error.to_string());
    }
    warn_error_to_status(&error, |error| {
        Status::internal(format!("Failed to query locks for branch push: {error}"))
    })
}

fn lock_diff_error(error: StateError) -> Status {
    if error.is_slow_down() {
        return Status::resource_exhausted(error.to_string());
    }
    warn_error_to_status(&error, |error| {
        Status::internal(format!(
            "Failed to compare branch head for lock check: {error}"
        ))
    })
}

/// The paths that change from `head` to `candidate` and carry one of the `foreign` locks
/// (sorted by resource hash), each with the index of its lock. At most
/// [`MAX_REPORTED_LOCKS`] are returned.
///
/// The diff is the plain tree diff: no view or ignore filter, since neither may hide a
/// committed change. It streams, so a large push is never held in memory as a change list.
async fn locked_changes(
    repository: &Arc<RepositoryContext>,
    head: Arc<State>,
    candidate: Arc<State>,
    foreign: &[LockData],
) -> Result<Vec<(RelativePath, usize)>, Status> {
    let (tx, mut rx) = mpsc::channel::<Result<NodeChange, StateError>>(DIFF_CHANNEL_CAPACITY);

    let diff = {
        let repository = repository.clone();
        async move {
            let mut sink = ChangeSink::Channel(&tx);
            state::diff(
                repository.clone(),
                head,
                repository,
                candidate,
                None,
                &mut sink,
                FilterMode::empty(),
            )
            .await
        }
    };
    // Owns the receiver: returning early drops it, which ends the diff.
    let lookup = async move {
        let mut locked = LockedChanges {
            foreign,
            found: Vec::new(),
        };
        while let Some(change) = rx.recv().await {
            locked.visit(&change?).await?;
            if locked.is_full() {
                break;
            }
        }
        Ok::<_, StateError>(locked.found)
    };

    let (diffed, locked) = tokio::join!(diff, lookup);
    let locked = locked.map_err(lock_diff_error)?;
    // A full report cut the diff short; anything less needs the diff to have run to its end.
    if locked.len() < MAX_REPORTED_LOCKS {
        diffed.map_err(lock_diff_error)?;
    }
    Ok(locked)
}

struct LockedChanges<'a> {
    foreign: &'a [LockData],
    found: Vec<(RelativePath, usize)>,
}

impl LockedChanges<'_> {
    fn is_full(&self) -> bool {
        self.found.len() >= MAX_REPORTED_LOCKS
    }

    fn check(&mut self, path: &RelativePath) {
        if self.is_full() {
            return;
        }
        let hash = hash_slice(path.as_str().as_bytes());
        if let Ok(lock) = self
            .foreign
            .binary_search_by(|lock| lock.resource.hash.cmp(&hash))
            && !self.found.iter().any(|(_, found)| *found == lock)
        {
            self.found.push((path.clone(), lock));
        }
    }

    async fn visit(&mut self, change: &NodeChange) -> Result<(), StateError> {
        self.check(&change.path);
        if change.action != FileAction::Move {
            // An added or deleted directory comes with a change for every path below it.
            return Ok(());
        }
        let Some(from_path) = change.from_path.as_ref() else {
            return Ok(());
        };
        self.check(from_path);

        // A moved directory is one change, and the diff only goes below it for content that
        // changed as well. Every path below it changes with it, at both ends of the move.
        self.check_below(&change.to, change.to.node, &change.path)
            .await?;
        let from_node = if change.from.node.is_valid_node_id() {
            change.from.node
        } else {
            match change
                .from
                .state
                .find_node_link(change.from.repository.clone(), from_path.as_str())
                .await
            {
                Ok(link) if link.revision == change.from.state.revision() => link.node,
                Ok(_) => return Ok(()),
                Err(error) if error.is_node_not_found() => return Ok(()),
                Err(error) => return Err(error),
            }
        };
        self.check_below(&change.from, from_node, from_path).await
    }

    /// Check every path below the directory `node` of `side`, which is at `path`. A file has
    /// nothing below it; a link is a leaf, as it is for the diff.
    async fn check_below(
        &mut self,
        side: &NodeChangeState,
        node: NodeID,
        path: &RelativePath,
    ) -> Result<(), StateError> {
        if !node.is_valid_node_id()
            || !side
                .state
                .node(side.repository.clone(), node)
                .await?
                .is_directory()
        {
            return Ok(());
        }

        let mut pending = vec![(node, path.clone())];
        while let Some((directory, directory_path)) = pending.pop() {
            if self.is_full() {
                break;
            }
            let mut children = StateNodeChildrenWithNameIterator::new(
                side.state.clone(),
                side.repository.clone(),
                directory,
            )
            .await?;
            while let Some((child, child_node, name)) = children.next().await? {
                let child_path = directory_path.push_into_buf(name).freeze();
                self.check(&child_path);
                if child_node.is_directory() {
                    pending.push((child, child_path));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use lore_storage::hash::hash_string;

    use super::*;
    use crate::grpc::get_write_token;

    /// Serialize a revision whose tree holds `files` (`/`-separated path and content), with
    /// the content stored so that fragment verification accepts a push of it. As in a
    /// committed revision, the address of a directory follows what is below it: the diff does
    /// not go into a directory whose address is unchanged.
    pub(crate) async fn build_tree_revision(
        repository: &Arc<RepositoryContext>,
        parent_self: Hash,
        revision_number: u64,
        files: &[(&str, &[u8])],
    ) -> Hash {
        use std::collections::HashMap;

        use lore_base::types::Address;
        use lore_revision::node::Node;
        use lore_revision::node::NodeFlags;
        use lore_revision::node::ROOT_NODE;

        let state = State::new();
        state.set_parent_self(parent_self);
        state.set_revision_number(revision_number);

        let mut below = HashMap::<&str, Vec<u8>>::new();
        for (path, content) in files {
            for (separator, _) in path.match_indices('/') {
                let below = below.entry(&path[..separator]).or_default();
                below.extend_from_slice(path[separator..].to_lowercase().as_bytes());
                below.extend_from_slice(content);
            }
        }

        let mut directories = HashMap::<&str, NodeID>::new();
        for (path, content) in files {
            let (address, _) = lore_revision::immutable::write(
                repository.clone(),
                lore_storage::Context::default(),
                bytes::Bytes::copy_from_slice(content),
                lore_storage::WriteOptions::default(),
            )
            .await
            .expect("immutable::write");

            let mut parent = ROOT_NODE;
            for (separator, _) in path.match_indices('/') {
                let directory_path = &path[..separator];
                parent = if let Some(directory) = directories.get(directory_path) {
                    *directory
                } else {
                    let name = directory_path.rsplit('/').next().unwrap_or(directory_path);
                    let directory = Node {
                        flags: NodeFlags::NoFlags.bits(),
                        name_hash: hash_string(name),
                        address: Address::zero_context_hash(hash_slice(&below[directory_path])),
                        ..Default::default()
                    };
                    let directory = state
                        .node_add(repository.clone(), parent, directory, name)
                        .await
                        .expect("node_add directory");
                    directories.insert(directory_path, directory);
                    directory
                };
            }

            let name = path.rsplit('/').next().unwrap_or(path);
            let file = Node {
                flags: NodeFlags::File.bits(),
                name_hash: hash_string(name),
                address,
                ..Default::default()
            };
            state
                .node_add(repository.clone(), parent, file, name)
                .await
                .expect("node_add file");
        }

        state
            .serialize(repository.clone(), &get_write_token())
            .await
            .expect("serialize state")
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use lore_base::runtime::LORE_CONTEXT;
    use lore_base::types::LockResource;
    use lore_revision::lock::util::assemble_resource_for_path;
    use lore_revision::lore::RepositoryId;
    use rand::random;

    use super::testing::build_tree_revision;
    use super::*;
    use crate::lock::store::LocalLockStore;
    use crate::store::test_store_create;

    const ALICE: &str = "alice";
    const BOB: &str = "bob";

    struct Fixture {
        repository: Arc<RepositoryContext>,
        branch: BranchId,
        store: LocalLockStore,
    }

    impl Fixture {
        async fn lock(&self, owner: &str, branch: BranchId, path: &str) {
            self.store
                .lock_resources(
                    owner,
                    self.repository.id,
                    &[assemble_resource_for_path(path, branch)],
                )
                .await
                .expect("lock");
        }

        /// Check a move of the branch from `head` to `candidate` made by `caller`.
        async fn check(&self, caller: &str, head: Hash, candidate: Hash) -> Result<(), Status> {
            let candidate = State::deserialize(self.repository.clone(), candidate)
                .await
                .expect("candidate state");
            PushLocks::new(Some(&self.store), caller)
                .ensure_unlocked(&self.repository, self.branch, head, None, &candidate)
                .await
        }
    }

    /// Run `test` against an empty repository and lock store.
    async fn with_fixture<Test, Fut>(test: Test)
    where
        Test: FnOnce(Fixture) -> Fut,
        Fut: Future<Output = ()>,
    {
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        Box::pin(LORE_CONTEXT.scope(execution, async move {
            let repository = Arc::new(RepositoryContext::new_server_context(
                immutable_store,
                mutable_store,
                random::<RepositoryId>(),
            ));
            test(Fixture {
                repository,
                branch: BranchId::from(uuid::Uuid::now_v7()),
                store: LocalLockStore::default(),
            })
            .await;
        }))
        .await;
    }

    fn assert_refused(result: Result<(), Status>, path: &str, owner: &str) {
        let status = result.expect_err("push should be refused");
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        let message = status.message();
        assert!(
            message.starts_with("resource locked by somebody else: "),
            "{message}"
        );
        assert!(
            message.contains(&format!("'{path}' (locked by {owner})")),
            "{message}"
        );
    }

    #[test]
    fn changed_path_hashes_like_the_client_lock_resource() {
        let path = "Content/Maps/Arena.umap";
        assert_eq!(
            hash_slice(
                RelativePath::new_from_clean_parts("Content/Maps", "Arena.umap")
                    .as_str()
                    .as_bytes()
            ),
            assemble_resource_for_path(path, BranchId::default()).hash
        );
    }

    #[tokio::test]
    async fn modified_added_and_deleted_paths_are_refused() {
        with_fixture(|fixture| async move {
            let repository = &fixture.repository;
            let head = build_tree_revision(
                repository,
                Hash::default(),
                1,
                &[("a.bin", b"a1"), ("dir/b.bin", b"b1"), ("keep.bin", b"k")],
            )
            .await;

            let modified = build_tree_revision(
                repository,
                head,
                2,
                &[("a.bin", b"a2"), ("dir/b.bin", b"b1"), ("keep.bin", b"k")],
            )
            .await;
            let modified_below = build_tree_revision(
                repository,
                head,
                2,
                &[("a.bin", b"a1"), ("dir/b.bin", b"b2"), ("keep.bin", b"k")],
            )
            .await;
            let deleted =
                build_tree_revision(repository, head, 2, &[("a.bin", b"a1"), ("keep.bin", b"k")])
                    .await;
            let added = build_tree_revision(
                repository,
                head,
                2,
                &[
                    ("a.bin", b"a1"),
                    ("dir/b.bin", b"b1"),
                    ("keep.bin", b"k"),
                    ("new/c.bin", b"c1"),
                ],
            )
            .await;

            fixture.lock(ALICE, fixture.branch, "a.bin").await;
            fixture.lock(ALICE, fixture.branch, "dir/b.bin").await;
            fixture.lock(ALICE, fixture.branch, "new/c.bin").await;
            fixture.lock(ALICE, fixture.branch, "keep.bin").await;

            assert_refused(fixture.check(BOB, head, modified).await, "a.bin", ALICE);
            assert_refused(
                fixture.check(BOB, head, modified_below).await,
                "dir/b.bin",
                ALICE,
            );
            assert_refused(fixture.check(BOB, head, deleted).await, "dir/b.bin", ALICE);
            assert_refused(fixture.check(BOB, head, added).await, "new/c.bin", ALICE);
            // A branch without a head yet: every path of the first revision is added.
            assert_refused(
                fixture.check(BOB, Hash::default(), head).await,
                "keep.bin",
                ALICE,
            );

            // The owner of the locks is not held back by them.
            for candidate in [modified, modified_below, deleted, added] {
                fixture
                    .check(ALICE, head, candidate)
                    .await
                    .expect("owner pushes through own locks");
            }
        })
        .await;
    }

    #[tokio::test]
    async fn both_ends_of_a_rename_are_refused() {
        with_fixture(|fixture| async move {
            let repository = &fixture.repository;
            let head = build_tree_revision(
                repository,
                Hash::default(),
                1,
                &[("old.bin", b"content"), ("keep.bin", b"k")],
            )
            .await;
            let renamed = build_tree_revision(
                repository,
                head,
                2,
                &[("new.bin", b"content"), ("keep.bin", b"k")],
            )
            .await;

            fixture.lock(ALICE, fixture.branch, "old.bin").await;
            assert_refused(fixture.check(BOB, head, renamed).await, "old.bin", ALICE);
            fixture
                .store
                .unlock_resources(
                    ALICE,
                    true,
                    repository.id,
                    &[assemble_resource_for_path("old.bin", fixture.branch)],
                )
                .await
                .expect("unlock");
            fixture.check(BOB, head, renamed).await.expect("unlocked");

            fixture.lock(ALICE, fixture.branch, "new.bin").await;
            assert_refused(fixture.check(BOB, head, renamed).await, "new.bin", ALICE);

            // A rename that only changes case is one move, from the old spelling to the new.
            let recased = build_tree_revision(
                repository,
                head,
                2,
                &[("old.bin", b"content"), ("KEEP.bin", b"k")],
            )
            .await;
            fixture.check(BOB, head, recased).await.expect("unlocked");
            fixture.lock(ALICE, fixture.branch, "keep.bin").await;
            assert_refused(fixture.check(BOB, head, recased).await, "keep.bin", ALICE);
            assert_refused(fixture.check(BOB, recased, head).await, "keep.bin", ALICE);
        })
        .await;
    }

    #[tokio::test]
    async fn paths_below_a_moved_or_deleted_directory_are_refused() {
        with_fixture(|fixture| async move {
            let repository = &fixture.repository;
            let head = build_tree_revision(
                repository,
                Hash::default(),
                1,
                &[
                    ("Maps/deep/a.umap", b"a"),
                    ("Maps/b.umap", b"b"),
                    ("keep.bin", b"k"),
                ],
            )
            .await;
            let moved = build_tree_revision(
                repository,
                head,
                2,
                &[
                    ("Levels/deep/a.umap", b"a"),
                    ("Levels/b.umap", b"b"),
                    ("keep.bin", b"k"),
                ],
            )
            .await;
            let deleted = build_tree_revision(repository, head, 2, &[("keep.bin", b"k")]).await;
            // A rename that only changes case keeps the directory node paired in the diff:
            // it is reported as one move and nothing below it is.
            let recased = build_tree_revision(
                repository,
                head,
                2,
                &[
                    ("maps/deep/a.umap", b"a"),
                    ("maps/b.umap", b"b"),
                    ("keep.bin", b"k"),
                ],
            )
            .await;

            fixture
                .lock(ALICE, fixture.branch, "Maps/deep/a.umap")
                .await;
            assert_refused(
                fixture.check(BOB, head, moved).await,
                "Maps/deep/a.umap",
                ALICE,
            );
            assert_refused(
                fixture.check(BOB, head, deleted).await,
                "Maps/deep/a.umap",
                ALICE,
            );
            assert_refused(
                fixture.check(BOB, head, recased).await,
                "Maps/deep/a.umap",
                ALICE,
            );

            // The destination of the move is covered as well.
            assert_refused(
                fixture.check(BOB, moved, head).await,
                "Maps/deep/a.umap",
                ALICE,
            );
            assert_refused(
                fixture.check(BOB, recased, head).await,
                "Maps/deep/a.umap",
                ALICE,
            );
        })
        .await;
    }

    #[tokio::test]
    async fn locks_elsewhere_do_not_refuse() {
        with_fixture(|fixture| async move {
            let repository = &fixture.repository;
            let head = build_tree_revision(
                repository,
                Hash::default(),
                1,
                &[("a.bin", b"a1"), ("b.bin", b"b")],
            )
            .await;
            let candidate =
                build_tree_revision(repository, head, 2, &[("a.bin", b"a2"), ("b.bin", b"b")])
                    .await;

            // An unchanged path, the same path on another branch, and in another repository.
            fixture.lock(ALICE, fixture.branch, "b.bin").await;
            fixture
                .lock(ALICE, BranchId::from(uuid::Uuid::now_v7()), "a.bin")
                .await;
            fixture
                .store
                .lock_resources(
                    ALICE,
                    random::<RepositoryId>(),
                    &[assemble_resource_for_path("a.bin", fixture.branch)],
                )
                .await
                .expect("lock in other repository");

            fixture
                .check(BOB, head, candidate)
                .await
                .expect("no lock on the changed path of this branch");
        })
        .await;
    }

    #[tokio::test]
    async fn states_are_not_loaded_without_a_foreign_lock() {
        with_fixture(|fixture| async move {
            let repository = &fixture.repository;
            let candidate =
                build_tree_revision(repository, Hash::default(), 1, &[("a.bin", b"a")]).await;
            // No revision has this hash: the check fails as soon as it loads the head to diff.
            let missing_head = Hash::from([7u8; 32].as_slice());

            fixture
                .check(BOB, missing_head, candidate)
                .await
                .expect("no locks: no diff");

            fixture.lock(BOB, fixture.branch, "a.bin").await;
            fixture
                .check(BOB, missing_head, candidate)
                .await
                .expect("only own locks: no diff");

            let candidate_state = State::deserialize(repository.clone(), candidate)
                .await
                .expect("candidate state");
            PushLocks::disabled()
                .ensure_unlocked(
                    repository,
                    fixture.branch,
                    missing_head,
                    None,
                    &candidate_state,
                )
                .await
                .expect("no lock store: no diff");

            // A foreign lock anywhere on the branch makes the diff necessary, and its failure
            // refuses the push.
            fixture.lock(ALICE, fixture.branch, "unrelated.bin").await;
            let status = fixture
                .check(BOB, missing_head, candidate)
                .await
                .expect_err("diff against a missing head fails");
            assert_eq!(status.code(), tonic::Code::Internal);
        })
        .await;
    }

    #[tokio::test]
    async fn loaded_head_is_not_loaded_again() {
        with_fixture(|fixture| async move {
            let repository = &fixture.repository;
            let head =
                build_tree_revision(repository, Hash::default(), 1, &[("a.bin", b"a")]).await;
            let candidate =
                build_tree_revision(repository, head, 2, &[("a.bin", b"a"), ("b.bin", b"b")]).await;
            let candidate = State::deserialize(repository.clone(), candidate)
                .await
                .expect("candidate state");
            fixture.lock(ALICE, fixture.branch, "in-memory.bin").await;

            // A file the stored head does not have: only a diff against this very object can
            // report it as deleted.
            let loaded = State::deserialize(repository.clone(), head)
                .await
                .expect("head state");
            let in_memory = lore_revision::node::Node {
                flags: lore_revision::node::NodeFlags::File.bits(),
                name_hash: lore_storage::hash::hash_string("in-memory.bin"),
                ..Default::default()
            };
            loaded
                .node_add(
                    repository.clone(),
                    lore_revision::node::ROOT_NODE,
                    in_memory,
                    "in-memory.bin",
                )
                .await
                .expect("node_add");
            assert_eq!(loaded.revision(), head);

            let locks = PushLocks::new(Some(&fixture.store), BOB);
            assert_refused(
                locks
                    .ensure_unlocked(repository, fixture.branch, head, Some(&loaded), &candidate)
                    .await,
                "in-memory.bin",
                ALICE,
            );
            // A loaded state that is not the head does not stand in for it.
            for loaded in [Some(&candidate), None] {
                locks
                    .ensure_unlocked(repository, fixture.branch, head, loaded, &candidate)
                    .await
                    .expect("diffed against the stored head");
            }
        })
        .await;
    }

    #[tokio::test]
    async fn unauthenticated_callers_share_the_lock_owner() {
        with_fixture(|fixture| async move {
            let repository = &fixture.repository;
            let head =
                build_tree_revision(repository, Hash::default(), 1, &[("a.bin", b"a1")]).await;
            let candidate = build_tree_revision(repository, head, 2, &[("a.bin", b"a2")]).await;

            // What `get_user_id` yields for every request of a server without authentication.
            let unknown = crate::util::get_user_id_from_token_ref(None);
            fixture.lock(&unknown, fixture.branch, "a.bin").await;
            fixture
                .check(&unknown, head, candidate)
                .await
                .expect("auth-off pushes are not refused");
        })
        .await;
    }

    #[tokio::test]
    async fn report_names_every_owner_and_is_bounded() {
        with_fixture(|fixture| async move {
            let repository = &fixture.repository;
            let names: Vec<String> = (0..MAX_REPORTED_LOCKS + 4)
                .map(|index| format!("dir/file{index:02}.bin"))
                .collect();
            let files: Vec<(&str, &[u8])> = names
                .iter()
                .map(|name| (name.as_str(), b"content".as_slice()))
                .collect();
            let head = build_tree_revision(repository, Hash::default(), 1, &[]).await;
            let candidate = build_tree_revision(repository, head, 2, &files).await;

            fixture.lock(ALICE, fixture.branch, &names[0]).await;
            fixture.lock("carol", fixture.branch, &names[1]).await;
            let status = fixture
                .check(BOB, head, candidate)
                .await
                .expect_err("two foreign locks");
            assert!(status.message().contains("(locked by alice)"));
            assert!(status.message().contains("(locked by carol)"));
            assert!(!status.message().contains("possibly more"));

            for name in &names[2..] {
                fixture.lock(ALICE, fixture.branch, name).await;
            }
            let status = fixture
                .check(BOB, head, candidate)
                .await
                .expect_err("many foreign locks");
            assert_eq!(
                status.message().matches("(locked by ").count(),
                MAX_REPORTED_LOCKS
            );
            assert!(status.message().ends_with(", and possibly more"));
        })
        .await;
    }

    struct FailingLockStore;

    #[async_trait]
    impl LockStore for FailingLockStore {
        async fn lock_resources(
            &self,
            _owner_id: &str,
            _repository: RepositoryId,
            _resources: &[LockResource],
        ) -> Result<Vec<LockData>, LockError> {
            Err(LockError::internal("unavailable"))
        }

        async fn query_locks(&self, _query: LockQuery) -> Result<Vec<LockData>, LockError> {
            Err(LockError::internal("unavailable"))
        }

        async fn check_locks_status(
            &self,
            _repository: RepositoryId,
            _resources: &[LockResource],
        ) -> Result<Vec<LockData>, LockError> {
            Err(LockError::internal("unavailable"))
        }

        async fn unlock_resources(
            &self,
            _owner_id: &str,
            _validate_user: bool,
            _repository: RepositoryId,
            _resources: &[LockResource],
        ) -> Result<Vec<LockResource>, LockError> {
            Err(LockError::internal("unavailable"))
        }
    }

    #[tokio::test]
    async fn failed_lock_query_refuses_the_push() {
        with_fixture(|fixture| async move {
            let repository = &fixture.repository;
            let head =
                build_tree_revision(repository, Hash::default(), 1, &[("a.bin", b"a1")]).await;
            let candidate = build_tree_revision(repository, head, 2, &[("a.bin", b"a2")]).await;
            let candidate = State::deserialize(repository.clone(), candidate)
                .await
                .expect("candidate state");

            let status = PushLocks::new(Some(&FailingLockStore), BOB)
                .ensure_unlocked(repository, fixture.branch, head, None, &candidate)
                .await
                .expect_err("lock query failure must refuse the push");
            assert_eq!(status.code(), tonic::Code::Internal);
        })
        .await;
    }
}
