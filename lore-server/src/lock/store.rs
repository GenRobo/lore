// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::borrow::Cow;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use async_trait::async_trait;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use lore_base::error::InvalidArguments;
use lore_base::error::LockNotFound;
use lore_base::error::LockNotOwned;
use lore_base::types::Hash;
use lore_base::types::LockData;
use lore_base::types::LockResource;
use lore_revision::lock::LockError;
use lore_revision::lock::LockQuery;
use lore_revision::lock::LockStore;
use lore_revision::lore::BranchId;
use lore_revision::lore::RepositoryId;
use lore_revision::util;
use serde::Deserialize;
use serde::Serialize;

#[derive(Eq, Hash, PartialEq)]
pub struct LockKey {
    repository: RepositoryId,
    branch: BranchId,
    hash: Hash,
}

/// Locks held in memory and, when opened on a file, saved to it on every change.
#[derive(Default)]
pub struct LocalLockStore {
    storage: DashMap<LockKey, LockData>,
    file: Option<LockFile>,
}

struct LockFile {
    path: PathBuf,
    /// Held from the first edit a change makes to `storage` until the file holds its result:
    /// the file is written in the order of the changes.
    changes: tokio::sync::Mutex<()>,
}

/// A lock as the lock file holds it, one JSON object per line.
#[derive(Serialize, Deserialize)]
struct SavedLock<'a> {
    repository: RepositoryId,
    branch: BranchId,
    hash: Hash,
    #[serde(borrow)]
    description: Cow<'a, str>,
    #[serde(borrow)]
    owner: Cow<'a, str>,
    locked_at: u64,
}

impl LocalLockStore {
    /// A store whose locks outlive the process: they are loaded from the file at `path` now
    /// (no file is no locks) and the file is rewritten on every change. A change that cannot
    /// be saved fails and leaves the locks as they were.
    pub fn open(path: PathBuf) -> std::io::Result<Self> {
        if let Some(directory) = path.parent() {
            std::fs::create_dir_all(directory)?;
        }
        let storage = DashMap::new();
        match std::fs::read(&path) {
            Ok(contents) => {
                for line in contents.split(|byte| *byte == b'\n') {
                    if line.is_empty() {
                        continue;
                    }
                    let saved: SavedLock<'_> = serde_json::from_slice(line)?;
                    storage.insert(
                        LockKey {
                            repository: saved.repository,
                            branch: saved.branch,
                            hash: saved.hash,
                        },
                        LockData {
                            resource: LockResource {
                                branch: saved.branch,
                                hash: saved.hash,
                                description: saved.description.into_owned(),
                            },
                            owner: saved.owner.into_owned(),
                            locked_at: saved.locked_at,
                        },
                    );
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }

        Ok(Self {
            storage,
            file: Some(LockFile {
                path,
                changes: tokio::sync::Mutex::new(()),
            }),
        })
    }

    /// Serialize the changes of a store that saves them; a store in memory has nothing to
    /// keep in order.
    async fn changes(&self) -> Option<tokio::sync::MutexGuard<'_, ()>> {
        match &self.file {
            Some(file) => Some(file.changes.lock().await),
            None => None,
        }
    }

    /// Write every lock to the lock file, if there is one. The caller holds `changes`.
    async fn save(&self) -> Result<(), LockError> {
        let Some(file) = &self.file else {
            return Ok(());
        };

        let mut contents = Vec::new();
        for lock in self.storage.iter() {
            let saved = SavedLock {
                repository: lock.key().repository,
                branch: lock.key().branch,
                hash: lock.key().hash,
                description: Cow::Borrowed(&lock.resource.description),
                owner: Cow::Borrowed(&lock.owner),
                locked_at: lock.locked_at,
            };
            serde_json::to_writer(&mut contents, &saved)
                .map_err(|error| LockError::internal_with_context(error, "encoding locks"))?;
            contents.push(b'\n');
        }

        let path = file.path.clone();
        lore_base::lore_spawn_blocking!(move || write_durably(&path, &contents))
            .await
            .map_err(|error| LockError::internal_with_context(error, "saving locks"))?
            .map_err(|error| LockError::internal_with_context(error, "saving locks"))
    }

    fn key(repository: RepositoryId, resource: &LockResource) -> LockKey {
        LockKey {
            repository,
            branch: resource.branch,
            hash: resource.hash,
        }
    }
}

/// Replace the file at `path` with `contents` so that a crash leaves either the old file or
/// the new one, and the new one is on disk when this returns.
fn write_durably(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = std::fs::File::create(&temporary)?;
    file.write_all(contents)?;
    file.sync_all()?;
    std::fs::rename(&temporary, path)?;
    #[cfg(unix)]
    if let Some(directory) = path.parent() {
        std::fs::File::open(directory)?.sync_all()?;
    }
    Ok(())
}

#[async_trait]
impl LockStore for LocalLockStore {
    async fn lock_resources(
        &self,
        owner_id: &str,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockData>, LockError> {
        let _changes = self.changes().await;
        let mut locks = Vec::<LockData>::with_capacity(resources.len());
        let mut failed = false;
        let timestamp = util::time::timestamp();
        for resource in resources {
            let key = Self::key(repository, resource);

            let lock = LockData {
                resource: resource.clone(),
                owner: owner_id.to_string(),
                locked_at: timestamp,
            };
            // `DashMap::entry` is safe here as it is not held across any awaits and no other locks are acquired while held
            #[allow(clippy::disallowed_methods)]
            match self.storage.entry(key) {
                Entry::Vacant(entry) => entry.insert(lock.clone()),
                Entry::Occupied(entry) => {
                    if entry.get().owner == lock.owner {
                        continue;
                    }
                    failed = true;
                    break;
                }
            };

            locks.push(lock);
        }

        if failed {
            for lock in &locks {
                self.storage.remove(&Self::key(repository, &lock.resource));
            }
            return Err(LockError::internal("resource already locked"));
        }

        if !locks.is_empty()
            && let Err(error) = self.save().await
        {
            for lock in &locks {
                self.storage.remove(&Self::key(repository, &lock.resource));
            }
            return Err(error);
        }

        Ok(locks)
    }

    async fn query_locks(&self, query: LockQuery) -> Result<Vec<LockData>, LockError> {
        let mut locks = Vec::new();

        match query {
            LockQuery::Repository(repository) => {
                for lock in self.storage.iter() {
                    if lock.key().repository == repository {
                        locks.push(lock.value().clone());
                    }
                }
            }
            LockQuery::RepositoryBranch(repository, branch) => {
                for lock in self.storage.iter() {
                    let key = lock.key();
                    let value = lock.value();
                    if key.repository == repository && key.branch == branch {
                        locks.push(value.clone());
                    }
                }
            }
            LockQuery::RepositoryBranchDescription(repository, branch, description) => {
                for lock in self.storage.iter() {
                    let key = lock.key();
                    let value = lock.value();
                    if key.repository == repository
                        && key.branch == branch
                        && value.resource.description == description
                    {
                        locks.push(value.clone());
                    }
                }
            }
            LockQuery::OwnerRepository(owner, repository) => {
                for lock in self.storage.iter() {
                    let key = lock.key();
                    let value = lock.value();
                    if key.repository == repository && value.owner == owner {
                        locks.push(value.clone());
                    }
                }
            }
            LockQuery::OwnerRepositoryBranch(owner, repository, branch) => {
                for lock in self.storage.iter() {
                    let key = lock.key();
                    let value = lock.value();
                    if key.repository == repository && key.branch == branch && value.owner == owner
                    {
                        locks.push(value.clone());
                    }
                }
            }
            LockQuery::HashRepositoryBranch(resource, repository, branch) => {
                let key = LockKey {
                    hash: resource,
                    repository,
                    branch,
                };

                if let Some(lock) = self.storage.get(&key) {
                    locks.push(lock.value().clone());
                }
            }
            _ => {
                return Err(InvalidArguments {
                    reason: "unsupported lock query".into(),
                }
                .into());
            }
        }

        Ok(locks)
    }

    async fn check_locks_status(
        &self,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockData>, LockError> {
        let mut locked = vec![];

        for resource in resources {
            let key = Self::key(repository, resource);

            if let Some(lock) = self.storage.get(&key) {
                locked.push(lock.value().clone());
            }
        }

        Ok(locked)
    }

    async fn unlock_resources(
        &self,
        owner_id: &str,
        validate_user: bool,
        repository: RepositoryId,
        resources: &[LockResource],
    ) -> Result<Vec<LockResource>, LockError> {
        let _changes = self.changes().await;
        let mut removed = Vec::new();
        let mut result: Result<(), LockError> = Ok(());
        for resource in resources {
            // `DashMap::entry` is safe here as it is not held across any awaits and no other locks are acquired while held
            #[allow(clippy::disallowed_methods)]
            match self.storage.entry(Self::key(repository, resource)) {
                Entry::Vacant(_) => {
                    result = Err(LockNotFound.into());
                    break;
                }
                Entry::Occupied(entry) => {
                    if validate_user && entry.get().owner != *owner_id {
                        result = Err(LockNotOwned.into());
                        break;
                    }
                    removed.push(entry.remove());
                }
            }
        }

        // The locks released before a refused one stay released, as they always did.
        if !removed.is_empty()
            && let Err(error) = self.save().await
        {
            for lock in removed {
                self.storage
                    .insert(Self::key(repository, &lock.resource), lock);
            }
            return Err(error);
        }

        result.map(|()| resources.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use std::slice::from_ref;

    use lore_revision::lock::util::assemble_resource_for_path;
    use rand::random;

    use super::*;

    /// A directory of its own under the system temporary directory.
    fn scratch_directory() -> PathBuf {
        let directory =
            std::env::temp_dir().join(format!("lore-lock-store-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&directory).expect("create scratch directory");
        directory
    }

    async fn held(store: &LocalLockStore, repository: RepositoryId) -> Vec<(String, String)> {
        let mut locks: Vec<(String, String)> = store
            .query_locks(LockQuery::Repository(repository))
            .await
            .expect("query")
            .into_iter()
            .map(|lock| (lock.resource.description, lock.owner))
            .collect();
        locks.sort();
        locks
    }

    #[tokio::test]
    async fn locks_outlive_the_store() {
        let directory = scratch_directory();
        let path = directory.join("locks.json");
        let repository = random::<RepositoryId>();
        let branch = BranchId::from(uuid::Uuid::now_v7());
        let a = assemble_resource_for_path("Content/a \"quoted\".uasset", branch);
        let b = assemble_resource_for_path("b.bin", branch);

        let store = LocalLockStore::open(path.clone()).expect("open without a file");
        let locked = store
            .lock_resources("alice", repository, &[a.clone(), b.clone()])
            .await
            .expect("lock");
        store
            .lock_resources("bob", random::<RepositoryId>(), from_ref(&a))
            .await
            .expect("lock in another repository");
        drop(store);

        let store = LocalLockStore::open(path.clone()).expect("reopen");
        assert_eq!(
            held(&store, repository).await,
            [
                (a.description.clone(), "alice".to_string()),
                (b.description.clone(), "alice".to_string()),
            ]
        );
        let reloaded = store
            .check_locks_status(repository, from_ref(&a))
            .await
            .expect("status");
        assert_eq!(reloaded, [locked[0].clone()]);
        // The reloaded lock still belongs to its owner only.
        let error = store
            .unlock_resources("bob", true, repository, from_ref(&a))
            .await
            .expect_err("not the owner");
        assert!(matches!(error, LockError::LockNotOwned(_)));
        store
            .unlock_resources("alice", true, repository, from_ref(&a))
            .await
            .expect("unlock");
        drop(store);

        let store = LocalLockStore::open(path).expect("reopen after unlock");
        assert_eq!(
            held(&store, repository).await,
            [(b.description.clone(), "alice".to_string())]
        );

        std::fs::remove_dir_all(directory).expect("remove scratch directory");
    }

    #[tokio::test]
    async fn change_that_cannot_be_saved_fails_and_is_undone() {
        let directory = scratch_directory();
        let path = directory.join("locks.json");
        let repository = random::<RepositoryId>();
        let branch = BranchId::from(uuid::Uuid::now_v7());
        let a = assemble_resource_for_path("a.bin", branch);
        let b = assemble_resource_for_path("b.bin", branch);

        let store = LocalLockStore::open(path.clone()).expect("open");
        store
            .lock_resources("alice", repository, from_ref(&a))
            .await
            .expect("lock");

        // A directory where the store writes the next version of its file.
        std::fs::create_dir(path.with_extension("tmp")).expect("block the save");
        store
            .lock_resources("alice", repository, from_ref(&b))
            .await
            .expect_err("lock that cannot be saved");
        store
            .unlock_resources("alice", true, repository, from_ref(&a))
            .await
            .expect_err("unlock that cannot be saved");
        assert_eq!(
            held(&store, repository).await,
            [(a.description.clone(), "alice".to_string())]
        );
        drop(store);

        // The file holds what the store holds.
        let store = LocalLockStore::open(path.clone()).expect("reopen");
        assert_eq!(
            held(&store, repository).await,
            [(a.description.clone(), "alice".to_string())]
        );

        std::fs::remove_dir_all(directory).expect("remove scratch directory");
    }

    #[tokio::test]
    async fn unreadable_lock_file_is_an_error() {
        let directory = scratch_directory();
        let path = directory.join("locks.json");
        std::fs::write(&path, b"{\"repository\":\n").expect("write");
        assert!(LocalLockStore::open(path).is_err());
        std::fs::remove_dir_all(directory).expect("remove scratch directory");
    }

    #[tokio::test]
    async fn store_without_a_file_keeps_locks_in_memory() {
        let repository = random::<RepositoryId>();
        let a = assemble_resource_for_path("a.bin", BranchId::from(uuid::Uuid::now_v7()));
        let store = LocalLockStore::default();
        store
            .lock_resources("alice", repository, from_ref(&a))
            .await
            .expect("lock");
        store
            .lock_resources("bob", repository, from_ref(&a))
            .await
            .expect_err("held by alice");
        assert_eq!(
            held(&store, repository).await,
            [(a.description.clone(), "alice".to_string())]
        );
        store
            .unlock_resources("bob", false, repository, from_ref(&a))
            .await
            .expect("privileged unlock");
        assert!(held(&store, repository).await.is_empty());
    }
}
