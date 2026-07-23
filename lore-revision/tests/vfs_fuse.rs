// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![cfg(all(target_os = "linux", feature = "vfs"))]

//! Integration test for the Linux FUSE backend. Builds a committed repository in an in-memory
//! store, mounts it over FUSE alongside a real backing directory, and reads through the mount to
//! verify lazy projection and pass-through coexistence. Skipped when `/dev/fuse` is unavailable
//! (e.g. locked-down CI containers).

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::io::Write;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use lore_base::error::NoRemote;
    use lore_base::runtime::LORE_CONTEXT;
    use lore_base::runtime::runtime;
    use lore_base::types::Context;
    use lore_revision::branch;
    use lore_revision::commit;
    use lore_revision::commit::CommitOptions;
    use lore_revision::file;
    use lore_revision::interface::LoreArray;
    use lore_revision::interface::LoreString;
    use lore_revision::lore::RepositoryId;
    use lore_revision::node::NodeFlags;
    use lore_revision::repository;
    use lore_revision::repository::RepositoryContext;
    use lore_revision::repository::RepositoryFormat;
    use lore_revision::stage;
    use lore_revision::stage::StageOptions;
    use lore_revision::state::State;
    use lore_revision::vfs::fuse::LoreFuse;
    use lore_transport::ProtocolError;

    include!("helper.rs");

    /// Poll a filesystem operation until it succeeds or the deadline passes, tolerating the brief
    /// window between mounting and the mount becoming visible.
    fn read_dir_ready(path: &Path) -> std::io::Result<Vec<std::fs::DirEntry>> {
        let mut last = None;
        for _ in 0..40 {
            match std::fs::read_dir(path) {
                Ok(entries) => return entries.collect(),
                Err(err) => {
                    last = Some(err);
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
        Err(last.unwrap_or_else(|| std::io::Error::other("read_dir never succeeded")))
    }

    #[test]
    // The test drives the mount with ordinary filesystem syscalls (including remove_file on the
    // mountpoint); those are user-facing FS operations, not repository-internal writes.
    #[allow(clippy::disallowed_methods)]
    fn mount_projects_lore_and_passes_through_backing() {
        if !Path::new("/dev/fuse").exists() {
            eprintln!("skipping FUSE mount test: /dev/fuse is not available");
            return;
        }

        let (immutable_store, mutable_store, execution) =
            runtime().block_on(test_store_create()).expect("Failed to create stores");
        let repository_id = RepositoryId::from(uuid::Uuid::now_v7());

        // Build a committed repository (readme.md + src/test.txt) in the in-memory store.
        let immutable = immutable_store.clone();
        let mutable = mutable_store.clone();
        let (repository, state) = runtime().block_on(LORE_CONTEXT.scope(execution.clone(), async move {
            let tempdir = generate_tempdir();
            let path = tempdir.to_path_buf();
            std::fs::create_dir_all(path.as_path()).expect("Create directory failed");
            let default_branch_id = Context::from(uuid::Uuid::now_v7());
            let write_token = repository::RepositoryWriteToken::acquire(path.as_path()).await;
            let created_repo = repository::create_local(
                path.as_path(),
                &write_token,
                repository_id,
                default_branch_id,
                branch::DEFAULT_DEFAULT_NAME.to_string(),
                repository::RepositoryConfig::default(),
                false,
            )
            .await
            .expect("Failed to initialize repository");

            let repository = Arc::new(
                RepositoryContext::new(
                    Some(path.clone()),
                    immutable,
                    mutable,
                    repository_id,
                    created_repo.instance_id,
                    Err(ProtocolError::from(NoRemote)),
                    Arc::default(),
                    RepositoryFormat::Lore,
                )
                .with_write_token(write_token.share()),
            );

            lore_revision::instance::store_current_anchor_branch(&repository, default_branch_id)
                .await
                .expect("Failed to store anchor branch");

            {
                let mut f = std::fs::File::create(path.join("readme.md")).expect("create readme");
                f.write_all(b"hello world").expect("write readme");
            }
            std::fs::create_dir_all(path.join("src")).expect("create src");
            {
                let mut f = std::fs::File::create(path.join("src/test.txt")).expect("create file");
                f.write_all(b"hello").expect("write file");
            }

            let paths = LoreArray::from_vec(vec![LoreString::from(&path)]);
            file::stage::stage(
                repository.clone(),
                &write_token,
                paths,
                StageOptions {
                    case_change: stage::StageCaseChange::Error,
                    node_flags: NodeFlags::NoFlags,
                    file_id: None,
                    no_children: false,
                    scan: true,
                },
            )
            .await
            .expect("Stage failed");

            Box::pin(commit::commit(
                repository.clone(),
                &write_token,
                CommitOptions::new("Initial commit".to_string()),
            ))
            .await
            .expect("Commit failed");

            let (state, _, _) = State::deserialize_current_and_staged(repository.clone())
                .await
                .expect("Deserialize failed");

            (repository, state)
        }));

        // A real backing directory holding a pass-through file (as a git working tree would).
        let backing = generate_tempdir();
        {
            let mut f =
                std::fs::File::create(backing.path().join("code.py")).expect("create backing file");
            f.write_all(b"print(1)\n").expect("write backing file");
        }

        let mountpoint = generate_tempdir();

        let fuse = LoreFuse::new(
            repository.clone(),
            state.clone(),
            None,
            Some(backing.path().to_path_buf()),
            execution.clone(),
        );
        let session = fuse.spawn(mountpoint.path()).expect("Failed to mount FUSE");

        // The mount presents projected Lore files and the pass-through file in one tree.
        let names: HashSet<String> = read_dir_ready(mountpoint.path())
            .expect("read_dir on mount")
            .into_iter()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(names.contains("readme.md"), "projected file missing: {names:?}");
        assert!(names.contains("src"), "projected dir missing: {names:?}");
        assert!(names.contains("code.py"), "pass-through file missing: {names:?}");

        // Lazy hydration of projected content.
        assert_eq!(
            std::fs::read(mountpoint.path().join("readme.md")).expect("read readme"),
            b"hello world"
        );
        assert_eq!(
            std::fs::read(mountpoint.path().join("src/test.txt")).expect("read nested"),
            b"hello"
        );
        // Pass-through content served from the backing directory.
        assert_eq!(
            std::fs::read(mountpoint.path().join("code.py")).expect("read pass-through"),
            b"print(1)\n"
        );

        // Writeback: edit a projected file in place through the mount.
        {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(mountpoint.path().join("readme.md"))
                .expect("open readme for write");
            file.write_all(b"goodbye").expect("write readme");
        }

        // The new content is served back (copied up into the backing overlay).
        assert_eq!(
            std::fs::read(mountpoint.path().join("readme.md")).expect("re-read readme"),
            b"goodbye"
        );

        // And the edit is reported to Lore's dirty tracking.
        let dirty = runtime().block_on(LORE_CONTEXT.scope(execution.clone(), {
            let repository = repository.clone();
            async move {
                let (current, staged, _) =
                    State::deserialize_current_and_staged(repository.clone())
                        .await
                        .expect("deserialize");
                let state = staged.unwrap_or_else(|| current.clone());
                let link = state
                    .find_node_link(repository.clone(), "readme.md")
                    .await
                    .expect("find readme node");
                let node = state
                    .node(repository.clone(), link.node)
                    .await
                    .expect("load readme node");
                node.is_dirty_modify()
            }
        }));
        assert!(dirty, "readme.md should be dirty-modify after an in-place edit");

        // Create a new file through the mount.
        {
            let mut file = std::fs::File::create(mountpoint.path().join("notes.txt"))
                .expect("create notes.txt");
            file.write_all(b"notes").expect("write notes.txt");
        }
        assert_eq!(
            std::fs::read(mountpoint.path().join("notes.txt")).expect("read notes.txt"),
            b"notes"
        );

        // Delete a projected file through the mount; it disappears (whiteout).
        std::fs::remove_file(mountpoint.path().join("src").join("test.txt"))
            .expect("unlink src/test.txt");
        assert!(
            std::fs::metadata(mountpoint.path().join("src").join("test.txt")).is_err(),
            "deleted projected file should be gone from the mount"
        );

        // Dirty tracking reflects the add and the delete.
        let (added, deleted) = runtime().block_on(LORE_CONTEXT.scope(execution.clone(), {
            let repository = repository.clone();
            async move {
                let (current, staged, _) =
                    State::deserialize_current_and_staged(repository.clone())
                        .await
                        .expect("deserialize");
                let state = staged.unwrap_or_else(|| current.clone());

                let added = match state.find_node_link(repository.clone(), "notes.txt").await {
                    Ok(link) if link.is_valid() => state
                        .node(repository.clone(), link.node)
                        .await
                        .expect("notes node")
                        .is_dirty_add(),
                    _ => false,
                };
                let deleted = match state.find_node_link(repository.clone(), "src/test.txt").await {
                    Ok(link) if link.is_valid() => state
                        .node(repository.clone(), link.node)
                        .await
                        .expect("test node")
                        .is_dirty_delete(),
                    _ => false,
                };
                (added, deleted)
            }
        }));
        assert!(added, "notes.txt should be dirty-add after creation");
        assert!(deleted, "src/test.txt should be dirty-delete after removal");

        // Unmount, then remount over the same backing directory.
        drop(session);
        let remount_point = generate_tempdir();
        let fuse = LoreFuse::new(
            repository.clone(),
            state.clone(),
            None,
            Some(backing.path().to_path_buf()),
            execution.clone(),
        );
        let session = fuse.spawn(remount_point.path()).expect("Failed to remount FUSE");
        let _ = read_dir_ready(remount_point.path()).expect("read_dir on remount");

        // The delete persists: its whiteout is re-seeded from the staged dirty-delete node.
        assert!(
            std::fs::metadata(remount_point.path().join("src").join("test.txt")).is_err(),
            "deleted file should stay gone after remount"
        );
        // The create and the in-place edit persist via the overlay.
        assert_eq!(
            std::fs::read(remount_point.path().join("notes.txt")).expect("read notes after remount"),
            b"notes"
        );
        assert_eq!(
            std::fs::read(remount_point.path().join("readme.md")).expect("read readme after remount"),
            b"goodbye"
        );

        drop(session);
    }
}
