// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![cfg(all(target_family = "windows", feature = "vfs"))]

//! Integration test for the Windows ProjFS backend, mirroring the FUSE test so the two
//! platforms are held to the same Lore-facing behavior: lazy projection, write interception
//! into dirty tracking, delete persistence, and live reconcile when the branch anchor
//! advances. Skipped when the Windows Projected File System optional feature is not enabled
//! (`Enable-WindowsOptionalFeature -Online -FeatureName Client-ProjFS`).

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
    use lore_revision::file::dirty::ExplicitDirty;
    use lore_revision::interface::ExecutionContext;
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
    use lore_revision::util::path::RelativePath;
    use lore_transport::ProtocolError;

    include!("helper.rs");

    /// Serializes the ProjFS tests: `serve` sets the process working directory, so two mounts
    /// in one test binary must not overlap.
    static SERVE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn projfs_available() -> bool {
        let Some(system_root) = std::env::var_os("SystemRoot") else {
            return false;
        };
        Path::new(&system_root)
            .join("System32")
            .join("ProjectedFSLib.dll")
            .exists()
    }

    /// Run a filesystem mutation from a child process. ProjFS suppresses notifications for the
    /// provider process's own I/O (by design, to avoid recursion), so writes that must reach
    /// dirty tracking have to originate from another process — as they do in real use, where
    /// the serving `lore` process and the editing application are distinct.
    fn mutate_from_child(command: &str) {
        let output = std::process::Command::new("powershell")
            .args(["-NoProfile", "-Command", command])
            .output()
            .expect("spawn powershell");
        assert!(
            output.status.success(),
            "powershell mutation failed: {command}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    /// A path in plain `C:\...` form (child processes reject the `\\?\` prefix).
    fn plain_path(path: &Path) -> String {
        let display = path.display().to_string();
        display
            .strip_prefix("\\\\?\\")
            .map(str::to_string)
            .unwrap_or(display)
    }

    /// Poll `cond` until it holds or the deadline passes.
    fn wait_for(deadline: Duration, mut cond: impl FnMut() -> bool) -> bool {
        let steps = (deadline.as_millis() / 100).max(1);
        for _ in 0..steps {
            if cond() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        cond()
    }

    /// Serve a ProjFS mount on a background thread; the thread returns once `unmount` signals
    /// the mount's stop event.
    fn spawn_serve(
        mountpoint: &Path,
        repository: Arc<RepositoryContext>,
        state: Arc<State>,
        execution: Arc<ExecutionContext>,
    ) -> std::thread::JoinHandle<()> {
        let mountpoint = mountpoint.to_path_buf();
        std::thread::spawn(move || {
            runtime().block_on(LORE_CONTEXT.scope(execution, async move {
                lore_revision::projfs::serve::serve(&mountpoint, repository, state, None, None);
            }));
        })
    }

    fn stage_options() -> StageOptions {
        StageOptions {
            case_change: stage::StageCaseChange::Error,
            node_flags: NodeFlags::NoFlags,
            file_id: None,
            no_children: false,
            scan: true,
        }
    }

    struct TestRepository {
        repository: Arc<RepositoryContext>,
        state: Arc<State>,
        execution: Arc<ExecutionContext>,
        write_token: repository::RepositoryWriteToken,
        /// Kept alive: dropping it deletes the repository working directory.
        _working: TempDir,
        working_path: std::path::PathBuf,
    }

    /// Build a committed repository in an in-memory store from `files` (relative path,
    /// content) pairs — the shared fixture both scenarios start from.
    fn build_repository(files: &[(&str, &[u8])]) -> TestRepository {
        let (immutable_store, mutable_store, execution) = runtime()
            .block_on(test_store_create())
            .expect("Failed to create stores");
        let repository_id = RepositoryId::from(uuid::Uuid::now_v7());

        let working = generate_tempdir();
        let working_path = working.to_path_buf();

        let files: Vec<(String, Vec<u8>)> = files
            .iter()
            .map(|(path, content)| (path.to_string(), content.to_vec()))
            .collect();

        let (repository, state, write_token) =
            runtime().block_on(LORE_CONTEXT.scope(execution.clone(), {
                let immutable = immutable_store.clone();
                let mutable = mutable_store.clone();
                let working_path = working_path.clone();
                async move {
                    std::fs::create_dir_all(&working_path).expect("create working dir");
                    let default_branch_id = Context::from(uuid::Uuid::now_v7());
                    let write_token =
                        repository::RepositoryWriteToken::acquire(&working_path).await;
                    let created = repository::create_local(
                        &working_path,
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
                            Some(working_path.clone()),
                            immutable,
                            mutable,
                            repository_id,
                            created.instance_id,
                            Err(ProtocolError::from(NoRemote)),
                            Arc::default(),
                            RepositoryFormat::Lore,
                        )
                        .with_write_token(write_token.share()),
                    );

                    lore_revision::instance::store_current_anchor_branch(
                        &repository,
                        default_branch_id,
                    )
                    .await
                    .expect("Failed to store anchor branch");

                    for (path, content) in &files {
                        let absolute = working_path.join(path);
                        if let Some(parent) = absolute.parent() {
                            std::fs::create_dir_all(parent).expect("create parent dir");
                        }
                        let mut f = std::fs::File::create(&absolute).expect("create file");
                        f.write_all(content).expect("write file");
                    }

                    let paths = LoreArray::from_vec(vec![LoreString::from(&working_path)]);
                    file::stage::stage(repository.clone(), &write_token, paths, stage_options())
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

                    (repository, state, write_token)
                }
            }));

        TestRepository {
            repository,
            state,
            execution,
            write_token,
            _working: working,
            working_path,
        }
    }

    /// Load a path's node from the staged (falling back to current) state and test a flag.
    fn node_state(
        repository: &Arc<RepositoryContext>,
        execution: &Arc<ExecutionContext>,
        path: &str,
        check: impl Fn(&lore_revision::node::Node) -> bool,
    ) -> bool {
        runtime().block_on(LORE_CONTEXT.scope(execution.clone(), {
            let repository = repository.clone();
            let path = path.to_string();
            async move {
                let Ok((current, staged, _)) =
                    State::deserialize_current_and_staged(repository.clone()).await
                else {
                    return false;
                };
                let state = staged.unwrap_or_else(|| current.clone());
                match state.find_node_link(repository.clone(), path.as_str()).await {
                    Ok(link) if link.is_valid() => state
                        .node(repository.clone(), link.node)
                        .await
                        .map(|node| check(&node))
                        .unwrap_or(false),
                    _ => false,
                }
            }
        }))
    }

    #[test]
    // The test drives the mount with ordinary filesystem syscalls (including remove_file on the
    // mountpoint); those are user-facing FS operations, not repository-internal writes.
    #[allow(clippy::disallowed_methods)]
    fn mount_projects_lore_and_tracks_writes() {
        if !projfs_available() {
            eprintln!("skipping ProjFS mount test: ProjectedFSLib.dll is not available");
            return;
        }
        let _guard = SERVE_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());

        let fixture = build_repository(&[
            ("readme.md", b"hello world"),
            ("src/test.txt", b"hello"),
            ("extra.txt", b"extra"),
            // Never touched while mounted, so it stays purely virtual: its visibility proves
            // the provider is attached (files materialized by earlier operations exist on disk
            // and would satisfy a readiness probe before the provider is up).
            ("virtual-marker.txt", b"marker"),
        ]);

        let mountpoint = generate_tempdir();
        let serve_thread = spawn_serve(
            mountpoint.path(),
            fixture.repository.clone(),
            fixture.state.clone(),
            fixture.execution.clone(),
        );
        if !wait_for(Duration::from_secs(10), || mountpoint
            .path()
            .join("readme.md")
            .exists())
        {
            if serve_thread.is_finished() {
                panic!("serve exited early: {:?}", serve_thread.join());
            }
            panic!("mount never became ready (serve still running)");
        }

        // The mount presents the projected Lore tree.
        let names: HashSet<String> = std::fs::read_dir(mountpoint.path())
            .expect("read_dir on mount")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        assert!(names.contains("readme.md"), "projected file missing: {names:?}");
        assert!(names.contains("src"), "projected dir missing: {names:?}");

        // Lazy hydration of projected content.
        assert_eq!(
            std::fs::read(mountpoint.path().join("readme.md")).expect("read readme"),
            b"hello world"
        );
        assert_eq!(
            std::fs::read(mountpoint.path().join("src/test.txt")).expect("read nested"),
            b"hello"
        );

        // Writeback: edit a projected file in place through the mount; the new content is
        // served back and the edit is reported to Lore's dirty tracking.
        mutate_from_child(&format!(
            "Set-Content -LiteralPath '{}\\readme.md' -Value 'goodbye' -NoNewline",
            plain_path(mountpoint.path())
        ));
        assert_eq!(
            std::fs::read(mountpoint.path().join("readme.md")).expect("re-read readme"),
            b"goodbye"
        );
        assert!(
            wait_for(Duration::from_secs(5), || node_state(
                &fixture.repository,
                &fixture.execution,
                "readme.md",
                |node| node.is_dirty_modify(),
            )),
            "readme.md should be dirty-modify after an in-place edit"
        );

        // Create a new file through the mount.
        mutate_from_child(&format!(
            "Set-Content -LiteralPath '{}\\notes.txt' -Value 'notes' -NoNewline",
            plain_path(mountpoint.path())
        ));
        assert_eq!(
            std::fs::read(mountpoint.path().join("notes.txt")).expect("read notes.txt"),
            b"notes"
        );
        assert!(
            wait_for(Duration::from_secs(5), || node_state(
                &fixture.repository,
                &fixture.execution,
                "notes.txt",
                |node| node.is_dirty_add(),
            )),
            "notes.txt should be dirty-add after creation"
        );

        // Delete a projected file through the mount; it disappears (ProjFS tombstone) and the
        // delete is tracked.
        mutate_from_child(&format!(
            "Remove-Item -LiteralPath '{}\\src\\test.txt'",
            plain_path(mountpoint.path())
        ));
        assert!(
            std::fs::metadata(mountpoint.path().join("src").join("test.txt")).is_err(),
            "deleted projected file should be gone from the mount"
        );
        assert!(
            wait_for(Duration::from_secs(5), || node_state(
                &fixture.repository,
                &fixture.execution,
                "src/test.txt",
                |node| node.is_dirty_delete(),
            )),
            "src/test.txt should be dirty-delete after removal"
        );

        // Unmount.
        lore_revision::projfs::serve::unmount(mountpoint.path()).expect("unmount");
        serve_thread.join().expect("serve thread");

        // Stage a delete outside the mount (as the CLI or a sync would).
        runtime().block_on(LORE_CONTEXT.scope(fixture.execution.clone(), {
            let repository = fixture.repository.clone();
            async move {
                let path = RelativePath::new_from_initial_path("extra.txt").expect("path");
                lore_revision::file::dirty::dirty_explicit(
                    repository,
                    vec![path],
                    ExplicitDirty::Delete,
                )
                .await
                .expect("stage out-of-band delete");
            }
        }));

        // Remount at the same root: the through-the-mount delete persists via its tombstone,
        // the out-of-band staged delete is hidden by the reseeded hidden set, and local edits
        // and creations persist as materialized files. Readiness is probed through the
        // purely-virtual marker, which only the attached provider can serve.
        let serve_thread = spawn_serve(
            mountpoint.path(),
            fixture.repository.clone(),
            fixture.state.clone(),
            fixture.execution.clone(),
        );
        assert!(
            wait_for(Duration::from_secs(10), || mountpoint
                .path()
                .join("virtual-marker.txt")
                .exists()),
            "remount never became ready"
        );

        assert!(
            std::fs::metadata(mountpoint.path().join("src").join("test.txt")).is_err(),
            "deleted file should stay gone after remount"
        );
        assert!(
            wait_for(Duration::from_secs(5), || std::fs::metadata(
                mountpoint.path().join("extra.txt")
            )
            .is_err()),
            "out-of-band staged delete should be hidden after remount"
        );
        assert_eq!(
            std::fs::read(mountpoint.path().join("notes.txt")).expect("read notes after remount"),
            b"notes"
        );
        assert_eq!(
            std::fs::read(mountpoint.path().join("readme.md")).expect("read readme after remount"),
            b"goodbye"
        );

        lore_revision::projfs::serve::unmount(mountpoint.path()).expect("unmount after remount");
        serve_thread.join().expect("serve thread after remount");
    }

    // Committing a new revision while mounted is picked up by the reconcile poller, including
    // refreshing placeholders ProjFS has already materialized on disk.
    #[test]
    #[allow(clippy::disallowed_methods)]
    fn mount_reconciles_after_new_revision() {
        if !projfs_available() {
            eprintln!("skipping ProjFS reconcile test: ProjectedFSLib.dll is not available");
            return;
        }
        let _guard = SERVE_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());

        let fixture = build_repository(&[("readme.md", b"hello")]);

        let mountpoint = generate_tempdir();
        let serve_thread = spawn_serve(
            mountpoint.path(),
            fixture.repository.clone(),
            fixture.state.clone(),
            fixture.execution.clone(),
        );
        assert!(
            wait_for(Duration::from_secs(10), || mountpoint
                .path()
                .join("readme.md")
                .exists()),
            "mount never became ready"
        );

        // Hydrate readme.md so reconcile has a materialized placeholder to refresh.
        assert_eq!(
            std::fs::read(mountpoint.path().join("readme.md")).expect("read readme"),
            b"hello"
        );
        assert!(
            !mountpoint.path().join("phase2.txt").exists(),
            "phase2.txt should not exist before the second revision"
        );

        // Commit revision 2 (adds phase2.txt, rewrites readme.md) while the mount is live.
        runtime().block_on(LORE_CONTEXT.scope(fixture.execution.clone(), {
            let repository = fixture.repository.clone();
            let working_path = fixture.working_path.clone();
            let write_token = &fixture.write_token;
            async move {
                {
                    let mut f = std::fs::File::create(working_path.join("phase2.txt"))
                        .expect("create phase2");
                    f.write_all(b"phase2").expect("write phase2");
                }
                {
                    let mut f = std::fs::File::create(working_path.join("readme.md"))
                        .expect("rewrite readme");
                    f.write_all(b"hello v2").expect("write readme v2");
                }
                let paths = LoreArray::from_vec(vec![LoreString::from(&working_path)]);
                file::stage::stage(repository.clone(), write_token, paths, stage_options())
                    .await
                    .expect("stage rev2");
                Box::pin(commit::commit(
                    repository.clone(),
                    write_token,
                    CommitOptions::new("rev2".to_string()),
                ))
                .await
                .expect("commit rev2");
            }
        }));

        // The poller reconciles the mount to the new revision; phase2.txt appears.
        assert!(
            wait_for(Duration::from_secs(20), || mountpoint
                .path()
                .join("phase2.txt")
                .exists()),
            "phase2.txt from the new revision should appear after reconcile"
        );
        assert_eq!(
            std::fs::read(mountpoint.path().join("phase2.txt")).expect("read phase2"),
            b"phase2"
        );

        // The already-hydrated readme.md placeholder is refreshed to the new content.
        assert!(
            wait_for(Duration::from_secs(20), || {
                std::fs::read(mountpoint.path().join("readme.md"))
                    .map(|content| content == b"hello v2")
                    .unwrap_or(false)
            }),
            "hydrated readme.md should serve the new revision's content after reconcile"
        );

        lore_revision::projfs::serve::unmount(mountpoint.path()).expect("unmount");
        serve_thread.join().expect("serve thread");
    }

    /// Whether a node is marked dirty in any way that would surface through `lore status`.
    fn is_dirty_any(node: &lore_revision::node::Node) -> bool {
        node.is_dirty_add() || node.is_dirty_modify() || node.is_dirty_delete()
    }

    // Extended editor-style soak: repeated direct saves, editor save-via-rename (write .tmp,
    // rename over the original), creations, and deletions from child processes, with a
    // concurrent reader hammering lazy hydration. Ignored by default; run with
    // `cargo test -p lore-revision --features vfs --test vfs_projfs -- --ignored`.
    #[test]
    #[ignore = "extended soak; run explicitly with -- --ignored"]
    #[allow(clippy::disallowed_methods)]
    fn mount_soak_editor_workload() {
        if !projfs_available() {
            eprintln!("skipping ProjFS soak: ProjectedFSLib.dll is not available");
            return;
        }
        let _guard = SERVE_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());

        let iterations: usize = std::env::var("LORE_VFS_SOAK_ITERS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(10);

        let mut files: Vec<(String, Vec<u8>)> = Vec::new();
        for index in 0..10 {
            files.push((format!("docs/d{index:02}.md"), format!("doc-{index} base").into_bytes()));
            files.push((
                format!("assets/a{index:02}.txt"),
                format!("asset-{index} base").into_bytes(),
            ));
        }
        // Never touched while mounted: stays purely virtual, so its visibility proves the
        // provider is attached (see the readiness note in the basic test).
        files.push(("virtual-marker.txt".to_string(), b"marker".to_vec()));
        let fixture_files: Vec<(&str, &[u8])> = files
            .iter()
            .map(|(path, content)| (path.as_str(), content.as_slice()))
            .collect();
        let fixture = build_repository(&fixture_files);

        let mountpoint = generate_tempdir();
        let serve_thread = spawn_serve(
            mountpoint.path(),
            fixture.repository.clone(),
            fixture.state.clone(),
            fixture.execution.clone(),
        );
        assert!(
            wait_for(Duration::from_secs(10), || mountpoint
                .path()
                .join("docs/d00.md")
                .exists()),
            "soak mount never became ready"
        );

        // Concurrent reader over files the mutation loop never touches (assets a05..a09):
        // exercises hydration racing the writes without content ambiguity.
        let reader_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reader_errors = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let reader = {
            let stop = reader_stop.clone();
            let errors = reader_errors.clone();
            let root = mountpoint.path().to_path_buf();
            std::thread::spawn(move || {
                let mut index = 5;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let path = root.join(format!("assets/a{index:02}.txt"));
                    match std::fs::read(&path) {
                        Ok(content) => {
                            if content != format!("asset-{index} base").into_bytes() {
                                errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    index = 5 + (index + 1) % 5;
                    std::thread::sleep(Duration::from_millis(5));
                }
            })
        };

        let root = plain_path(mountpoint.path());
        for iteration in 0..iterations {
            let direct = iteration % 10;
            let renamed = (iteration + 1) % 10;
            let mut script = format!(
                "$ErrorActionPreference = 'Stop'\n\
                 Set-Content -LiteralPath '{root}\\docs\\d{direct:02}.md' -Value 'iter{iteration}-direct' -NoNewline\n\
                 Set-Content -LiteralPath '{root}\\docs\\d{renamed:02}.md.tmp' -Value 'iter{iteration}-rename' -NoNewline\n\
                 Move-Item -LiteralPath '{root}\\docs\\d{renamed:02}.md.tmp' -Destination '{root}\\docs\\d{renamed:02}.md' -Force\n\
                 Set-Content -LiteralPath '{root}\\created_{iteration}.txt' -Value 'created{iteration}' -NoNewline\n"
            );
            let victim = iteration / 3;
            if iteration % 3 == 0 && victim < 5 {
                // a00..a04 are the deletable pool; a05..a09 belong to the concurrent reader.
                script.push_str(&format!(
                    "Remove-Item -LiteralPath '{root}\\assets\\a{victim:02}.txt'\n"
                ));
            }
            mutate_from_child(&script);

            // Read back through the mount: the latest write wins immediately.
            assert_eq!(
                std::fs::read(mountpoint.path().join(format!("docs/d{direct:02}.md")))
                    .expect("read direct edit"),
                format!("iter{iteration}-direct").into_bytes(),
                "direct save content mismatch at iteration {iteration}"
            );
            assert_eq!(
                std::fs::read(mountpoint.path().join(format!("docs/d{renamed:02}.md")))
                    .expect("read renamed edit"),
                format!("iter{iteration}-rename").into_bytes(),
                "save-via-rename content mismatch at iteration {iteration}"
            );
            assert_eq!(
                std::fs::read(mountpoint.path().join(format!("created_{iteration}.txt")))
                    .expect("read created file"),
                format!("created{iteration}").into_bytes(),
            );
            eprintln!("soak iteration {iteration} ok");
        }

        reader_stop.store(true, std::sync::atomic::Ordering::Relaxed);
        reader.join().expect("reader thread");
        assert_eq!(
            reader_errors.load(std::sync::atomic::Ordering::Relaxed),
            0,
            "concurrent reader observed errors or wrong content"
        );

        // Dirty tracking reflects the workload (notifications are asynchronous; poll).
        let last = iterations - 1;
        let direct = last % 10;
        assert!(
            wait_for(Duration::from_secs(10), || node_state(
                &fixture.repository,
                &fixture.execution,
                &format!("docs/d{direct:02}.md"),
                is_dirty_any,
            )),
            "directly saved file should be dirty"
        );
        assert!(
            wait_for(Duration::from_secs(10), || node_state(
                &fixture.repository,
                &fixture.execution,
                &format!("created_{last}.txt"),
                |node| node.is_dirty_add(),
            )),
            "created file should be dirty-add"
        );
        assert!(
            wait_for(Duration::from_secs(10), || node_state(
                &fixture.repository,
                &fixture.execution,
                "assets/a00.txt",
                |node| node.is_dirty_delete(),
            )),
            "deleted asset should be dirty-delete"
        );

        // Repository internals never leak into tracking.
        assert!(
            !node_state(&fixture.repository, &fixture.execution, ".lore", is_dirty_any),
            ".lore must not be tracked"
        );
        assert!(
            !node_state(
                &fixture.repository,
                &fixture.execution,
                ".lore/.projfsid",
                is_dirty_any
            ),
            ".lore/.projfsid must not be tracked"
        );

        // Clean unmount, then remount and spot-check persistence.
        lore_revision::projfs::serve::unmount(mountpoint.path()).expect("soak unmount");
        serve_thread.join().expect("soak serve thread");

        let serve_thread = spawn_serve(
            mountpoint.path(),
            fixture.repository.clone(),
            fixture.state.clone(),
            fixture.execution.clone(),
        );
        assert!(
            wait_for(Duration::from_secs(10), || mountpoint
                .path()
                .join("virtual-marker.txt")
                .exists()),
            "soak remount never became ready"
        );
        assert!(
            std::fs::metadata(mountpoint.path().join("assets/a00.txt")).is_err(),
            "deleted asset should stay gone after remount"
        );
        assert_eq!(
            std::fs::read(mountpoint.path().join(format!("created_{last}.txt")))
                .expect("created file after remount"),
            format!("created{last}").into_bytes(),
        );
        assert_eq!(
            std::fs::read(mountpoint.path().join(format!("docs/d{direct:02}.md")))
                .expect("edited file after remount"),
            format!("iter{last}-direct").into_bytes(),
        );

        lore_revision::projfs::serve::unmount(mountpoint.path()).expect("final unmount");
        serve_thread.join().expect("final serve thread");
    }

    // Repeated commit → reconcile cycles against a live mount: every round adds a file and
    // rewrites a baseline file the mount has already hydrated, so each reconcile must both
    // project the new entry and refresh a materialized placeholder.
    #[test]
    #[ignore = "extended soak; run explicitly with -- --ignored"]
    #[allow(clippy::disallowed_methods)]
    fn mount_soak_reconcile_cycles() {
        if !projfs_available() {
            eprintln!("skipping ProjFS soak: ProjectedFSLib.dll is not available");
            return;
        }
        let _guard = SERVE_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());

        let rounds: usize = std::env::var("LORE_VFS_SOAK_ROUNDS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(4);

        let fixture = build_repository(&[("baseline.txt", b"baseline r1"), ("stable.txt", b"stable")]);

        let mountpoint = generate_tempdir();
        let serve_thread = spawn_serve(
            mountpoint.path(),
            fixture.repository.clone(),
            fixture.state.clone(),
            fixture.execution.clone(),
        );
        assert!(
            wait_for(Duration::from_secs(10), || mountpoint
                .path()
                .join("stable.txt")
                .exists()),
            "reconcile soak mount never became ready"
        );
        assert_eq!(
            std::fs::read(mountpoint.path().join("baseline.txt")).expect("hydrate baseline"),
            b"baseline r1"
        );

        for round in 2..(2 + rounds) {
            runtime().block_on(LORE_CONTEXT.scope(fixture.execution.clone(), {
                let repository = fixture.repository.clone();
                let working_path = fixture.working_path.clone();
                let write_token = &fixture.write_token;
                async move {
                    std::fs::write(
                        working_path.join(format!("phase{round}.txt")),
                        format!("phase{round}"),
                    )
                    .expect("write phase file");
                    std::fs::write(
                        working_path.join("baseline.txt"),
                        format!("baseline r{round}"),
                    )
                    .expect("rewrite baseline");
                    let paths = LoreArray::from_vec(vec![LoreString::from(&working_path)]);
                    file::stage::stage(repository.clone(), write_token, paths, stage_options())
                        .await
                        .expect("stage round");
                    Box::pin(commit::commit(
                        repository.clone(),
                        write_token,
                        CommitOptions::new(format!("round {round}")),
                    ))
                    .await
                    .expect("commit round");
                }
            }));

            assert!(
                wait_for(Duration::from_secs(20), || {
                    mountpoint.path().join(format!("phase{round}.txt")).exists()
                        && std::fs::read(mountpoint.path().join("baseline.txt"))
                            .map(|content| content == format!("baseline r{round}").into_bytes())
                            .unwrap_or(false)
                }),
                "round {round}: mount should reconcile to the new revision"
            );
            eprintln!("reconcile round {round} ok");
        }

        lore_revision::projfs::serve::unmount(mountpoint.path()).expect("unmount");
        serve_thread.join().expect("serve thread");
    }

    // The `clone --virtually` workflow end to end: the repository directory itself is the
    // virtualization root, an external process edits through it, and the edits are staged and
    // committed from the materialized files — the write-tracking and content paths must agree.
    #[test]
    #[ignore = "extended soak; run explicitly with -- --ignored"]
    #[allow(clippy::disallowed_methods)]
    fn mount_soak_commit_roundtrip_in_place() {
        if !projfs_available() {
            eprintln!("skipping ProjFS soak: ProjectedFSLib.dll is not available");
            return;
        }
        let _guard = SERVE_LOCK.lock().unwrap_or_else(|poison| poison.into_inner());

        let fixture = build_repository(&[("readme.md", b"hello"), ("data/blob.bin", b"blob")]);

        // Serve at the repository directory itself, as clone --virtually does.
        let serve_thread = spawn_serve(
            &fixture.working_path,
            fixture.repository.clone(),
            fixture.state.clone(),
            fixture.execution.clone(),
        );
        // Everything is physically present here, so readiness cannot be probed through a
        // virtual file; wait for the notification pipeline instead by making an edit and
        // waiting for its dirty flag.
        let root = plain_path(&fixture.working_path);
        assert!(
            wait_for(Duration::from_secs(10), || {
                mutate_from_child(&format!(
                    "Set-Content -LiteralPath '{root}\\readme.md' -Value 'edited in place' -NoNewline"
                ));
                node_state(&fixture.repository, &fixture.execution, "readme.md", |node| {
                    node.is_dirty_modify()
                })
            }),
            "in-place edit should be tracked as dirty-modify"
        );
        mutate_from_child(&format!(
            "Set-Content -LiteralPath '{root}\\new_asset.txt' -Value 'fresh' -NoNewline"
        ));
        assert!(
            wait_for(Duration::from_secs(10), || node_state(
                &fixture.repository,
                &fixture.execution,
                "new_asset.txt",
                |node| node.is_dirty_add(),
            )),
            "new file should be tracked as dirty-add"
        );

        // Stage and commit from the materialized files.
        runtime().block_on(LORE_CONTEXT.scope(fixture.execution.clone(), {
            let repository = fixture.repository.clone();
            let working_path = fixture.working_path.clone();
            let write_token = &fixture.write_token;
            async move {
                let paths = LoreArray::from_vec(vec![LoreString::from(&working_path)]);
                file::stage::stage(repository.clone(), write_token, paths, stage_options())
                    .await
                    .expect("stage mount edits");
                Box::pin(commit::commit(
                    repository.clone(),
                    write_token,
                    CommitOptions::new("commit through-mount edits".to_string()),
                ))
                .await
                .expect("commit mount edits");
            }
        }));

        // The committed revision contains the through-the-mount content.
        let (readme_size, asset_size) =
            runtime().block_on(LORE_CONTEXT.scope(fixture.execution.clone(), {
                let repository = fixture.repository.clone();
                async move {
                    let (current, _, _) = State::deserialize_current_and_staged(repository.clone())
                        .await
                        .expect("load committed state");
                    let readme = current
                        .find_node_link(repository.clone(), "readme.md")
                        .await
                        .expect("readme link");
                    let readme = current
                        .node(repository.clone(), readme.node)
                        .await
                        .expect("readme node");
                    let asset = current
                        .find_node_link(repository.clone(), "new_asset.txt")
                        .await
                        .expect("asset link");
                    let asset = current
                        .node(repository.clone(), asset.node)
                        .await
                        .expect("asset node");
                    (readme.size, asset.size)
                }
            }));
        assert_eq!(readme_size, b"edited in place".len() as u64);
        assert_eq!(asset_size, b"fresh".len() as u64);

        lore_revision::projfs::serve::unmount(&fixture.working_path).expect("unmount");
        serve_thread.join().expect("serve thread");
    }
}
