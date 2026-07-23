// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Cross-VCS safety: a Lore workspace sharing its root with a git working tree must keep the
//! two tracked sets disjoint. Staging refuses git-tracked paths (the invariant that prevents
//! a backward sync from clobbering source), and destructive syncs at a shared root demand an
//! explicit `.loreignore` allowlist.

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::path::Path;
    use std::sync::Arc;

    use lore_base::error::NoRemote;
    use lore_base::runtime::LORE_CONTEXT;
    use lore_base::runtime::runtime;
    use lore_base::types::Context;
    use lore_revision::branch;
    use lore_revision::file;
    use lore_revision::interface::ExecutionContext;
    use lore_revision::interface::LoreArray;
    use lore_revision::interface::LoreString;
    use lore_revision::lore::RepositoryId;
    use lore_revision::node::NodeFlags;
    use lore_revision::repository;
    use lore_revision::repository::RepositoryContext;
    use lore_revision::repository::RepositoryFormat;
    use lore_revision::revision::sync;
    use lore_revision::stage;
    use lore_revision::stage::StageOptions;
    use lore_transport::ProtocolError;

    include!("helper.rs");

    fn git_available() -> bool {
        std::process::Command::new("git")
            .arg("--version")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    fn git(root: &Path, args: &[&str]) {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .output()
            .expect("run git");
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    struct Workspace {
        repository: Arc<RepositoryContext>,
        execution: Arc<ExecutionContext>,
        write_token: repository::RepositoryWriteToken,
        _working: TempDir,
        working_path: std::path::PathBuf,
    }

    /// An initialized (but empty) Lore repository over in-memory stores.
    fn build_workspace() -> Workspace {
        build_workspace_with_ignore(None)
    }

    /// Like [`build_workspace`], with a `.loreignore` written to the root beforehand and
    /// loaded into the repository context's filter — the way a CLI-opened repository sees it.
    fn build_workspace_with_ignore(loreignore: Option<&str>) -> Workspace {
        build_workspace_full(loreignore, None)
    }

    /// Full-control builder: `working_root` rebinds the context's working tree to a
    /// mountpoint with the given liveness, the way a repository open does for a mounted
    /// workspace.
    fn build_workspace_full(
        loreignore: Option<&str>,
        working_root: Option<(std::path::PathBuf, bool)>,
    ) -> Workspace {
        let (immutable_store, mutable_store, execution) = runtime()
            .block_on(test_store_create())
            .expect("Failed to create stores");
        let repository_id = RepositoryId::from(uuid::Uuid::now_v7());

        let working = generate_tempdir();
        let working_path = working.to_path_buf();

        let loreignore = loreignore.map(str::to_string);
        let (repository, write_token) =
            runtime().block_on(LORE_CONTEXT.scope(execution.clone(), {
                let immutable = immutable_store.clone();
                let mutable = mutable_store.clone();
                let working_path = working_path.clone();
                async move {
                    std::fs::create_dir_all(&working_path).expect("create working dir");
                    if let Some(content) = loreignore.as_deref() {
                        std::fs::write(working_path.join(".loreignore"), content)
                            .expect("write .loreignore");
                    }
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

                    // Load the ignore filter from disk exactly as a CLI open would.
                    let filter =
                        repository::load_filter(&working_path).expect("load filter");
                    let mut repository = RepositoryContext::new(
                        Some(working_path.clone()),
                        immutable,
                        mutable,
                        repository_id,
                        created.instance_id,
                        Err(ProtocolError::from(NoRemote)),
                        filter,
                        RepositoryFormat::Lore,
                    )
                    .with_write_token(write_token.share());
                    if let Some((mountpoint, live)) = working_root {
                        repository = repository.with_working_root(mountpoint, live);
                    }
                    let repository = Arc::new(repository);

                    lore_revision::instance::store_current_anchor_branch(
                        &repository,
                        default_branch_id,
                    )
                    .await
                    .expect("Failed to store anchor branch");

                    (repository, write_token)
                }
            }));

        Workspace {
            repository,
            execution,
            write_token,
            _working: working,
            working_path,
        }
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

    fn write_file(root: &Path, relative: &str, content: &[u8]) {
        let absolute = root.join(relative);
        if let Some(parent) = absolute.parent() {
            std::fs::create_dir_all(parent).expect("create parent");
        }
        let mut file = std::fs::File::create(&absolute).expect("create file");
        file.write_all(content).expect("write file");
    }

    /// Stage `path` under the given execution context, returning the error string if any.
    fn stage_path(
        workspace: &Workspace,
        execution: Arc<ExecutionContext>,
        path: &Path,
    ) -> Result<(), String> {
        runtime().block_on(LORE_CONTEXT.scope(execution, {
            let repository = workspace.repository.clone();
            let write_token = &workspace.write_token;
            let path = path.to_path_buf();
            async move {
                let paths = LoreArray::from_vec(vec![LoreString::from(&path)]);
                file::stage::stage(repository, write_token, paths, stage_options())
                    .await
                    .map(|_| ())
                    .map_err(|err| err.to_string())
            }
        }))
    }

    #[test]
    fn stage_refuses_git_tracked_paths() {
        if !git_available() {
            eprintln!("skipping cross-VCS staging test: git is not available");
            return;
        }

        let workspace = build_workspace();
        let root = &workspace.working_path;

        // A git repository shares the workspace root and tracks the source tree.
        git(root, &["init", "--quiet"]);
        git(root, &["config", "user.email", "test@example.com"]);
        git(root, &["config", "user.name", "test"]);
        write_file(root, "src/code.py", b"print(1)\n");
        write_file(root, "asset.bin", b"binary asset");
        git(root, &["add", "src/code.py"]);
        git(root, &["commit", "--quiet", "-m", "source"]);

        // Staging the git-tracked file (directly, or via a scan that reaches it) is refused.
        let error = stage_path(
            &workspace,
            workspace.execution.clone(),
            &root.join("src/code.py"),
        )
        .expect_err("staging a git-tracked file must be refused");
        assert!(
            error.contains("tracked by the git working tree"),
            "unexpected error: {error}"
        );
        let error = stage_path(&workspace, workspace.execution.clone(), root)
            .expect_err("a scan reaching git-tracked files must be refused");
        assert!(
            error.contains("tracked by the git working tree"),
            "unexpected error: {error}"
        );

        // A git-untracked asset stages fine — the sets stay disjoint.
        stage_path(
            &workspace,
            workspace.execution.clone(),
            &root.join("asset.bin"),
        )
        .expect("staging a git-untracked asset must succeed");

        // The global --force deliberately overrides the guard.
        let forced = Arc::new(ExecutionContext::new_client_with_user_id(
            LoreGlobalArgs {
                force: 1,
                ..Default::default()
            },
            lore_revision::relay::EventDispatcher::no_dispatch(),
            "test-user".to_string(),
        ));
        stage_path(&workspace, forced, &root.join("src/code.py"))
            .expect("--force must override the cross-VCS guard");
    }

    /// Stage a path and commit the staged state.
    fn stage_and_commit(workspace: &Workspace, path: &Path, message: &str) {
        runtime().block_on(LORE_CONTEXT.scope(workspace.execution.clone(), {
            let repository = workspace.repository.clone();
            let write_token = &workspace.write_token;
            let path = path.to_path_buf();
            let message = message.to_string();
            async move {
                let paths = LoreArray::from_vec(vec![LoreString::from(&path)]);
                file::stage::stage(
                    repository.clone(),
                    write_token,
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
                .expect("stage");
                Box::pin(lore_revision::commit::commit(
                    repository,
                    write_token,
                    lore_revision::commit::CommitOptions::new(message),
                ))
                .await
                .expect("commit");
            }
        }));
    }

    // GRID VF-5: at a root with a correct allowlist, `sync --reset` must retain foreign
    // (allowlist-ignored) files exactly as a normal sync does — never delete them to match
    // the target revision.
    #[test]
    fn sync_reset_retains_allowlist_ignored_foreign_files() {
        let workspace = build_workspace_with_ignore(Some("*\n!robots/\n"));
        let root = workspace.working_path.clone();

        // Committed Lore content: the asset subtree.
        write_file(&root, "robots/mock/asset.usd", b"asset payload");
        stage_and_commit(&workspace, &root.join("robots"), "asset");

        // Foreign source files, never staged (the allowlist ignores them).
        write_file(&root, "src/app.py", b"print(1)\n");
        write_file(&root, "readme.md", b"# source\n");

        // Destructive reset to the current revision.
        let result = runtime().block_on(LORE_CONTEXT.scope(workspace.execution.clone(), {
            let repository = workspace.repository.clone();
            let write_token = &workspace.write_token;
            async move {
                sync::sync(
                    repository,
                    write_token,
                    sync::SyncOptions {
                        reset: true,
                        ..Default::default()
                    },
                )
                .await
            }
        }));
        if let Err(err) = &result {
            eprintln!("sync --reset returned: {err}");
        }

        assert!(
            root.join("src/app.py").exists(),
            "sync --reset must retain allowlist-ignored foreign files (src/app.py deleted)"
        );
        assert!(
            root.join("readme.md").exists(),
            "sync --reset must retain allowlist-ignored foreign files (readme.md deleted)"
        );
        assert!(
            root.join("robots/mock/asset.usd").exists(),
            "the Lore-tracked asset must survive the reset"
        );
    }

    // The failure mode behind GRID VF-5: with an allowlist that exists but parses to nothing
    // effective (e.g. written on a single line), `sync --reset` sees every foreign file as
    // unfiltered and deletes it. The cross-VCS deletion guard must retain git-tracked files
    // regardless of allowlist quality.
    #[test]
    fn sync_reset_never_deletes_git_tracked_files() {
        if !git_available() {
            eprintln!("skipping cross-VCS reset test: git is not available");
            return;
        }

        // A malformed allowlist: one line, so the '*' never takes effect as a base exclude.
        let workspace = build_workspace_with_ignore(Some("* !robots/\n"));
        let root = workspace.working_path.clone();

        // Foreign git-tracked source, present before any Lore activity (the repository
        // context snapshots git presence on first use).
        git(&root, &["init", "--quiet"]);
        git(&root, &["config", "user.email", "test@example.com"]);
        git(&root, &["config", "user.name", "test"]);
        write_file(&root, "src/app.py", b"print(1)\n");
        write_file(&root, "readme.md", b"# source\n");
        write_file(&root, "scratch.txt", b"untracked scratch");
        git(&root, &["add", "src/app.py", "readme.md"]);
        git(&root, &["commit", "--quiet", "-m", "source"]);

        write_file(&root, "robots/mock/asset.usd", b"asset payload");
        stage_and_commit(&workspace, &root.join("robots"), "asset");

        let result = runtime().block_on(LORE_CONTEXT.scope(workspace.execution.clone(), {
            let repository = workspace.repository.clone();
            let write_token = &workspace.write_token;
            async move {
                sync::sync(
                    repository,
                    write_token,
                    sync::SyncOptions {
                        reset: true,
                        ..Default::default()
                    },
                )
                .await
            }
        }));
        if let Err(err) = &result {
            eprintln!("sync --reset returned: {err}");
        }

        assert!(
            root.join("src/app.py").exists(),
            "sync --reset must never delete git-tracked source (src/app.py)"
        );
        assert!(
            root.join("readme.md").exists(),
            "sync --reset must never delete git-tracked source (readme.md)"
        );
        assert!(
            root.join("robots/mock/asset.usd").exists(),
            "the Lore-tracked asset must survive the reset"
        );
    }

    // GRID VF-4: the mountpoint binding redirects the working tree while repository metadata
    // stays at the original directory, and the binding file round-trips.
    #[test]
    fn mountpoint_binding_splits_working_tree_from_metadata() {
        let mountpoint = generate_tempdir();
        let workspace =
            build_workspace_full(None, Some((mountpoint.to_path_buf(), true)));

        assert_eq!(
            workspace.repository.require_path().expect("working tree"),
            mountpoint.path(),
            "the working tree must be the mountpoint"
        );
        assert_eq!(
            workspace
                .repository
                .require_metadata_path()
                .expect("metadata root"),
            workspace.working_path.as_path(),
            "repository metadata must stay at the original directory"
        );
        assert_eq!(workspace.repository.working_root_binding(), Some(true));

        // Binding persistence round-trip.
        let dot = workspace.working_path.join(".lore");
        repository::write_mount_binding(&dot, mountpoint.path()).expect("write binding");
        assert_eq!(
            repository::read_mount_binding(&dot).expect("read binding"),
            mountpoint.path()
        );
    }

    // GRID VF-4: scan-driven staging and sync refuse a mountpoint binding whose mount is not
    // being served — a partial tree must not be misread as deletions — while dirty-driven
    // staging (the default) stays available.
    #[test]
    fn scan_and_sync_refuse_dead_mountpoint_binding() {
        let mountpoint = generate_tempdir();
        let workspace =
            build_workspace_full(None, Some((mountpoint.to_path_buf(), false)));

        let error = stage_path(
            &workspace,
            workspace.execution.clone(),
            mountpoint.path(),
        )
        .expect_err("a scan against a dead mountpoint binding must be refused");
        assert!(
            error.contains("not") && error.contains("mounted"),
            "unexpected error: {error}"
        );

        let error = runtime().block_on(LORE_CONTEXT.scope(workspace.execution.clone(), {
            let repository = workspace.repository.clone();
            let write_token = &workspace.write_token;
            async move {
                sync::sync(repository, write_token, sync::SyncOptions::default())
                    .await
                    .map_err(|err| err.to_string())
            }
        }))
        .expect_err("sync against a dead mountpoint binding must be refused");
        assert!(
            error.contains("not") && error.contains("mounted"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn destructive_sync_requires_allowlist_at_git_root() {
        let workspace = build_workspace();
        let root = &workspace.working_path;

        // A shared root: .git present (a plain directory suffices for detection), no
        // .loreignore allowlist.
        std::fs::create_dir_all(root.join(".git")).expect("create .git");

        let sync_reset = |workspace: &Workspace| -> Result<(), String> {
            runtime().block_on(LORE_CONTEXT.scope(workspace.execution.clone(), {
                let repository = workspace.repository.clone();
                let write_token = &workspace.write_token;
                async move {
                    sync::sync(
                        repository,
                        write_token,
                        sync::SyncOptions {
                            reset: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|err| err.to_string())
                }
            }))
        };

        let error = sync_reset(&workspace).expect_err("destructive sync must be refused");
        assert!(
            error.contains("allowlist"),
            "expected the allowlist fail-safe, got: {error}"
        );

        // With an allowlist present the fail-safe no longer triggers (the sync proceeds and
        // may fail later for unrelated reasons — no remote — but not with the fail-safe).
        write_file(root, ".loreignore", b"*\n!assets/\n");
        match sync_reset(&workspace) {
            Ok(()) => {}
            Err(error) => assert!(
                !error.contains("allowlist"),
                "fail-safe must not trigger with an allowlist present: {error}"
            ),
        }
    }
}
