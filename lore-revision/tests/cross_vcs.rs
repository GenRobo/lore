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
        let (immutable_store, mutable_store, execution) = runtime()
            .block_on(test_store_create())
            .expect("Failed to create stores");
        let repository_id = RepositoryId::from(uuid::Uuid::now_v7());

        let working = generate_tempdir();
        let working_path = working.to_path_buf();

        let (repository, write_token) =
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
