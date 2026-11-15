// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! A file and a directory at one path (GRID#3302).
//!
//! Realization replaces an entry of the other type only when nothing is lost with it: content
//! the current revision tracks, unmodified, is removed and the target written; local content
//! (untracked, ignored, modified) refuses the whole operation before anything is changed,
//! unless the operation is forced or a reset.

#![allow(clippy::disallowed_methods)] // Test fixtures write to the filesystem outside the repo write-token discipline.

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;

    use lore_base::types::Hash;
    use lore_revision::branch;
    use lore_revision::commit;
    use lore_revision::commit::CommitOptions;
    use lore_revision::file;
    use lore_revision::filter::FilterMode;
    use lore_revision::interface::ExecutionContext;
    use lore_revision::interface::LoreArray;
    use lore_revision::interface::LoreString;
    use lore_revision::lore::BranchId;
    use lore_revision::lore::LORE_CONTEXT;
    use lore_revision::lore::RepositoryId;
    use lore_revision::lore::runtime;
    use lore_revision::node::NodeFlags;
    use lore_revision::relay::EventDispatcher;
    use lore_revision::repository;
    use lore_revision::repository::RepositoryContext;
    use lore_revision::repository::RepositoryWriteToken;
    use lore_revision::revision::sync;
    use lore_revision::stage;
    use lore_revision::stage::StageOptions;

    include!("helper.rs");

    /// The path that is a file in one revision and a directory in another.
    const ASSET: &str = "s0/asset.bin";

    const FILE_CONTENT: &[u8] = b"asset as one file";
    const FILE_EDITED: &[u8] = b"asset as one file, edited";
    const INNER_A: &[u8] = b"inner a";
    const INNER_B: &[u8] = b"inner b, one level down";
    const LOCAL_CONTENT: &[u8] = b"local work that no revision has";

    /// Offline execution context (no remote is consulted), optionally with the global
    /// `--force`.
    fn execution(force: bool) -> Arc<ExecutionContext> {
        Arc::new(ExecutionContext::new_client(
            LoreGlobalArgs {
                force: force.into(),
                ..Default::default()
            }
            .set_offline(),
            EventDispatcher::no_dispatch(),
        ))
    }

    /// Run a test body on the Lore runtime inside an execution context.
    async fn run<F>(body: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        runtime()
            .spawn(LORE_CONTEXT.scope(execution(false), body))
            .await
            .expect("Test task failed");
    }

    /// An initialized repository on disk, driven through the production stage, commit and sync
    /// primitives.
    struct Fixture {
        repository: Arc<RepositoryContext>,
        write_token: RepositoryWriteToken,
        root: PathBuf,
        _tempdir: TempDir,
    }

    impl Fixture {
        async fn new() -> Self {
            let tempdir = generate_tempdir();
            let root = tempdir.to_path_buf();
            Self::new_at(tempdir, root).await
        }

        /// A repository at `root`, a directory inside `tempdir`.
        async fn new_at(tempdir: TempDir, root: PathBuf) -> Self {
            std::fs::create_dir_all(&root).expect("Failed to create repository directory");
            let write_token = RepositoryWriteToken::acquire(root.as_path()).await;
            let repository = repository::create_local(
                root.as_path(),
                &write_token,
                RepositoryId::from(uuid::Uuid::now_v7()),
                BranchId::from(uuid::Uuid::now_v7()),
                branch::DEFAULT_DEFAULT_NAME.to_string(),
                repository::RepositoryConfig::default(),
                false,
            )
            .await
            .expect("Failed to initialize repository");

            Self {
                repository,
                write_token,
                root,
                _tempdir: tempdir,
            }
        }

        /// Write a `.loreignore` and load it into the repository context the way a
        /// repository open does.
        fn use_ignore_rules(&mut self, rules: &str) {
            self.write(".loreignore", rules.as_bytes());
            let filter = repository::load_filter(&self.root).expect("load filter");
            self.repository = Arc::new(
                self.repository
                    .to_filter_context(filter)
                    .with_write_token(self.write_token.share()),
            );
        }

        fn write(&self, relative: &str, content: &[u8]) {
            let absolute = self.root.join(relative);
            if let Some(parent) = absolute.parent() {
                std::fs::create_dir_all(parent).expect("Failed to create parent directory");
            }
            std::fs::write(absolute, content).expect("Failed to write file");
        }

        fn read(&self, relative: &str) -> Vec<u8> {
            std::fs::read(self.root.join(relative))
                .unwrap_or_else(|err| panic!("Failed to read {relative}: {err}"))
        }

        /// Remove a file or a whole directory.
        fn remove(&self, relative: &str) {
            let absolute = self.root.join(relative);
            if absolute.is_dir() {
                std::fs::remove_dir_all(absolute).expect("Failed to remove directory");
            } else {
                std::fs::remove_file(absolute).expect("Failed to remove file");
            }
        }

        fn is_file(&self, relative: &str) -> bool {
            self.root.join(relative).is_file()
        }

        fn is_dir(&self, relative: &str) -> bool {
            self.root.join(relative).is_dir()
        }

        /// Every file of the working tree with its content, and every directory, keyed by
        /// relative path: what "the working tree is unchanged" compares.
        fn snapshot(&self) -> BTreeMap<String, Option<Vec<u8>>> {
            fn walk(root: &Path, directory: &Path, out: &mut BTreeMap<String, Option<Vec<u8>>>) {
                for entry in std::fs::read_dir(directory).expect("Failed to list directory") {
                    let path = entry.expect("Failed to read directory entry").path();
                    let relative = path
                        .strip_prefix(root)
                        .expect("Entry outside the working tree")
                        .to_string_lossy()
                        .replace('\\', "/");
                    if relative == ".lore" {
                        continue;
                    }
                    if path.is_dir() {
                        out.insert(relative, None);
                        walk(root, &path, out);
                    } else {
                        out.insert(relative, Some(std::fs::read(&path).expect("read file")));
                    }
                }
            }
            let mut out = BTreeMap::new();
            walk(&self.root, &self.root, &mut out);
            out
        }

        /// Stage a path (the whole working tree for `""`) by scanning it.
        async fn stage(&self, relative: &str) {
            file::stage::stage(
                self.repository.clone(),
                &self.write_token,
                LoreArray::from_vec(vec![LoreString::from(&self.root.join(relative))]),
                StageOptions {
                    case_change: stage::StageCaseChange::Error,
                    node_flags: NodeFlags::NoFlags,
                    file_id: None,
                    no_children: false,
                    scan: true,
                },
            )
            .await
            .unwrap_or_else(|err| panic!("Failed to stage '{relative}': {err}"));
        }

        async fn commit(&self, message: &str) -> Hash {
            Box::pin(commit::commit(
                self.repository.clone(),
                &self.write_token,
                CommitOptions::new(message.to_string()),
            ))
            .await
            .expect("Failed to commit revision")
        }

        /// Stage the whole working tree and commit it.
        async fn commit_all(&self, message: &str) -> Hash {
            self.stage("").await;
            self.commit(message).await
        }

        /// Replace the asset by a directory holding two files, one of them nested, staged.
        ///
        /// The replaced path is staged by name: staging a parent of it does not pick up a
        /// file that became a directory.
        async fn stage_asset_as_directory(&self) {
            if self.root.join(ASSET).exists() {
                self.remove(ASSET);
            }
            self.write(&format!("{ASSET}/a.bin"), INNER_A);
            self.write(&format!("{ASSET}/sub/b.bin"), INNER_B);
            self.stage(ASSET).await;
        }

        fn assert_asset_is_directory(&self) {
            assert!(self.is_dir(ASSET), "{ASSET} must be a directory");
            assert_eq!(self.read(&format!("{ASSET}/a.bin")), INNER_A);
            assert_eq!(self.read(&format!("{ASSET}/sub/b.bin")), INNER_B);
        }

        fn assert_asset_is_file(&self, content: &[u8]) {
            assert!(self.is_file(ASSET), "{ASSET} must be a file");
            assert_eq!(self.read(ASSET), content);
        }

        /// Sync the working tree to a revision of the current branch.
        async fn sync_to(&self, revision: Hash, options: sync::SyncOptions) -> Result<(), String> {
            Box::pin(sync::sync(
                self.repository.clone(),
                &self.write_token,
                sync::SyncOptions {
                    revision: Some(revision.to_string()),
                    ..options
                },
            ))
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
        }

        /// The same sync under the global `--force`.
        async fn forced_sync_to(&self, revision: Hash) -> Result<(), String> {
            LORE_CONTEXT
                .scope(
                    execution(true),
                    self.sync_to(revision, sync::SyncOptions::default()),
                )
                .await
        }
    }

    /// A sync refused for local content fails with the local-modifications error (the path is
    /// reported through the log).
    fn assert_refused_for_local_content(result: Result<(), String>, what: &str) {
        let error = result.expect_err(what);
        assert!(
            error.contains("Local modifications prevent synchronization"),
            "{what}: unexpected error: {error}"
        );
    }

    /// Two revisions on the main branch: the asset as a file, then as a directory. A second
    /// file changes along, to show whether a refused sync changed anything else. The working
    /// tree is left at the directory revision.
    async fn file_then_directory(fixture: &Fixture) -> (Hash, Hash) {
        fixture.write("other.txt", b"other at the file revision");
        fixture.write(ASSET, FILE_CONTENT);
        let file_revision = fixture.commit_all("asset is a file").await;

        fixture.write("other.txt", b"other at the directory revision");
        fixture.stage_asset_as_directory().await;
        let directory_revision = fixture.commit_all("asset is a directory").await;

        (file_revision, directory_revision)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn tracked_directory_is_replaced_by_file_and_back() {
        run(async {
            let fixture = Fixture::new().await;
            let (file_revision, directory_revision) = file_then_directory(&fixture).await;
            fixture.assert_asset_is_directory();

            // Directory obstruction, everything in it tracked and unmodified: replaced
            fixture
                .sync_to(file_revision, sync::SyncOptions::default())
                .await
                .expect("a clean tracked directory must be replaced by the file");
            fixture.assert_asset_is_file(FILE_CONTENT);
            assert_eq!(fixture.read("other.txt"), b"other at the file revision");

            // File obstruction, tracked and unmodified: replaced
            fixture
                .sync_to(directory_revision, sync::SyncOptions::default())
                .await
                .expect("a clean tracked file must be replaced by the directory");
            fixture.assert_asset_is_directory();
            assert_eq!(
                fixture.read("other.txt"),
                b"other at the directory revision"
            );
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn directory_with_untracked_file_refuses_then_force_replaces() {
        run(async {
            let fixture = Fixture::new().await;
            let (file_revision, _) = file_then_directory(&fixture).await;
            fixture.write(&format!("{ASSET}/sub/local.txt"), LOCAL_CONTENT);
            let before = fixture.snapshot();

            assert_refused_for_local_content(
                fixture
                    .sync_to(file_revision, sync::SyncOptions::default())
                    .await,
                "an untracked file in the directory must refuse its replacement",
            );
            assert_eq!(
                fixture.snapshot(),
                before,
                "a refused sync must not change the working tree"
            );

            fixture
                .forced_sync_to(file_revision)
                .await
                .expect("a forced sync must replace the directory");
            fixture.assert_asset_is_file(FILE_CONTENT);
            assert_eq!(fixture.read("other.txt"), b"other at the file revision");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn directory_with_modified_file_refuses_before_any_change() {
        run(async {
            let fixture = Fixture::new().await;
            let (file_revision, _) = file_then_directory(&fixture).await;
            fixture.write(&format!("{ASSET}/a.bin"), LOCAL_CONTENT);
            let before = fixture.snapshot();

            assert_refused_for_local_content(
                fixture
                    .sync_to(file_revision, sync::SyncOptions::default())
                    .await,
                "a modified file in the directory must refuse its replacement",
            );
            assert_eq!(
                fixture.snapshot(),
                before,
                "a refused sync must not change the working tree"
            );
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn directory_with_ignored_file_refuses_then_reset_replaces() {
        run(async {
            let mut fixture = Fixture::new().await;
            let (file_revision, _) = file_then_directory(&fixture).await;

            fixture.use_ignore_rules("*.tmp\n");
            fixture.write(&format!("{ASSET}/cache.tmp"), LOCAL_CONTENT);
            let before = fixture.snapshot();

            assert_refused_for_local_content(
                fixture
                    .sync_to(
                        file_revision,
                        sync::SyncOptions {
                            filter_mode: FilterMode::Full,
                            ..Default::default()
                        },
                    )
                    .await,
                "an ignored file in the directory must refuse its replacement",
            );
            assert_eq!(
                fixture.snapshot(),
                before,
                "a refused sync must not change the working tree"
            );

            fixture
                .sync_to(
                    file_revision,
                    sync::SyncOptions {
                        reset: true,
                        filter_mode: FilterMode::Full,
                        ..Default::default()
                    },
                )
                .await
                .expect("a reset must replace the directory");
            fixture.assert_asset_is_file(FILE_CONTENT);
        })
        .await;
    }

    /// The workspace GRID#3302 was hit in: the tracked file was replaced on disk by a
    /// directory that no revision has, and the branch moved on with an edit of the file.
    #[tokio::test(flavor = "multi_thread")]
    async fn untracked_directory_over_tracked_file_refuses_then_reset_replaces() {
        run(async {
            let fixture = Fixture::new().await;
            fixture.write(ASSET, FILE_CONTENT);
            let file_revision = fixture.commit_all("asset is a file").await;
            fixture.write(ASSET, FILE_EDITED);
            let edited_revision = fixture.commit_all("asset edited").await;
            fixture
                .sync_to(file_revision, sync::SyncOptions::default())
                .await
                .expect("sync back to the first revision");

            fixture.remove(ASSET);
            fixture.write(&format!("{ASSET}/inner.bin"), LOCAL_CONTENT);
            let before = fixture.snapshot();

            assert_refused_for_local_content(
                fixture
                    .sync_to(edited_revision, sync::SyncOptions::default())
                    .await,
                "an untracked directory over the file must refuse the sync",
            );
            assert_eq!(
                fixture.snapshot(),
                before,
                "a refused sync must not change the working tree"
            );

            // The recovery: reset the working tree to the revision
            fixture
                .sync_to(
                    edited_revision,
                    sync::SyncOptions {
                        reset: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("a reset must replace the untracked directory");
            fixture.assert_asset_is_file(FILE_EDITED);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_directory_over_tracked_file_is_replaced() {
        run(async {
            let fixture = Fixture::new().await;
            fixture.write(ASSET, FILE_CONTENT);
            let file_revision = fixture.commit_all("asset is a file").await;
            fixture.write(ASSET, FILE_EDITED);
            let edited_revision = fixture.commit_all("asset edited").await;
            fixture
                .sync_to(file_revision, sync::SyncOptions::default())
                .await
                .expect("sync back to the first revision");

            // A directory inside is local content like a file is, and is refused up front
            fixture.remove(ASSET);
            std::fs::create_dir_all(fixture.root.join(ASSET).join("nested"))
                .expect("create nested directories");
            let before = fixture.snapshot();
            assert_refused_for_local_content(
                fixture
                    .sync_to(edited_revision, sync::SyncOptions::default())
                    .await,
                "a directory holding a directory is not replaced without force",
            );
            assert_eq!(
                fixture.snapshot(),
                before,
                "a refused sync must not change the working tree"
            );

            // Nothing is lost with an empty directory
            std::fs::remove_dir(fixture.root.join(ASSET).join("nested")).expect("remove nested");
            fixture
                .sync_to(edited_revision, sync::SyncOptions::default())
                .await
                .expect("an empty directory must be replaced by the file");
            fixture.assert_asset_is_file(FILE_EDITED);
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn untracked_file_where_directory_goes_refuses_then_force_replaces() {
        run(async {
            let fixture = Fixture::new().await;
            fixture.write("other.txt", b"other before the asset");
            let empty_revision = fixture.commit_all("no asset").await;
            fixture.write("other.txt", b"other with the asset");
            fixture.stage_asset_as_directory().await;
            let directory_revision = fixture.commit_all("asset is a directory").await;
            fixture
                .sync_to(empty_revision, sync::SyncOptions::default())
                .await
                .expect("sync back to the revision without the asset");
            assert!(!fixture.root.join(ASSET).exists());

            // File obstruction that no revision has
            fixture.write(ASSET, LOCAL_CONTENT);
            let before = fixture.snapshot();

            assert_refused_for_local_content(
                fixture
                    .sync_to(directory_revision, sync::SyncOptions::default())
                    .await,
                "an untracked file must refuse the directory created over it",
            );
            assert_eq!(
                fixture.snapshot(),
                before,
                "a refused sync must not change the working tree"
            );

            fixture
                .forced_sync_to(directory_revision)
                .await
                .expect("a forced sync must replace the file");
            fixture.assert_asset_is_directory();
            assert_eq!(fixture.read("other.txt"), b"other with the asset");
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn modified_file_where_directory_goes_refuses_before_any_change() {
        run(async {
            let fixture = Fixture::new().await;
            let (file_revision, directory_revision) = file_then_directory(&fixture).await;
            fixture
                .sync_to(file_revision, sync::SyncOptions::default())
                .await
                .expect("sync back to the file revision");

            fixture.write(ASSET, LOCAL_CONTENT);
            let before = fixture.snapshot();

            assert_refused_for_local_content(
                fixture
                    .sync_to(directory_revision, sync::SyncOptions::default())
                    .await,
                "a modified file must refuse the directory created over it",
            );
            assert_eq!(
                fixture.snapshot(),
                before,
                "a refused sync must not change the working tree"
            );

            fixture
                .forced_sync_to(directory_revision)
                .await
                .expect("a forced sync must replace the file");
            fixture.assert_asset_is_directory();
        })
        .await;
    }

    /// Forced replacement keeps the cross-VCS deletion protection: a directory holding a file
    /// that a git working tree covering the workspace tracks is never removed.
    #[tokio::test(flavor = "multi_thread")]
    async fn forced_replacement_never_removes_git_tracked_files() {
        let git = |root: &Path, args: &[&str]| {
            std::process::Command::new("git")
                .arg("-C")
                .arg(root)
                .args(args)
                .output()
                .is_ok_and(|output| output.status.success())
        };
        if !git(Path::new("."), &["--version"]) {
            eprintln!("skipping cross-VCS replacement test: git is not available");
            return;
        }

        run(async move {
            // The git working tree starts one level above the Lore workspace and tracks one
            // file inside what Lore will have as the asset directory. The repository context
            // snapshots git presence on first use, so git comes first.
            let tempdir = generate_tempdir();
            let root = tempdir.join("workspace");
            let source = format!("{ASSET}/tool.py");
            std::fs::create_dir_all(root.join(ASSET)).expect("create asset directory");
            std::fs::write(root.join(&source), b"print(1)\n").expect("write source");
            assert!(git(&tempdir, &["init", "--quiet"]));
            assert!(git(&tempdir, &["config", "user.email", "test@example.com"]));
            assert!(git(&tempdir, &["config", "user.name", "test"]));
            assert!(git(&root, &["add", &source]));
            assert!(git(&tempdir, &["commit", "--quiet", "-m", "source"]));
            std::fs::remove_dir_all(root.join(ASSET)).expect("remove asset directory");

            let fixture = Fixture::new_at(tempdir, root).await;
            fixture.write(ASSET, FILE_CONTENT);
            fixture.stage(ASSET).await;
            let file_revision = fixture.commit("asset is a file").await;
            fixture.stage_asset_as_directory().await;
            fixture.commit("asset is a directory").await;
            fixture.write(&source, b"print(1)\n");

            let error = fixture
                .forced_sync_to(file_revision)
                .await
                .expect_err("a directory holding a git-tracked file must not be replaced");
            assert!(
                error.contains("tracked by the git working tree"),
                "unexpected error: {error}"
            );
            assert_eq!(fixture.read(&source), b"print(1)\n");
        })
        .await;
    }
}
