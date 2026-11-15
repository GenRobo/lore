// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#[cfg(test)]
mod tests {
    #![allow(clippy::disallowed_methods)] // Test fixture writes; not subject to repository write-token discipline.

    use std::fs;
    use std::io::Write;
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::Arc;

    use lore_base::error::NoRemote;
    use lore_base::runtime::LORE_CONTEXT;
    use lore_base::runtime::runtime;
    use lore_base::types::Context;
    use lore_base::types::Hash;
    use lore_revision::branch;
    use lore_revision::commit;
    use lore_revision::commit::CommitOptions;
    use lore_revision::file;
    use lore_revision::file::dirty;
    use lore_revision::file::dirty::ExplicitDirty;
    use lore_revision::interface::ExecutionContext;
    use lore_revision::interface::LoreArray;
    use lore_revision::interface::LoreString;
    use lore_revision::lore::RepositoryId;
    use lore_revision::node::Node;
    use lore_revision::node::NodeFlags;
    use lore_revision::repository;
    use lore_revision::repository::RepositoryContext;
    use lore_revision::repository::RepositoryFormat;
    use lore_revision::repository::RepositoryWriteToken;
    use lore_revision::revision::sync;
    use lore_revision::revision::sync::SyncOptions;
    use lore_revision::stage;
    use lore_revision::stage::StageOptions;
    use lore_revision::state::State;
    use lore_revision::util::path::RelativePath;
    use lore_transport::ProtocolError;

    include!("helper.rs");

    #[tokio::test(flavor = "multi_thread")]
    async fn sync_explicit_revision() {
        let (_immutable_store, _mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        let repository_id = RepositoryId::from(uuid::Uuid::now_v7());
        let tempdir = generate_tempdir();
        let temp_path = tempdir.to_path_buf();
        let path = temp_path.clone();

        #[allow(clippy::disallowed_methods)]
        runtime()
            .spawn(LORE_CONTEXT.scope(execution.clone(), async move {
                std::fs::create_dir_all(path.as_path()).expect("Create directory failed");
                let write_token = repository::RepositoryWriteToken::acquire(path.as_path()).await;
                let repository = repository::create_local(
                    path.as_path(),
                    &write_token,
                    repository_id,
                    Context::from(uuid::Uuid::now_v7()),
                    branch::DEFAULT_DEFAULT_NAME.to_string(),
                    repository::RepositoryConfig::default(),
                    false,
                )
                .await
                .expect("Failed to create repository");

                let file_path = path.as_path().join("test.file");
                {
                    let mut file = std::fs::File::options()
                        .create(true)
                        .truncate(true)
                        .read(true)
                        .write(true)
                        .open(file_path.as_path())
                        .expect("Failed to create test file");
                    file.write_all(&[0, 1, 2, 3, 4])
                        .expect("Failed to write test file");
                }

                let _signature = file::stage::stage(
                    repository.clone(),
                    &write_token,
                    LoreArray::from_vec(vec![LoreString::from(&path)]),
                    StageOptions {
                        case_change: stage::StageCaseChange::Error,
                        node_flags: NodeFlags::NoFlags,
                        file_id: None,
                        no_children: false,
                        scan: true,
                    },
                )
                .await
                .expect("Failed to stage file");

                let options = CommitOptions {
                    message: String::new(),
                    link_messages: std::collections::HashMap::new(),
                    link: None,
                    layer_messages: std::collections::HashMap::new(),
                    layer: None,
                    stats: false,
                };
                let first_signature =
                    Box::pin(commit::commit(repository.clone(), &write_token, options))
                        .await
                        .expect("Failed to commit revision");

                let other_file_path = path.as_path().join("second.test.file");
                {
                    let mut file = std::fs::File::options()
                        .create(true)
                        .truncate(true)
                        .read(true)
                        .write(true)
                        .open(other_file_path.as_path())
                        .expect("Failed to create test file");
                    file.write_all(&[0, 1, 2, 3, 4])
                        .expect("Failed to write test file");
                }

                let _signature = file::stage::stage(
                    repository.clone(),
                    &write_token,
                    LoreArray::from_vec(vec![LoreString::from(&path)]),
                    StageOptions {
                        case_change: stage::StageCaseChange::Error,
                        node_flags: NodeFlags::NoFlags,
                        file_id: None,
                        no_children: false,
                        scan: true,
                    },
                )
                .await
                .expect("Failed to stage file");

                let options = CommitOptions {
                    message: String::new(),
                    link_messages: std::collections::HashMap::new(),
                    link: None,
                    layer_messages: std::collections::HashMap::new(),
                    layer: None,
                    stats: false,
                };
                let second_signature =
                    Box::pin(commit::commit(repository.clone(), &write_token, options))
                        .await
                        .expect("Failed to commit revision");

                // Sync back to first revision
                Box::pin(sync::sync(
                    repository.clone(),
                    &write_token,
                    SyncOptions {
                        revision: Some(first_signature.to_string()),
                        filter_mode: lore_revision::filter::FilterMode::Full,
                        ..Default::default()
                    },
                ))
                .await
                .expect("Failed to sync back to first revision");

                // Verify file added in first revision is still there
                assert!(
                    fs::metadata(file_path.as_path())
                        .expect("Failed to find first file as expected")
                        .is_file()
                );

                // Verify file added in second revision is gone
                fs::metadata(other_file_path.as_path()).expect_err(
                    "File added in second revision was not removed as expected after sync back",
                );

                // Sync forward to second revision
                Box::pin(sync::sync(
                    repository.clone(),
                    &write_token,
                    SyncOptions {
                        revision: Some(second_signature.to_string()),
                        filter_mode: lore_revision::filter::FilterMode::Full,
                        ..Default::default()
                    },
                ))
                .await
                .expect("Failed to sync forward to second revision");

                // Verify file added in first revision is still there
                assert!(
                    fs::metadata(file_path.as_path())
                        .expect("Failed to find first file as expected")
                        .is_file()
                );

                // Verify file added in second revision is restored
                assert!(
                    fs::metadata(other_file_path.as_path())
                        .expect("Failed to find first file as expected")
                        .is_file()
                );

                let _ = std::fs::remove_dir_all(path.as_path());
            }))
            .await
            .expect("Test task failed");

        let _ = std::fs::remove_dir_all(temp_path.as_path());
    }

    // ---------------------------------------------------------------------------------------
    // GRID#3295: a lazy workspace (bare repository + virtual mount) that is unmounted takes a
    // new revision in without materializing it. The tests below build the on-disk shape such
    // a workspace has after an unmount — a recorded mount binding and a backing overlay that
    // holds only the files written through the mount — and need no FUSE.
    // ---------------------------------------------------------------------------------------

    fn write_file(root: &Path, relative: &str, content: &[u8]) {
        let absolute = root.join(relative);
        fs::create_dir_all(absolute.parent().expect("parent")).expect("create parent");
        fs::write(absolute, content).expect("write file");
    }

    async fn stage_and_commit(
        repository: &Arc<RepositoryContext>,
        write_token: &RepositoryWriteToken,
        root: &Path,
        scan: bool,
        message: &str,
    ) -> Hash {
        file::stage::stage(
            repository.clone(),
            write_token,
            LoreArray::from_vec(vec![LoreString::from(&root.to_path_buf())]),
            StageOptions {
                case_change: stage::StageCaseChange::Error,
                node_flags: NodeFlags::NoFlags,
                file_id: None,
                no_children: false,
                scan,
            },
        )
        .await
        .expect("stage");
        Box::pin(commit::commit(
            repository.clone(),
            write_token,
            CommitOptions::new(message.to_string()),
        ))
        .await
        .expect("commit")
    }

    fn execution_with(globals: LoreGlobalArgs) -> Arc<ExecutionContext> {
        Arc::new(ExecutionContext::new_client_with_user_id(
            globals,
            lore_revision::relay::EventDispatcher::no_dispatch(),
            "test-user".to_string(),
        ))
    }

    /// A repository holding two revisions, and a lazy workspace standing at the first one with
    /// an empty overlay.
    ///
    /// First revision: `a.txt`, `b.txt`, `keep.txt`, `dir/c.txt`. The second one, unless the
    /// test builds its own, rewrites `a.txt` (same size), deletes `b.txt` and adds `new.txt`.
    struct Lazy {
        execution: Arc<ExecutionContext>,
        write_token: RepositoryWriteToken,
        /// The workspace as a repository open sees it while unmounted: metadata in the
        /// repository directory, working tree redirected to the backing overlay.
        repository: Arc<RepositoryContext>,
        /// The repository directory, where the metadata lives.
        root: PathBuf,
        overlay: PathBuf,
        first: Hash,
        second: Hash,
        _directories: [TempDir; 3],
    }

    impl Lazy {
        fn build() -> Self {
            Self::build_with(None, |root| {
                write_file(root, "a.txt", b"a-two");
                fs::remove_file(root.join("b.txt")).expect("delete b.txt");
                write_file(root, "new.txt", b"new");
            })
        }

        /// `loreignore` is written to the overlay root and loaded as the workspace's filter;
        /// `second` edits the eager tree the second revision is committed from.
        fn build_with(loreignore: Option<&str>, second: impl FnOnce(&Path)) -> Self {
            let (immutable, mutable, execution) = runtime()
                .block_on(test_store_create())
                .expect("Failed to create stores");
            let repository_id = RepositoryId::from(uuid::Uuid::now_v7());
            let directories = [generate_tempdir(), generate_tempdir(), generate_tempdir()];
            let root = directories[0].to_path_buf();
            let overlay = directories[1].to_path_buf();
            let mountpoint = directories[2].to_path_buf();

            let (repository, write_token, first, second) =
                runtime().block_on(LORE_CONTEXT.scope(execution.clone(), async {
                    let branch_id = Context::from(uuid::Uuid::now_v7());
                    let write_token = RepositoryWriteToken::acquire(&root).await;
                    let created = repository::create_local(
                        &root,
                        &write_token,
                        repository_id,
                        branch_id,
                        branch::DEFAULT_DEFAULT_NAME.to_string(),
                        repository::RepositoryConfig::default(),
                        false,
                    )
                    .await
                    .expect("Failed to create repository");

                    let context = |filter| {
                        RepositoryContext::new(
                            Some(root.clone()),
                            immutable.clone(),
                            mutable.clone(),
                            repository_id,
                            created.instance_id,
                            Err(ProtocolError::from(NoRemote)),
                            filter,
                            RepositoryFormat::Lore,
                        )
                        .with_write_token(write_token.share())
                    };

                    // The revisions are committed from an ordinary working tree in the
                    // repository directory, standing in for the peer that pushes them.
                    let eager = Arc::new(context(Arc::default()));
                    lore_revision::instance::store_current_anchor_branch(&eager, branch_id)
                        .await
                        .expect("Failed to store anchor branch");
                    // The branch itself: `create_local` wrote it to its own on-disk stores.
                    branch::create(
                        eager.clone(),
                        &write_token,
                        branch_id,
                        branch::DEFAULT_DEFAULT_NAME,
                        branch::default_category(),
                        "test-user",
                        0,
                        Vec::new(),
                        false,
                        false,
                    )
                    .await
                    .expect("Failed to create the branch");
                    write_file(&root, "a.txt", b"a-one");
                    write_file(&root, "b.txt", b"b-one");
                    write_file(&root, "keep.txt", b"keep");
                    write_file(&root, "dir/c.txt", b"c-one");
                    let first = stage_and_commit(&eager, &write_token, &root, true, "first").await;
                    second(&root);
                    let second =
                        stage_and_commit(&eager, &write_token, &root, true, "second").await;

                    // Stand the workspace at the first revision and bind it to an overlay the
                    // way `lore mount` records it; the mountpoint is not being served.
                    lore_revision::instance::store_current_anchor(&eager, first)
                        .await
                        .expect("Failed to rewind the current anchor");
                    repository::write_mount_binding(
                        &root.join(".lore"),
                        &mountpoint,
                        Some(&overlay),
                    )
                    .expect("Failed to record the mount binding");
                    let filter = match loreignore {
                        Some(content) => {
                            write_file(&overlay, ".loreignore", content.as_bytes());
                            repository::load_filter(&overlay).expect("load filter")
                        }
                        None => Arc::default(),
                    };
                    let lazy = Arc::new(context(filter).with_working_root(overlay.clone(), false));

                    (lazy, write_token, first, second)
                }));

            Self {
                execution,
                write_token,
                repository,
                root,
                overlay,
                first,
                second,
                _directories: directories,
            }
        }

        fn run<T>(&self, future: impl Future<Output = T>) -> T {
            runtime().block_on(LORE_CONTEXT.scope(self.execution.clone(), future))
        }

        fn sync(&self, options: SyncOptions) -> Result<(), String> {
            self.sync_as(self.execution.clone(), options)
        }

        fn sync_as(
            &self,
            execution: Arc<ExecutionContext>,
            options: SyncOptions,
        ) -> Result<(), String> {
            runtime().block_on(LORE_CONTEXT.scope(execution, async {
                Box::pin(sync::sync(
                    self.repository.clone(),
                    &self.write_token,
                    options,
                ))
                .await
                .map_err(|err| err.to_string())
            }))
        }

        /// `lore branch reset <revision>` on the checked-out branch.
        fn branch_reset_as(
            &self,
            execution: Arc<ExecutionContext>,
            revision: Hash,
        ) -> Result<(), String> {
            runtime().block_on(LORE_CONTEXT.scope(execution, async {
                Box::pin(branch::reset::reset(
                    self.repository.clone(),
                    &self.write_token,
                    String::new(),
                    revision.to_string(),
                ))
                .await
                .map_err(|err| err.to_string())
            }))
        }

        fn write(&self, relative: &str, content: &[u8]) {
            write_file(&self.overlay, relative, content);
        }

        fn read(&self, relative: &str) -> Option<Vec<u8>> {
            fs::read(self.overlay.join(relative)).ok()
        }

        /// Record a change the way the mount reports it while it is served.
        fn track(&self, relative: &str, action: ExplicitDirty) {
            let path = RelativePath::new_from_initial_path(relative).expect("relative path");
            self.run(dirty::dirty_explicit(
                self.repository.clone(),
                vec![path],
                action,
            ))
            .expect("track change");
        }

        fn current(&self) -> Hash {
            self.run(lore_revision::instance::load_current_anchor(
                &self.repository,
            ))
            .expect("current anchor")
            .0
        }

        /// The local latest revision of the checked-out branch.
        fn latest(&self) -> Hash {
            self.run(async {
                let (_, branch_id) = lore_revision::instance::load_current_anchor(&self.repository)
                    .await
                    .expect("current anchor");
                branch::load_latest(self.repository.clone(), branch_id)
                    .await
                    .expect("branch latest")
            })
        }

        fn staged(&self) -> Option<Hash> {
            self.run(lore_revision::instance::load_staged_revision(
                &self.repository,
            ))
            .expect("staged anchor")
        }

        /// The node a revision (or staged state) holds at a path, if any.
        fn node(&self, revision: Hash, relative: &str) -> Option<Node> {
            self.run(async {
                let state = State::deserialize(self.repository.clone(), revision)
                    .await
                    .expect("deserialize state");
                match state.find_node(self.repository.clone(), relative).await {
                    Ok(node) => Some(node),
                    Err(err) if err.is_node_not_found() => None,
                    Err(err) => panic!("lookup of {relative} failed: {err}"),
                }
            })
        }

        /// Stage the tracked changes (no scan) and commit them, as `lore stage .` and
        /// `lore commit` do on the unmounted workspace.
        fn stage_and_commit(&self, message: &str) -> Hash {
            self.run(stage_and_commit(
                &self.repository,
                &self.write_token,
                &self.overlay,
                false,
                message,
            ))
        }
    }

    // Incoming files are never written into the overlay; an unmodified overlay copy of a file
    // the new revision replaces or deletes goes away so the next mount projects the new
    // revision; an unmodified copy of an untouched file is left alone.
    #[test]
    fn lazy_sync_drops_stale_copies_and_materializes_nothing() {
        let lazy = Lazy::build();
        lazy.write("a.txt", b"a-one");
        lazy.write("b.txt", b"b-one");
        lazy.write("keep.txt", b"keep");
        lazy.write("dir/c.txt", b"c-one");

        lazy.sync(SyncOptions::default())
            .expect("a lazy workspace with only clean overlay copies must sync");

        assert_eq!(lazy.current(), lazy.second, "the anchor must advance");
        assert_eq!(
            lazy.read("a.txt"),
            None,
            "a stale copy must not mask the incoming a.txt, and the incoming a.txt must not be \
             materialized"
        );
        assert_eq!(lazy.read("b.txt"), None, "a copy of a deleted file must go");
        assert_eq!(
            lazy.read("new.txt"),
            None,
            "an incoming file must stay absent"
        );
        assert_eq!(lazy.read("keep.txt").as_deref(), Some(&b"keep"[..]));
        assert_eq!(lazy.read("dir/c.txt").as_deref(), Some(&b"c-one"[..]));
        assert_eq!(lazy.staged(), None, "nothing was tracked, nothing is now");
    }

    // A local edit and a local addition at paths the new revision does not touch survive the
    // sync, stay tracked, and commit on top of the new revision.
    #[test]
    fn lazy_sync_keeps_unrelated_changes_and_they_still_commit() {
        let lazy = Lazy::build();
        lazy.write("keep.txt", b"keep, edited");
        lazy.track("keep.txt", ExplicitDirty::Modify);
        lazy.write("added/mine.txt", b"mine");
        lazy.track("added/mine.txt", ExplicitDirty::Add);
        lazy.write("a.txt", b"a-one");

        lazy.sync(SyncOptions::default())
            .expect("changes at untouched paths must not block the sync");

        assert_eq!(lazy.current(), lazy.second);
        assert_eq!(lazy.read("keep.txt").as_deref(), Some(&b"keep, edited"[..]));
        assert_eq!(lazy.read("added/mine.txt").as_deref(), Some(&b"mine"[..]));
        assert_eq!(lazy.read("a.txt"), None);

        let third = lazy.stage_and_commit("local changes");
        assert_eq!(
            lazy.node(third, "keep.txt").expect("keep.txt").size,
            b"keep, edited".len() as u64,
            "the edit must be committed"
        );
        assert!(lazy.node(third, "added/mine.txt").is_some());
        assert_eq!(
            lazy.node(third, "a.txt").expect("a.txt").address.hash,
            lazy.node(lazy.second, "a.txt").expect("a.txt").address.hash,
            "the commit must sit on the incoming a.txt"
        );
        assert!(lazy.node(third, "b.txt").is_none());
        assert!(lazy.node(third, "new.txt").is_some());
        assert!(lazy.node(third, "dir/c.txt").is_some());
    }

    // A local change at a path the new revision also touches fails the sync, names the path,
    // and changes nothing: not the conflicting file, not the stale copy beside it, not the
    // anchors.
    #[test]
    fn lazy_sync_refuses_overlapping_changes_without_mutation() {
        // Edited locally, rewritten upstream.
        let lazy = Lazy::build();
        lazy.write("a.txt", b"a-mine");
        lazy.track("a.txt", ExplicitDirty::Modify);
        lazy.write("b.txt", b"b-one");
        let staged = lazy.staged();
        let error = lazy
            .sync(SyncOptions::default())
            .expect_err("an edit of a file the new revision rewrites must be refused");
        assert!(
            error.contains("a.txt") && error.contains("conflict"),
            "the refusal must name the path: {error}"
        );
        assert_eq!(lazy.read("a.txt").as_deref(), Some(&b"a-mine"[..]));
        assert_eq!(
            lazy.read("b.txt").as_deref(),
            Some(&b"b-one"[..]),
            "no overlay entry may be removed by a refused sync"
        );
        assert_eq!(lazy.current(), lazy.first, "the anchor must not move");
        assert_eq!(lazy.staged(), staged, "the tracking must not change");

        // The same holds without any tracking, and for an edit that leaves the size as it
        // was: the overlay content decides, not the size or the recorded tracking.
        let lazy = Lazy::build();
        lazy.write("a.txt", b"a-me!");
        let error = lazy
            .sync(SyncOptions::default())
            .expect_err("an untracked, same-size edit must be refused as well");
        assert!(error.contains("a.txt"), "{error}");
        assert_eq!(lazy.read("a.txt").as_deref(), Some(&b"a-me!"[..]));

        // Edited locally, deleted upstream.
        let lazy = Lazy::build();
        lazy.write("b.txt", b"b-me!");
        let error = lazy
            .sync(SyncOptions::default())
            .expect_err("an edit of a file the new revision deletes must be refused");
        assert!(error.contains("b.txt"), "{error}");
        assert_eq!(lazy.read("b.txt").as_deref(), Some(&b"b-me!"[..]));

        // Added locally and upstream, with different content.
        let lazy = Lazy::build();
        lazy.write("new.txt", b"mine");
        lazy.track("new.txt", ExplicitDirty::Add);
        let error = lazy
            .sync(SyncOptions::default())
            .expect_err("an addition that collides with an incoming file must be refused");
        assert!(error.contains("new.txt"), "{error}");

        // Deleted locally, rewritten upstream.
        let lazy = Lazy::build();
        lazy.track("a.txt", ExplicitDirty::Delete);
        let error = lazy
            .sync(SyncOptions::default())
            .expect_err("a deletion of a file the new revision rewrites must be refused");
        assert!(
            error.contains("a.txt") && error.contains("deleted locally"),
            "{error}"
        );
        assert_eq!(lazy.current(), lazy.first);

        // A file added where the new revision has a directory.
        let lazy = Lazy::build_with(None, |root| write_file(root, "tree/leaf.txt", b"leaf"));
        lazy.write("tree", b"a file");
        let error = lazy
            .sync(SyncOptions::default())
            .expect_err("a file where the new revision has a directory must be refused");
        assert!(error.contains("tree"), "{error}");

        // A directory deleted locally, with a file rewritten beneath it upstream.
        let lazy = Lazy::build_with(None, |root| write_file(root, "dir/c.txt", b"c-two"));
        lazy.track("dir", ExplicitDirty::Delete);
        let error = lazy
            .sync(SyncOptions::default())
            .expect_err("deleting a directory the new revision changes inside must be refused");
        assert!(
            error.contains("dir (deleted locally"),
            "the refusal must name the directory: {error}"
        );
        assert_eq!(lazy.current(), lazy.first);

        // A file written where both revisions have a directory: fine while the new revision
        // leaves that directory alone, a collision once it changes something inside it.
        let lazy = Lazy::build();
        lazy.write("dir", b"now a file");
        lazy.sync(SyncOptions::default())
            .expect("a local file over a directory nothing changes in is a local change");
        assert_eq!(lazy.read("dir").as_deref(), Some(&b"now a file"[..]));

        let lazy = Lazy::build_with(None, |root| write_file(root, "dir/c.txt", b"c-two"));
        lazy.write("dir", b"now a file");
        let error = lazy
            .sync(SyncOptions::default())
            .expect_err("a local file over a directory the new revision changes must be refused");
        assert!(
            error.contains("dir (local file replaces a directory"),
            "the refusal must name the path: {error}"
        );
        assert_eq!(lazy.read("dir").as_deref(), Some(&b"now a file"[..]));
        assert_eq!(lazy.current(), lazy.first);
    }

    // An overlay file that already holds the incoming content is not a conflict (as in an
    // eager sync), `--force` gives the incoming revision precedence, and `--forward-changes`
    // the local change.
    #[test]
    fn lazy_sync_resolves_overlaps_when_told_how() {
        let lazy = Lazy::build();
        lazy.write("a.txt", b"a-two");
        lazy.track("a.txt", ExplicitDirty::Modify);
        lazy.sync(SyncOptions::default())
            .expect("content identical to the incoming file must be accepted");
        assert_eq!(lazy.current(), lazy.second);
        assert_eq!(lazy.read("a.txt").as_deref(), Some(&b"a-two"[..]));
        assert_eq!(lazy.staged(), None, "a.txt is no longer a local change");

        let lazy = Lazy::build();
        lazy.write("a.txt", b"a-mine");
        lazy.track("a.txt", ExplicitDirty::Modify);
        lazy.write("keep.txt", b"keep, edited");
        lazy.track("keep.txt", ExplicitDirty::Modify);
        let forced = execution_with(LoreGlobalArgs {
            force: 1,
            ..Default::default()
        });
        lazy.sync_as(forced, SyncOptions::default())
            .expect("--force must give the incoming revision precedence");
        assert_eq!(lazy.current(), lazy.second);
        assert_eq!(
            lazy.read("a.txt"),
            None,
            "the conflicting edit must give way"
        );
        assert_eq!(
            lazy.read("keep.txt").as_deref(),
            Some(&b"keep, edited"[..]),
            "--force only overrides what the new revision touches"
        );
        let third = lazy.stage_and_commit("after force");
        assert_eq!(
            lazy.node(third, "a.txt").expect("a.txt").address.hash,
            lazy.node(lazy.second, "a.txt").expect("a.txt").address.hash
        );

        let lazy = Lazy::build();
        lazy.write("a.txt", b"a-mine");
        lazy.track("a.txt", ExplicitDirty::Modify);
        lazy.sync(SyncOptions {
            forward_changes: true,
            ..Default::default()
        })
        .expect("--forward-changes must keep the local change");
        assert_eq!(lazy.current(), lazy.second);
        assert_eq!(lazy.read("a.txt").as_deref(), Some(&b"a-mine"[..]));
        let third = lazy.stage_and_commit("mine over theirs");
        assert_eq!(lazy.node(third, "a.txt").expect("a.txt").size, 6);
    }

    // `sync --reset` discards every workspace entry of the overlay and all tracking — edits,
    // additions and deletions — also when the workspace already stands at the target revision,
    // and leaves alone what the ignore filter excludes.
    #[test]
    fn lazy_sync_reset_discards_edits_and_deletions() {
        let reset = || SyncOptions {
            reset: true,
            ..Default::default()
        };

        let lazy = Lazy::build_with(Some(".loreignore\nforeign/\n"), |root| {
            write_file(root, "a.txt", b"a-two");
        });
        lazy.write("a.txt", b"a-mine");
        lazy.track("a.txt", ExplicitDirty::Modify);
        lazy.write("added/mine.txt", b"mine");
        lazy.track("added/mine.txt", ExplicitDirty::Add);
        lazy.track("keep.txt", ExplicitDirty::Delete);
        lazy.write("foreign/tool.cfg", b"not lore's");

        lazy.sync(reset())
            .expect("reset must discard the local changes");
        assert_eq!(lazy.current(), lazy.second);
        assert_eq!(lazy.read("a.txt"), None);
        assert_eq!(lazy.read("added/mine.txt"), None);
        assert!(
            !lazy.overlay.join("added").exists(),
            "emptied directories must go"
        );
        assert_eq!(
            lazy.staged(),
            None,
            "no edit and no deletion may stay tracked"
        );
        assert_eq!(
            lazy.read("foreign/tool.cfg").as_deref(),
            Some(&b"not lore's"[..]),
            "ignored content is not the workspace's to delete"
        );
        assert!(lazy.overlay.join(".loreignore").exists());

        // Same revision: nothing to take in, everything local to discard.
        lazy.write("keep.txt", b"keep, edited");
        lazy.track("keep.txt", ExplicitDirty::Modify);
        lazy.track("dir/c.txt", ExplicitDirty::Delete);
        assert!(lazy.staged().is_some());
        lazy.sync(reset())
            .expect("reset at the target revision must still discard");
        assert_eq!(lazy.current(), lazy.second);
        assert_eq!(lazy.read("keep.txt"), None);
        assert_eq!(lazy.staged(), None);
        assert!(lazy.overlay.join("foreign/tool.cfg").exists());
    }

    // A deletion made through the mount carries over when the new revision leaves the path
    // alone; tracking that no longer has anything behind it does not turn into a deletion.
    #[test]
    fn lazy_sync_carries_deletions_and_drops_stale_tracking() {
        let lazy = Lazy::build();
        // Deleted locally, untouched upstream: stays deleted.
        lazy.track("keep.txt", ExplicitDirty::Delete);
        // Deleted locally and upstream: nothing left to track.
        lazy.track("b.txt", ExplicitDirty::Delete);
        // A directory deleted locally, nothing changed beneath it upstream: stays deleted.
        lazy.track("dir", ExplicitDirty::Delete);
        // Written through the mount with unchanged content, rewritten upstream: the copy is
        // removed, and its tracking must not become a deletion of a.txt.
        lazy.write("a.txt", b"a-one");
        lazy.track("a.txt", ExplicitDirty::Modify);

        lazy.sync(SyncOptions::default())
            .expect("none of these overlap an incoming change");
        assert_eq!(lazy.current(), lazy.second);
        assert_eq!(lazy.read("a.txt"), None);

        let staged = lazy
            .staged()
            .expect("the deletion of keep.txt stays tracked");
        assert!(
            lazy.node(staged, "keep.txt")
                .expect("keep.txt")
                .is_dirty_delete()
        );
        assert!(
            !lazy.node(staged, "a.txt").expect("a.txt").is_dirty(),
            "a.txt must not stay tracked once its overlay copy is gone"
        );
        assert!(
            lazy.node(staged, "dir/c.txt")
                .expect("dir/c.txt")
                .is_dirty_delete()
        );

        let third = lazy.stage_and_commit("delete keep.txt");
        assert!(lazy.node(third, "keep.txt").is_none());
        assert_eq!(
            lazy.node(third, "a.txt")
                .expect("a.txt must survive")
                .address
                .hash,
            lazy.node(lazy.second, "a.txt").expect("a.txt").address.hash
        );
        assert!(lazy.node(third, "new.txt").is_some());
        assert!(lazy.node(third, "dir/c.txt").is_none());
    }

    // When the overlay copy cannot be compared with the old revision the sync fails; it must
    // not treat the copy as unmodified and delete it.
    #[test]
    #[cfg(target_family = "unix")]
    fn lazy_sync_keeps_a_file_it_cannot_compare() {
        use std::os::unix::fs::PermissionsExt;

        let lazy = Lazy::build();
        // Same size as the committed a.txt, so only its content can tell.
        lazy.write("a.txt", b"a-???");
        let unreadable = lazy.overlay.join("a.txt");
        fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o000)).expect("chmod");
        if fs::read(&unreadable).is_ok() {
            eprintln!("skipping: permissions do not stop this user from reading (root?)");
            return;
        }

        let error = lazy
            .sync(SyncOptions::default())
            .expect_err("a failed comparison must fail the sync");
        assert!(error.contains("compare"), "{error}");
        assert!(
            unreadable.exists(),
            "the uncompared file must not be deleted"
        );
        assert_eq!(lazy.current(), lazy.first, "the anchor must not move");

        fs::set_permissions(&unreadable, fs::Permissions::from_mode(0o644)).expect("chmod");
    }

    // The new revision replaces a directory with a file: unmodified overlay copies beneath it
    // go, and so does the emptied overlay directory, which would otherwise mask the file. A
    // local file beneath it is a conflict.
    #[test]
    fn lazy_sync_lets_an_incoming_file_replace_a_directory() {
        let replace_dir = |root: &Path| {
            fs::remove_dir_all(root.join("dir")).expect("remove dir");
            write_file(root, "dir", b"now a file");
        };

        let lazy = Lazy::build_with(None, replace_dir);
        lazy.write("dir/c.txt", b"c-one");
        lazy.sync(SyncOptions::default())
            .expect("a clean copy beneath a replaced directory must not block the sync");
        assert_eq!(lazy.current(), lazy.second);
        assert!(
            fs::symlink_metadata(lazy.overlay.join("dir")).is_err(),
            "the overlay directory must not mask the incoming file"
        );

        let lazy = Lazy::build_with(None, replace_dir);
        lazy.write("dir/c.txt", b"c-one");
        lazy.write("dir/mine.txt", b"mine");
        lazy.track("dir/mine.txt", ExplicitDirty::Add);
        let error = lazy
            .sync(SyncOptions::default())
            .expect_err("a local file beneath an incoming file must be refused");
        assert!(error.contains("dir/mine.txt"), "{error}");
        assert_eq!(lazy.read("dir/c.txt").as_deref(), Some(&b"c-one"[..]));
        assert_eq!(lazy.current(), lazy.first);
    }

    // What cannot be done without realizing content is refused before anything changes: a
    // layer is synchronized by writing its files, and a dependency-selective sync has nothing
    // to select in a workspace where no incoming file is written.
    #[test]
    fn lazy_sync_refuses_what_it_would_have_to_materialize() {
        #[derive(serde::Serialize)]
        struct LayerConfig {
            layers: Vec<lore_revision::layer::Layer>,
        }

        let lazy = Lazy::build();
        lazy.write("a.txt", b"a-one");

        let error = lazy
            .sync(SyncOptions {
                root_files: vec!["a.txt".to_string()],
                ..Default::default()
            })
            .expect_err("a dependency-selective sync must be refused");
        assert!(error.contains("--root-file"), "{error}");

        let config = LayerConfig {
            layers: vec![lore_revision::layer::Layer {
                target_path: "layered".to_string(),
                repository: RepositoryId::from(uuid::Uuid::now_v7()),
                ..Default::default()
            }],
        };
        fs::write(
            lore_revision::layer::layer_config_path(&lazy.root),
            toml::to_string_pretty(&config).expect("serialize the layer configuration"),
        )
        .expect("write the layer configuration");
        let error = lazy
            .sync(SyncOptions::default())
            .expect_err("a lazy workspace with layers must be refused");
        assert!(error.contains("layers"), "{error}");
        let error = lazy
            .sync(SyncOptions {
                reset: true,
                ..Default::default()
            })
            .expect_err("also with --reset");
        assert!(error.contains("layers"), "{error}");

        assert_eq!(lazy.current(), lazy.first, "the anchor must not move");
        assert_eq!(
            lazy.read("a.txt").as_deref(),
            Some(&b"a-one"[..]),
            "the overlay must not change"
        );
    }

    // `--dry-run` validates and changes nothing.
    #[test]
    fn lazy_sync_dry_run_changes_nothing() {
        let dry_run = || {
            execution_with(LoreGlobalArgs {
                dry_run: 1,
                ..Default::default()
            })
        };

        let lazy = Lazy::build();
        lazy.write("a.txt", b"a-one");
        lazy.write("keep.txt", b"keep, edited");
        lazy.track("keep.txt", ExplicitDirty::Modify);
        let staged = lazy.staged();
        lazy.sync_as(dry_run(), SyncOptions::default())
            .expect("dry run of a clean transition");
        lazy.sync_as(
            dry_run(),
            SyncOptions {
                reset: true,
                ..Default::default()
            },
        )
        .expect("dry run of a reset");
        assert_eq!(lazy.read("a.txt").as_deref(), Some(&b"a-one"[..]));
        assert_eq!(lazy.read("keep.txt").as_deref(), Some(&b"keep, edited"[..]));
        assert_eq!(lazy.current(), lazy.first);
        assert_eq!(lazy.staged(), staged);

        // And it still reports a conflict.
        lazy.write("a.txt", b"a-mine");
        lazy.sync_as(dry_run(), SyncOptions::default())
            .expect_err("a dry run must report the conflict a real sync would hit");
    }

    // `branch reset <revision>` on the checked-out branch takes the same transition, and its
    // dry run moves neither the overlay nor the anchors.
    #[test]
    fn lazy_branch_reset_reconciles_the_overlay() {
        let lazy = Lazy::build();
        lazy.sync(SyncOptions::default())
            .expect("take the second revision in");
        // An unmodified copy of the second revision's a.txt, and an unrelated edit.
        lazy.write("a.txt", b"a-two");
        lazy.write("keep.txt", b"keep, edited");
        lazy.track("keep.txt", ExplicitDirty::Modify);

        let dry_run = execution_with(LoreGlobalArgs {
            dry_run: 1,
            ..Default::default()
        });
        lazy.branch_reset_as(dry_run, lazy.first)
            .expect("dry run of a branch reset");
        assert_eq!(
            lazy.current(),
            lazy.second,
            "a dry run must not move the anchor"
        );
        assert_eq!(
            lazy.latest(),
            lazy.second,
            "a dry run must not move the branch"
        );
        assert_eq!(lazy.read("a.txt").as_deref(), Some(&b"a-two"[..]));

        lazy.branch_reset_as(lazy.execution.clone(), lazy.first)
            .expect("branch reset back to the first revision");
        assert_eq!(lazy.current(), lazy.first);
        assert_eq!(lazy.latest(), lazy.first);
        assert_eq!(
            lazy.read("a.txt"),
            None,
            "a copy of the second revision's a.txt must not mask the first revision's"
        );
        assert_eq!(lazy.read("keep.txt").as_deref(), Some(&b"keep, edited"[..]));
        let staged = lazy.staged().expect("the edit stays tracked");
        assert!(lazy.node(staged, "keep.txt").expect("keep.txt").is_dirty());
    }
}
