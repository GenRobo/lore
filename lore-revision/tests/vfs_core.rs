// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![cfg(feature = "vfs")]

//! Kernel-free unit tests for the platform-neutral VFS core (`lore_revision::vfs::core`).
//! These build a real committed repository in an in-memory store and exercise
//! resolve/enumerate/read against it without mounting anything.

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::Arc;

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
    use lore_revision::repository::clone::VirtualLayer;
    use lore_revision::stage;
    use lore_revision::stage::StageOptions;
    use lore_revision::state::State;
    use lore_revision::util::path::RelativePath;
    use lore_revision::vfs::core;
    use lore_revision::vfs::core::EntryKind;
    use lore_transport::ProtocolError;

    include!("helper.rs");

    #[tokio::test]
    async fn resolve_enumerate_read_over_committed_tree() {
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        let repository_id = RepositoryId::from(uuid::Uuid::now_v7());

        #[allow(clippy::disallowed_methods)]
        runtime()
            .spawn(LORE_CONTEXT.scope(execution.clone(), async move {
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
                        immutable_store.clone(),
                        mutable_store.clone(),
                        repository_id,
                        created_repo.instance_id,
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

                // A top-level file and a file in a subdirectory.
                let readme = path.join("readme.md");
                {
                    let mut f = std::fs::File::create(&readme).expect("Create readme failed");
                    f.write_all(b"hello world").expect("Write failed");
                }
                let subdir = path.join("src");
                std::fs::create_dir_all(&subdir).expect("Create subdir failed");
                let file_path = subdir.join("test.txt");
                {
                    let mut f = std::fs::File::create(&file_path).expect("Create file failed");
                    f.write_all(b"hello").expect("Write failed");
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

                let layers = [VirtualLayer {
                    module: repository.clone(),
                    module_path: RelativePath::default(),
                    layer_path: RelativePath::default(),
                    state: state.clone(),
                }];

                // enumerate root: the top-level file and the subdirectory.
                let root = core::enumerate(&layers, "").await;
                let readme_entry = root
                    .iter()
                    .find(|e| e.name == "readme.md")
                    .expect("readme.md not enumerated at root");
                assert_eq!(readme_entry.kind, EntryKind::File);
                assert_eq!(readme_entry.size, "hello world".len() as u64);
                let src_entry = root
                    .iter()
                    .find(|e| e.name == "src")
                    .expect("src not enumerated at root");
                assert_eq!(src_entry.kind, EntryKind::Directory);

                // enumerate the subdirectory.
                let src = core::enumerate(&layers, "src").await;
                let test_entry = src
                    .iter()
                    .find(|e| e.name == "test.txt")
                    .expect("test.txt not enumerated under src");
                assert_eq!(test_entry.kind, EntryKind::File);
                assert_eq!(test_entry.size, 5);

                // resolve a nested file.
                let resolved = core::resolve(&layers, "src/test.txt")
                    .await
                    .expect("src/test.txt did not resolve");
                assert_eq!(resolved.kind(), EntryKind::File);
                assert_eq!(resolved.size(), 5);

                // read its content back.
                let mut buf = vec![0u8; 5];
                let read = core::read_range(&resolved, 0, &mut buf)
                    .await
                    .expect("read_range failed");
                assert_eq!(read, 5);
                assert_eq!(&buf, b"hello");

                // partial read from an offset.
                let mut buf2 = vec![0u8; 3];
                let read2 = core::read_range(&resolved, 2, &mut buf2)
                    .await
                    .expect("partial read_range failed");
                assert_eq!(read2, 3);
                assert_eq!(&buf2, b"llo");

                // read past EOF yields zero bytes.
                let mut buf3 = vec![0u8; 4];
                let read3 = core::read_range(&resolved, 5, &mut buf3)
                    .await
                    .expect("EOF read_range failed");
                assert_eq!(read3, 0);

                // a missing path resolves to None.
                assert!(
                    core::resolve(&layers, "does/not/exist").await.is_none(),
                    "nonexistent path should not resolve"
                );
            }))
            .await
            .expect("Test task failed");
    }
}
