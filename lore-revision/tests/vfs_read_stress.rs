// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#![cfg(feature = "vfs")]

//! Stress and integrity tests for the lazy read path (`lore_revision::vfs::core::read_range`).
//!
//! A repository is committed into an "origin" store: an initial revision, then several edits that
//! each rewrite 64 KiB blocks in place, so a file's content is assembled from fragments stored by
//! more than one revision. Readers use a second repository context whose local store starts
//! empty and whose remote is a test double serving the origin store through the transport's
//! `Storage` trait. Every read therefore starts cold and takes the real path (fetch, hash check,
//! local cache, assembly) with no server and, except for the mount test, no kernel.
//!
//! The double can tamper with what it serves, which is how the tests prove that a fetch that
//! does not match its address fails the read instead of producing bytes.

#[cfg(test)]
mod tests {
    #![allow(clippy::disallowed_methods)] // Test fixture writes; not subject to repository write-token discipline.

    use std::collections::HashSet;
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::Weak;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use lore_base::error::Disconnected;
    use lore_base::error::NoRemote;
    use lore_base::error::NotFound;
    use lore_base::runtime::LORE_CONTEXT;
    use lore_base::runtime::runtime;
    use lore_base::types::Address;
    use lore_base::types::Context;
    use lore_base::types::Fragment;
    use lore_base::types::FragmentReference;
    use lore_base::types::Hash;
    use lore_base::types::VerifyResult;
    use lore_revision::branch;
    use lore_revision::commit;
    use lore_revision::commit::CommitOptions;
    use lore_revision::file;
    use lore_revision::immutable;
    use lore_revision::instance::InstanceId;
    use lore_revision::interface::ExecutionContext;
    use lore_revision::interface::LoreArray;
    use lore_revision::interface::LoreString;
    use lore_revision::lore::RepositoryId;
    use lore_revision::node::Node;
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
    use lore_revision::vfs::core::ResolvedNode;
    use lore_storage::FragmentFlags;
    use lore_storage::ImmutableStore;
    use lore_storage::ReadOptions;
    use lore_storage::store_types::StoreMatch;
    use lore_transport::Admin;
    use lore_transport::Connection;
    use lore_transport::Environment;
    use lore_transport::EnvironmentConfig;
    use lore_transport::Lock;
    use lore_transport::Protocol;
    use lore_transport::ProtocolError;
    use lore_transport::Repository;
    use lore_transport::Revision;
    use lore_transport::Storage;
    use parking_lot::Mutex;

    include!("helper.rs");

    /// Edits rewrite whole blocks of this size, like the workload that showed GRID#3299.
    const BLOCK: usize = 64 * 1024;
    /// The kernel's default readahead window, and so the size of most FUSE reads.
    const WINDOW: usize = 128 * 1024;
    /// Revisions committed after the initial one.
    const EDITS: u64 = 4;
    /// Threads reading at once, as many as the FUSE backend runs workers.
    const READERS: usize = 16;

    /// How many times the stress test starts over from an empty local store. Raise it to loop:
    /// `LORE_VFS_READ_STRESS_ITERATIONS=300 cargo test --features vfs --test vfs_read_stress`.
    fn stress_iterations() -> usize {
        std::env::var("LORE_VFS_READ_STRESS_ITERATIONS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(2)
    }

    /// One step of the splitmix64 sequence.
    fn mix(state: &mut u64) -> u64 {
        *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = *state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Incompressible bytes that depend only on `key`.
    fn fill(key: u64, buf: &mut [u8]) {
        let mut state = key;
        for chunk in buf.chunks_mut(8) {
            let value = mix(&mut state).to_le_bytes();
            chunk.copy_from_slice(&value[..chunk.len()]);
        }
    }

    /// Key of block `block` of file `file` as written by edit `edit` (0 is the initial revision).
    fn block_key(file: usize, block: usize, edit: u64) -> u64 {
        ((file as u64) << 40) | ((block as u64) << 16) | edit
    }

    /// Compressible bytes that depend only on `key`: numbered lines of text.
    fn fill_text(key: u64, buf: &mut [u8]) {
        let mut written = 0;
        let mut line = 0u32;
        while written < buf.len() {
            let text = format!("{key:016x} {line:08} the reads below must return these lines\n");
            let take = text.len().min(buf.len() - written);
            buf[written..written + take].copy_from_slice(&text.as_bytes()[..take]);
            written += take;
            line += 1;
        }
    }

    /// Whether `block` of file `file` lies in the run of blocks that a.bin (file 0, blocks
    /// 8..16) and b.bin (file 1, blocks 20..28) have in common. Edits leave the run alone.
    fn in_shared_run(file: usize, block: usize) -> bool {
        (file == 0 && (8..16).contains(&block)) || (file == 1 && (20..28).contains(&block))
    }

    /// A committed repository and the bytes each of its files holds at the last revision.
    struct Origin {
        repository: Arc<RepositoryContext>,
        store: Arc<dyn ImmutableStore>,
        repository_id: RepositoryId,
        instance_id: InstanceId,
        /// The first revision, before any edit.
        initial: Hash,
        /// The last revision, which readers are on.
        revision: Hash,
        files: Vec<(String, Vec<u8>)>,
        execution: Arc<ExecutionContext>,
        _write_token: repository::RepositoryWriteToken,
        _tempdir: TempDir,
    }

    impl Origin {
        fn block_on<F: Future>(&self, future: F) -> F::Output {
            runtime().block_on(LORE_CONTEXT.scope(self.execution.clone(), future))
        }

        fn file(&self, name: &str) -> &[u8] {
            &self
                .files
                .iter()
                .find(|(file, _)| file == name)
                .expect("file is in the fixture")
                .1
        }
    }

    async fn stage_and_commit(
        repository: &Arc<RepositoryContext>,
        token: &repository::RepositoryWriteToken,
        path: &std::path::Path,
        message: &str,
    ) -> Hash {
        file::stage::stage(
            repository.clone(),
            token,
            LoreArray::from_vec(vec![LoreString::from(path)]),
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
            token,
            CommitOptions::new(message.to_string()),
        ))
        .await
        .expect("Commit failed")
    }

    /// Commit the fixture: files of several sizes, two of them sharing a 512 KiB run of blocks
    /// (the same fragments under two file contexts) and two holding text (stored compressed),
    /// then `EDITS` revisions that each rewrite one to three 64 KiB blocks of every multi-block
    /// file.
    fn build_origin() -> Origin {
        let (store, mutable_store, execution) = runtime()
            .block_on(test_store_create())
            .expect("Failed to create stores");
        let repository_id = RepositoryId::from(uuid::Uuid::now_v7());
        let tempdir = generate_tempdir();
        let path = tempdir.to_path_buf();

        // (name, whole blocks, extra tail bytes, text)
        let layout: [(&str, usize, usize, bool); 7] = [
            ("a.bin", 32, 0, false),
            ("b.bin", 32, 12_345, false),
            ("assets/c.bin", 80, 0, false),
            ("assets/small.bin", 1, 30_000, false),
            ("logs/note.txt", 0, 40_000, true),
            ("logs/run.log", 12, 4_321, true),
            ("tiny.txt", 0, 5, false),
        ];
        let mut files: Vec<(String, Vec<u8>)> = layout
            .iter()
            .enumerate()
            .map(|(index, &(name, blocks, tail, text))| {
                let mut bytes = vec![0u8; blocks * BLOCK + tail];
                for (block, chunk) in bytes.chunks_mut(BLOCK).enumerate() {
                    let key = block_key(index, block, 0);
                    if text {
                        fill_text(key, chunk);
                    } else {
                        fill(key, chunk);
                    }
                }
                (name.to_string(), bytes)
            })
            .collect();
        // The shared run: blocks 8..16 of a.bin are blocks 20..28 of b.bin.
        let shared = files[0].1[8 * BLOCK..16 * BLOCK].to_vec();
        files[1].1[20 * BLOCK..28 * BLOCK].copy_from_slice(&shared);

        let write_files = |files: &[(String, Vec<u8>)]| {
            for (name, bytes) in files {
                let target = path.join(name);
                std::fs::create_dir_all(target.parent().expect("file has a parent"))
                    .expect("Create directory failed");
                let mut file = std::fs::File::create(&target).expect("Create file failed");
                file.write_all(bytes).expect("Write failed");
            }
        };

        let (repository, instance_id, initial, revision, write_token) =
            runtime().block_on(LORE_CONTEXT.scope(execution.clone(), async {
                let default_branch_id = Context::from(uuid::Uuid::now_v7());
                let write_token = repository::RepositoryWriteToken::acquire(path.as_path()).await;
                let created = repository::create_local(
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
                let instance_id = created.instance_id;
                let repository = Arc::new(
                    RepositoryContext::new(
                        Some(path.clone()),
                        store.clone(),
                        mutable_store.clone(),
                        repository_id,
                        instance_id,
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

                write_files(&files);
                let initial = stage_and_commit(&repository, &write_token, &path, "initial").await;
                let mut revision = initial;

                for edit in 1..=EDITS {
                    for (index, (_, bytes)) in files.iter_mut().enumerate() {
                        let blocks = bytes.len() / BLOCK;
                        if blocks < 2 {
                            continue;
                        }
                        let mut pick = block_key(index, 0, edit);
                        for _ in 0..=(mix(&mut pick) % 3) {
                            let block = (mix(&mut pick) % blocks as u64) as usize;
                            if in_shared_run(index, block) {
                                continue;
                            }
                            fill(
                                block_key(index, block, edit),
                                &mut bytes[block * BLOCK..(block + 1) * BLOCK],
                            );
                        }
                    }
                    write_files(&files);
                    let next =
                        stage_and_commit(&repository, &write_token, &path, &format!("edit {edit}"))
                            .await;
                    assert_ne!(next, revision, "edit {edit} did not make a revision");
                    revision = next;
                }

                (repository, instance_id, initial, revision, write_token)
            }));

        Origin {
            repository,
            store,
            repository_id,
            instance_id,
            initial,
            revision,
            files,
            execution,
            _write_token: write_token,
            _tempdir: tempdir,
        }
    }

    /// What the double does to a fragment before serving it.
    type Tamper = Box<
        dyn Fn(&Address, Fragment, Bytes) -> Result<(Fragment, Bytes), ProtocolError> + Send + Sync,
    >;

    /// The remote: serves fragments out of the origin's store, optionally late and tampered with.
    struct OriginStorage {
        store: Arc<dyn ImmutableStore>,
        partition: RepositoryId,
        /// Delay each answer by a varying few hundred microseconds, so concurrent fetches
        /// complete out of order.
        jitter: bool,
        gets: AtomicU64,
        tamper: Mutex<Option<Tamper>>,
    }

    fn unserved(what: &str) -> ProtocolError {
        ProtocolError::internal(format!("the origin double does not serve {what}"))
    }

    #[async_trait]
    impl Storage for OriginStorage {
        async fn session_start(
            &self,
            _repository: RepositoryId,
            _correlation_id: &str,
        ) -> Result<u32, ProtocolError> {
            Ok(1)
        }

        async fn session_stop(&self, _session_id: u32) -> Result<(), ProtocolError> {
            Ok(())
        }

        async fn get(
            &self,
            _session_id: u32,
            address: &Address,
        ) -> Result<(Fragment, Bytes), ProtocolError> {
            let count = self.gets.fetch_add(1, Ordering::Relaxed);
            let found = match self
                .store
                .clone()
                .get(self.partition, *address, StoreMatch::MatchFull)
                .await
            {
                Ok(found) => found,
                Err(_) => self
                    .store
                    .clone()
                    .get(self.partition, *address, StoreMatch::MatchHash)
                    .await
                    .map_err(|_absent| ProtocolError::from(NotFound))?,
            };
            if self.jitter {
                let mut state = count;
                tokio::time::sleep(Duration::from_micros(mix(&mut state) % 700)).await;
            }
            match self.tamper.lock().as_ref() {
                Some(tamper) => tamper(address, found.0, found.1),
                None => Ok(found),
            }
        }

        async fn put(
            &self,
            _session_id: u32,
            _address: Address,
            _fragment: Fragment,
            _payload: Option<Bytes>,
        ) -> Result<(), ProtocolError> {
            Err(unserved("put"))
        }

        async fn query(
            &self,
            _session_id: u32,
            _address: &[Address],
        ) -> Result<Bytes, ProtocolError> {
            Err(unserved("query"))
        }

        /// Asked to heal a fragment whose payload did not hash to its address: never healed.
        async fn verify(
            &self,
            _session_id: u32,
            _address: &Address,
            _heal: bool,
        ) -> Result<VerifyResult, ProtocolError> {
            Ok(VerifyResult::default())
        }

        async fn copy(
            &self,
            _session_id: u32,
            _source_repository: RepositoryId,
            _source_address: Address,
            _target_context: Context,
        ) -> Result<(), ProtocolError> {
            Err(unserved("copy"))
        }
    }

    struct OriginEnvironment;

    #[async_trait]
    impl Environment for OriginEnvironment {
        async fn get(&self) -> Result<EnvironmentConfig, ProtocolError> {
            Ok(EnvironmentConfig::default())
        }
    }

    /// A transport protocol with only the storage service, registered under its own URL scheme.
    struct OriginProtocol {
        storage: Arc<OriginStorage>,
    }

    #[async_trait]
    impl Protocol for OriginProtocol {
        async fn storage(
            &self,
            _connection: Weak<Connection>,
            _remote_url: &str,
            _auth_url: &str,
            _identity: &str,
            _repository: RepositoryId,
            _index: usize,
        ) -> Result<Arc<dyn Storage>, ProtocolError> {
            Ok(self.storage.clone())
        }

        async fn revision(
            &self,
            _connection: Weak<Connection>,
            _remote_url: &str,
            _auth_url: &str,
            _identity: &str,
            _repository: RepositoryId,
        ) -> Result<Arc<dyn Revision>, ProtocolError> {
            Err(unserved("revisions"))
        }

        async fn repository(
            &self,
            _connection: Weak<Connection>,
            _remote_url: &str,
            _auth_url: &str,
            _identity: &str,
        ) -> Result<Arc<dyn Repository>, ProtocolError> {
            Err(unserved("repositories"))
        }

        async fn admin(
            &self,
            _connection: Weak<Connection>,
            _remote_url: &str,
            _auth_url: &str,
            _identity: &str,
            _repository: RepositoryId,
        ) -> Result<Arc<dyn Admin>, ProtocolError> {
            Err(unserved("admin"))
        }

        async fn lock(
            &self,
            _connection: Weak<Connection>,
            _remote_url: &str,
            _auth_url: &str,
            _identity: &str,
            _repository: RepositoryId,
        ) -> Result<Arc<dyn Lock>, ProtocolError> {
            Err(unserved("locks"))
        }

        async fn environment(
            &self,
            _connection: Weak<Connection>,
            _remote_url: &str,
        ) -> Result<Arc<dyn Environment>, ProtocolError> {
            Ok(Arc::new(OriginEnvironment))
        }
    }

    /// A workspace on the origin's last revision with nothing in its local store.
    struct ColdReader {
        repository: Arc<RepositoryContext>,
        state: Arc<State>,
        layers: Vec<VirtualLayer>,
        storage: Arc<OriginStorage>,
        _store_dir: TempDir,
    }

    impl ColdReader {
        fn set_tamper(&self, tamper: Option<Tamper>) {
            *self.storage.tamper.lock() = tamper;
        }
    }

    /// `cache` is the workspace's `--cache` option: off, as a mount runs unless told otherwise,
    /// every read fetches its fragments again; on, fetched fragments are kept in the local store.
    fn cold_reader(origin: &Origin, jitter: bool, cache: bool) -> ColdReader {
        static NEXT_SCHEME: AtomicUsize = AtomicUsize::new(0);
        let scheme = format!("vfsorigin{}", NEXT_SCHEME.fetch_add(1, Ordering::Relaxed));
        let storage = Arc::new(OriginStorage {
            store: origin.store.clone(),
            partition: origin.repository_id,
            jitter,
            gets: AtomicU64::new(0),
            tamper: Mutex::new(None),
        });
        lore_transport::connection::add(
            &scheme,
            Arc::new(OriginProtocol {
                storage: storage.clone(),
            }),
        )
        .expect("Failed to register the origin protocol");

        // An on-disk store, like the per-mount store of a served workspace.
        let store_dir = generate_tempdir();
        origin.block_on(async {
            let connection = lore_transport::connect(
                &format!("{scheme}://origin/"),
                "",
                origin.repository_id,
                2,
            )
            .await
            .expect("Failed to connect to the origin double");
            let immutable_store = lore_storage::local::immutable_store::create(
                Some(store_dir.path().join("immutable")),
                ImmutableStoreCreateOptions::none(),
                false,
                lore_storage::ImmutableStoreSettings::default(),
            )
            .await
            .expect("Failed to create the cold immutable store");
            let mutable_store: Arc<dyn lore_storage::MutableStore> =
                lore_storage::local::mutable_store::create(
                    None::<&str>,
                    lore_storage::MutableStoreSettings::default(),
                    immutable_store.clone(),
                )
                .await
                .expect("Failed to create the cold mutable store");
            let repository = Arc::new(RepositoryContext::new(
                None,
                immutable_store,
                mutable_store,
                origin.repository_id,
                origin.instance_id,
                Ok(connection),
                Arc::default(),
                RepositoryFormat::Lore,
            ));
            repository.set_disable_cache(!cache);
            let state = State::deserialize(repository.clone(), origin.revision)
                .await
                .expect("Failed to fetch the revision state");
            let layers = vec![VirtualLayer {
                module: repository.clone(),
                module_path: RelativePath::default(),
                layer_path: RelativePath::default(),
                state: state.clone(),
            }];
            ColdReader {
                repository,
                state,
                layers,
                storage,
                _store_dir: store_dir,
            }
        })
    }

    /// Resolve `name` and read `len` bytes at `offset`, as one FUSE read request does. The
    /// buffer starts out non-zero so that bytes the read leaves alone cannot pass for content.
    async fn read_at(
        layers: &[VirtualLayer],
        name: &str,
        offset: usize,
        len: usize,
    ) -> Result<Vec<u8>, String> {
        let resolved = core::try_resolve(layers, name)
            .await
            .map_err(|err| format!("{name}: {err}"))?
            .ok_or_else(|| format!("{name} did not resolve"))?;
        let mut buf = vec![0xA5u8; len];
        let read = core::read_range(&resolved, offset as u64, &mut buf)
            .await
            .map_err(|err| format!("{name} @{offset}+{len}: {err}"))?;
        buf.truncate(read);
        Ok(buf)
    }

    /// Compare one read with the committed bytes; `None` when it is right.
    fn check(name: &str, expected: &[u8], offset: usize, len: usize, got: &[u8]) -> Option<String> {
        let want = &expected[offset.min(expected.len())..(offset + len).min(expected.len())];
        if got == want {
            return None;
        }
        if got.len() != want.len() {
            return Some(format!(
                "{name} @{offset}+{len}: read {} bytes, expected {}",
                got.len(),
                want.len()
            ));
        }
        let first = got.iter().zip(want).position(|(a, b)| a != b).unwrap_or(0);
        let last = got.iter().zip(want).rposition(|(a, b)| a != b).unwrap_or(0);
        Some(format!(
            "{name} @{offset}+{len}: wrong bytes at file offsets {}..={}",
            offset + first,
            offset + last
        ))
    }

    /// What a round of reads came to.
    #[derive(Default)]
    struct Reads {
        /// Reads that returned the committed bytes.
        right: usize,
        /// Reads that returned something else, each described. Never acceptable.
        wrong: Vec<String>,
        /// Reads that returned an error, each described.
        failed: Vec<String>,
    }

    /// Read every file from `READERS` threads at once, each thread driving its reads the way a
    /// FUSE worker does (`block_on` from a thread outside the runtime), in three patterns:
    /// consecutive 128 KiB windows handed out to whichever thread is free (kernel readahead),
    /// random unaligned ranges that overlap each other, and sequential 1 MiB reads (copy-up).
    fn hammer(origin: &Origin, layers: &[VirtualLayer], seed: u64) -> Reads {
        let reads: Mutex<Reads> = Mutex::new(Reads::default());
        let cursors: Vec<AtomicUsize> = origin.files.iter().map(|_| AtomicUsize::new(0)).collect();
        std::thread::scope(|scope| {
            for reader in 0..READERS {
                let reads = &reads;
                let cursors = &cursors;
                scope.spawn(move || {
                    let mut random = seed ^ ((reader as u64) << 32);
                    let verify = |name: &str, expected: &[u8], offset: usize, len: usize| {
                        let outcome = origin.block_on(read_at(layers, name, offset, len));
                        let mut reads = reads.lock();
                        match outcome {
                            Ok(got) => match check(name, expected, offset, len, &got) {
                                None => reads.right += 1,
                                Some(wrong) => reads.wrong.push(wrong),
                            },
                            Err(err) => reads.failed.push(err),
                        }
                    };
                    match reader % 3 {
                        0 => {
                            for (index, (name, expected)) in origin.files.iter().enumerate() {
                                loop {
                                    let window = cursors[index].fetch_add(1, Ordering::Relaxed);
                                    if window * WINDOW >= expected.len() {
                                        break;
                                    }
                                    verify(name, expected, window * WINDOW, WINDOW);
                                }
                            }
                        }
                        1 => {
                            for _ in 0..48 {
                                let (name, expected) = &origin.files
                                    [(mix(&mut random) % origin.files.len() as u64) as usize];
                                let offset = (mix(&mut random) % expected.len() as u64) as usize;
                                let len = 1 + (mix(&mut random) % (3 * WINDOW as u64)) as usize;
                                verify(name, expected, offset, len);
                            }
                        }
                        _ => {
                            let count = origin.files.len();
                            for step in 0..count {
                                let (name, expected) = &origin.files[(reader + step) % count];
                                for offset in (0..expected.len()).step_by(1024 * 1024) {
                                    verify(name, expected, offset, 1024 * 1024);
                                }
                            }
                        }
                    }
                });
            }
        });
        reads.into_inner()
    }

    /// The root fragment of `name` at `revision`, read from the origin: its address, its
    /// header, and its payload as stored (`decompress` off) or as content.
    fn root_fragment(
        origin: &Origin,
        revision: Hash,
        name: &str,
        decompress: bool,
    ) -> (Address, Fragment, Bytes) {
        origin.block_on(async {
            let layers = [VirtualLayer {
                module: origin.repository.clone(),
                module_path: RelativePath::default(),
                layer_path: RelativePath::default(),
                state: State::deserialize(origin.repository.clone(), revision)
                    .await
                    .expect("Failed to load the origin state"),
            }];
            let resolved = core::resolve(&layers, name)
                .await
                .expect("file resolves in the origin");
            let options = if decompress {
                ReadOptions::default()
            } else {
                ReadOptions::default().no_decompress()
            };
            let (fragment, payload) =
                immutable::load_raw(origin.repository.clone(), resolved.node.address, options)
                    .await
                    .expect("Failed to load the root fragment");
            (resolved.node.address, fragment, payload)
        })
    }

    /// The entries of the fragment list at the root of `name` at `revision`.
    fn root_list_at(
        origin: &Origin,
        revision: Hash,
        name: &str,
    ) -> (Address, Vec<FragmentReference>) {
        let (address, fragment, payload) = root_fragment(origin, revision, name, true);
        assert_ne!(
            fragment.flags & FragmentFlags::PayloadFragmented,
            0,
            "{name} is expected to be stored as a fragment list"
        );
        let references = payload
            .chunks_exact(size_of::<FragmentReference>())
            .map(|entry| FragmentReference {
                hash: Hash::from(<[u8; 32]>::try_from(&entry[..32]).expect("32 bytes")),
                offset_content: u64::from_le_bytes(entry[32..40].try_into().expect("8 bytes")),
            })
            .collect();
        (address, references)
    }

    /// The entries of the fragment list at the root of `name` at the last revision.
    fn root_list(origin: &Origin, name: &str) -> (Address, Vec<FragmentReference>) {
        root_list_at(origin, origin.revision, name)
    }

    /// A fragment of a.bin for the double to damage, with the content span it covers: one
    /// that b.bin does not use, and with neighbours on both sides.
    fn victim_fragment(origin: &Origin) -> (Hash, usize, usize) {
        let (_, list) = root_list(origin, "a.bin");
        let (_, other) = root_list(origin, "b.bin");
        let shared: HashSet<Hash> = other.iter().map(|entry| entry.hash).collect();
        let index = (3..list.len() - 1)
            .find(|&index| !shared.contains(&list[index].hash))
            .expect("a.bin has a fragment of its own");
        (
            list[index].hash,
            list[index].offset_content as usize,
            list[index + 1].offset_content as usize,
        )
    }

    /// GRID#3299: many threads reading the same files through the lazy read path, cold, must get
    /// exactly the committed bytes for every range.
    #[test]
    fn cold_concurrent_reads_return_committed_bytes() {
        let origin = build_origin();

        // The fixture is what it claims. An edited file is assembled from fragments stored by
        // the initial revision and fragments stored by later ones.
        let (_, list_a) = root_list(&origin, "a.bin");
        let (_, initial_a) = root_list_at(&origin, origin.initial, "a.bin");
        let initial_hashes: HashSet<Hash> = initial_a.iter().map(|entry| entry.hash).collect();
        let kept = list_a
            .iter()
            .filter(|entry| initial_hashes.contains(&entry.hash))
            .count();
        assert!(
            kept > 0 && kept < list_a.len(),
            "a.bin is expected to mix fragments of several revisions ({kept} of {} are initial)",
            list_a.len()
        );
        // Two files share fragments under their own lists.
        let (_, list_b) = root_list(&origin, "b.bin");
        let hashes_a: HashSet<Hash> = list_a.iter().map(|entry| entry.hash).collect();
        assert!(
            list_b.iter().any(|entry| hashes_a.contains(&entry.hash)),
            "a.bin and b.bin are expected to share fragments"
        );
        assert!(
            list_a
                .windows(2)
                .any(|pair| !(pair[1].offset_content - pair[0].offset_content)
                    .is_multiple_of(WINDOW as u64)),
            "fragment boundaries are expected not to line up with read windows"
        );
        // Text is stored compressed, so reads of it decompress before they hash.
        let (_, note, _) = root_fragment(&origin, origin.revision, "logs/note.txt", false);
        assert!(
            note.flags & FragmentFlags::PayloadCompressed != 0
                && u64::from(note.size_payload) < note.size_content,
            "logs/note.txt is expected to be stored compressed"
        );

        for iteration in 0..stress_iterations() {
            let cache = iteration % 2 == 1;
            let reader = cold_reader(&origin, true, cache);
            let reads = hammer(&origin, &reader.layers, iteration as u64);
            assert!(
                reads.wrong.is_empty() && reads.failed.is_empty(),
                "iteration {iteration}: {} read(s) returned wrong bytes, {} failed; first: {}",
                reads.wrong.len(),
                reads.failed.len(),
                reads.wrong.first().or(reads.failed.first()).unwrap()
            );
            let fetched = reader.storage.gets.load(Ordering::Relaxed);
            assert!(
                fetched > list_a.len() as u64,
                "the reads were expected to fetch from the remote"
            );
            // The same reads again: from the local store when it caches, fetched anew when not.
            let reads = hammer(&origin, &reader.layers, !(iteration as u64));
            assert!(
                reads.wrong.is_empty() && reads.failed.is_empty(),
                "iteration {iteration}, second pass: {} read(s) returned wrong bytes, {} failed; first: {}",
                reads.wrong.len(),
                reads.failed.len(),
                reads.wrong.first().or(reads.failed.first()).unwrap()
            );
            if cache {
                assert_eq!(
                    reader.storage.gets.load(Ordering::Relaxed),
                    fetched,
                    "with the cache on, the second pass was expected to be served locally"
                );
            }
        }
    }

    /// Fetches that fail in passing, whether the transport retries them (a refused command) or
    /// gives up (a lost connection), make reads slower or make them fail. They never make a read
    /// return other bytes.
    #[test]
    fn failing_fetches_never_turn_into_wrong_bytes() {
        let origin = build_origin();
        let refused: fn() -> ProtocolError = || ProtocolError::internal("refused in passing");
        let disconnected: fn() -> ProtocolError = || ProtocolError::from(Disconnected);
        for (what, error) in [("refused", refused), ("disconnected", disconnected)] {
            let reader = cold_reader(&origin, true, false);
            let calls = AtomicU64::new(0);
            reader.set_tamper(Some(Box::new(move |_address, fragment, payload| {
                if calls.fetch_add(1, Ordering::Relaxed) % 5 == 2 {
                    Err(error())
                } else {
                    Ok((fragment, payload))
                }
            })));
            let reads = hammer(&origin, &reader.layers, 7);
            assert!(
                reads.wrong.is_empty(),
                "{what}: {} read(s) returned wrong bytes, first: {}",
                reads.wrong.len(),
                reads.wrong[0]
            );
            assert!(reads.right > 0, "{what}: no read got through");
        }
    }

    /// A fetch that does not hash to its address fails every read that needs it, leaves the
    /// rest of the file readable, and poisons nothing: once the remote serves the right bytes
    /// the same read succeeds.
    #[test]
    fn corrupted_fetch_fails_the_read() {
        let origin = build_origin();
        let expected = origin.file("a.bin");
        let (victim, victim_start, victim_end) = victim_fragment(&origin);

        let flip: fn(Bytes) -> Bytes = |payload| {
            let mut bytes = payload.to_vec();
            let middle = bytes.len() / 2;
            bytes[middle] ^= 0x01;
            Bytes::from(bytes)
        };
        let cut: fn(Bytes) -> Bytes = |payload| payload.slice(..payload.len() - 1);
        let zero: fn(Bytes) -> Bytes = |payload| Bytes::from(vec![0u8; payload.len()]);

        let kinds = [("bit flip", flip), ("short payload", cut), ("zeros", zero)];
        for (cache, (what, damage)) in [false, true]
            .into_iter()
            .flat_map(|cache| kinds.into_iter().map(move |kind| (cache, kind)))
        {
            let what = format!("{what}, cache {}", if cache { "on" } else { "off" });
            let reader = cold_reader(&origin, false, cache);
            reader.set_tamper(Some(Box::new(move |address, mut fragment, payload| {
                if address.hash != victim {
                    return Ok((fragment, payload));
                }
                let payload = damage(payload);
                fragment.size_payload = payload.len() as u32;
                Ok((fragment, payload))
            })));

            let inside = origin.block_on(read_at(
                &reader.layers,
                "a.bin",
                victim_start + 100,
                victim_end - victim_start - 200,
            ));
            assert!(
                inside.is_err(),
                "{what}: a read inside the fragment returned bytes"
            );
            let across =
                origin.block_on(read_at(&reader.layers, "a.bin", victim_start - 4096, 8192));
            assert!(
                across.is_err(),
                "{what}: a read into the fragment returned bytes"
            );
            let whole = origin.block_on(read_at(&reader.layers, "a.bin", 0, expected.len()));
            assert!(
                whole.is_err(),
                "{what}: a read of the whole file returned bytes"
            );

            // Windows that end where the damaged fragment begins, or begin where it ends, do
            // not need it.
            let before = origin
                .block_on(read_at(&reader.layers, "a.bin", 0, victim_start))
                .unwrap_or_else(|err| panic!("{what}: the bytes before the fragment: {err}"));
            assert_eq!(check("a.bin", expected, 0, victim_start, &before), None);
            let after = origin
                .block_on(read_at(&reader.layers, "a.bin", victim_end, WINDOW))
                .unwrap_or_else(|err| panic!("{what}: the bytes after the fragment: {err}"));
            assert_eq!(check("a.bin", expected, victim_end, WINDOW, &after), None);

            reader.set_tamper(None);
            let healed = origin
                .block_on(read_at(&reader.layers, "a.bin", 0, expected.len()))
                .unwrap_or_else(|err| panic!("{what}: once the remote is right again: {err}"));
            assert_eq!(check("a.bin", expected, 0, expected.len(), &healed), None);
        }
    }

    /// A fragment whose header claims another content size than its place in the file allows is
    /// not read around. The store's ranged read clamps a window to the size the root fragment
    /// claims and reports a window wholly past it as read without writing a byte, so the too
    /// small size served below would otherwise turn those windows into reads of the caller's own
    /// zeros.
    #[test]
    fn fetched_size_that_disagrees_with_the_node_fails_the_read() {
        let origin = build_origin();
        let expected = origin.file("a.bin");
        let (root, _) = root_list(&origin, "a.bin");
        let reader = cold_reader(&origin, false, false);
        reader.set_tamper(Some(Box::new(move |address, mut fragment, payload| {
            if *address == root {
                fragment.size_content = 1024 * 1024;
            }
            Ok((fragment, payload))
        })));

        // Past the claimed size first, then across it, then inside it.
        for offset in [
            1024 * 1024,
            expected.len() - WINDOW,
            1024 * 1024 - WINDOW / 2,
            0,
        ] {
            if let Ok(bytes) = origin.block_on(read_at(&reader.layers, "a.bin", offset, WINDOW)) {
                panic!(
                    "a read at {offset} of a file whose root fragment has the wrong size returned {}",
                    check("a.bin", expected, offset, WINDOW, &bytes)
                        .unwrap_or_else(|| "the file's bytes".to_string())
                );
            }
        }
        // Other files are untouched.
        let other = origin.file("b.bin");
        let bytes = origin
            .block_on(read_at(&reader.layers, "b.bin", 0, other.len()))
            .expect("b.bin reads");
        assert_eq!(check("b.bin", other, 0, other.len(), &bytes), None);
    }

    /// Fragments that pass their hash check but do not add up to the file (a list whose offsets
    /// disagree with the sizes of the fragments it names, or a node larger than its content)
    /// fail the read. The store's ranged read does not compare a fragment's size with the span
    /// its list gives it: left to it, a fragment longer than its span is cut short without a
    /// word, and a window past the root fragment's content is reported as read with nothing
    /// written.
    #[test]
    fn fragments_that_do_not_add_up_fail_the_read() {
        let origin = build_origin();
        let context = Context::from(uuid::Uuid::now_v7());
        let mut first = vec![0u8; 100_000];
        fill(1, &mut first);
        let mut second = vec![0u8; 100_000];
        fill(2, &mut second);

        let put = |flags: u32, size_content: u64, payload: Vec<u8>| -> Address {
            let address = Address {
                context,
                hash: Hash::hash_buffer(&payload),
            };
            let fragment = Fragment {
                flags,
                size_payload: payload.len() as u32,
                size_content,
            };
            origin
                .block_on(origin.store.clone().put(
                    origin.repository_id,
                    address,
                    fragment,
                    Some(Bytes::from(payload)),
                    false,
                ))
                .expect("Failed to store a fixture fragment");
            address
        };
        let first_leaf = put(0, first.len() as u64, first.clone());
        let second_leaf = put(0, second.len() as u64, second.clone());
        // A list placing the second fragment at `second_offset`, for `size` bytes of content.
        let list = |second_offset: u64, size: u64| -> Address {
            let mut payload = Vec::with_capacity(2 * size_of::<FragmentReference>());
            for (hash, offset) in [(first_leaf.hash, 0u64), (second_leaf.hash, second_offset)] {
                payload.extend_from_slice(hash.data());
                payload.extend_from_slice(&offset.to_le_bytes());
            }
            put(FragmentFlags::PayloadFragmented.bits(), size, payload)
        };

        let (template, state) = origin.block_on(async {
            let state = State::deserialize(origin.repository.clone(), origin.revision)
                .await
                .expect("Failed to load the origin state");
            let layers = [VirtualLayer {
                module: origin.repository.clone(),
                module_path: RelativePath::default(),
                layer_path: RelativePath::default(),
                state: state.clone(),
            }];
            let resolved = core::resolve(&layers, "a.bin")
                .await
                .expect("a.bin resolves");
            (resolved, state)
        });
        let read = |address: Address, size: u64, offset: u64, len: usize| {
            let resolved = ResolvedNode {
                repository: origin.repository.clone(),
                state: state.clone(),
                node: Node {
                    address,
                    size,
                    ..template.node
                },
                node_id: template.node_id,
            };
            origin.block_on(async {
                let mut buf = vec![0xA5u8; len];
                core::read_range(&resolved, offset, &mut buf)
                    .await
                    .map(|read| {
                        buf.truncate(read);
                        buf
                    })
            })
        };

        // The control: a list that adds up reads back as the two fragments end to end.
        let sound = list(100_000, 200_000);
        let bytes = read(sound, 200_000, 0, 200_000).expect("a sound list reads");
        assert_eq!(bytes, [first.as_slice(), second.as_slice()].concat());
        let bytes = read(sound, 200_000, 99_990, 20).expect("a sound list reads across fragments");
        assert_eq!(bytes, [&first[99_990..], &second[..10]].concat());

        // The first fragment holds 100 000 bytes; the list gives it 90 000.
        let overlapping = list(90_000, 190_000);
        assert!(read(overlapping, 190_000, 0, 190_000).is_err());
        assert!(read(overlapping, 190_000, 80_000, 20_000).is_err());
        // The first fragment holds 100 000 bytes; the list gives it 110 000.
        let gapped = list(110_000, 210_000);
        assert!(read(gapped, 210_000, 0, 50_000).is_err());
        assert!(read(gapped, 210_000, 105_000, 10_000).is_err());
        // The node claims more content than its root fragment holds.
        assert!(read(sound, 300_000, 0, 4096).is_err());
        assert!(read(sound, 300_000, 250_000, 4096).is_err());
        // And less.
        assert!(read(sound, 150_000, 0, 4096).is_err());
    }

    /// While the remote cannot be reached, a workspace that has not read its tree yet says so:
    /// listing a directory or resolving a path fails. Neither comes back empty or "not there",
    /// and once the remote answers again the same calls succeed.
    #[test]
    fn unreadable_tree_is_an_error_not_an_empty_directory() {
        let origin = build_origin();
        let reader = cold_reader(&origin, false, false);
        reader.set_tamper(Some(Box::new(|_address, _fragment, _payload| {
            Err(ProtocolError::from(Disconnected))
        })));
        origin.block_on(async {
            assert!(core::try_enumerate(&reader.layers, "").await.is_err());
            assert!(core::try_enumerate(&reader.layers, "assets").await.is_err());
            assert!(core::try_resolve(&reader.layers, "a.bin").await.is_err());
            assert!(
                core::try_resolve(&reader.layers, "assets/c.bin")
                    .await
                    .is_err()
            );
        });

        reader.set_tamper(None);
        origin.block_on(async {
            let names = |entries: Vec<core::DirEntry>| {
                let mut names: Vec<String> = entries.into_iter().map(|entry| entry.name).collect();
                names.sort();
                names
            };
            let root = core::try_enumerate(&reader.layers, "")
                .await
                .expect("the root lists");
            assert_eq!(
                names(root),
                ["a.bin", "assets", "b.bin", "logs", "tiny.txt"]
            );
            let assets = core::try_enumerate(&reader.layers, "assets")
                .await
                .expect("assets lists");
            assert_eq!(names(assets), ["c.bin", "small.bin"]);
            let found = core::try_resolve(&reader.layers, "assets/c.bin")
                .await
                .expect("assets/c.bin resolves");
            assert!(found.is_some());
            // A path that is not in the tree is a miss, not a failure.
            for absent in ["missing", "assets/missing", "a.bin/below-a-file"] {
                let missed = core::try_resolve(&reader.layers, absent)
                    .await
                    .unwrap_or_else(|err| panic!("{absent}: {err}"));
                assert!(missed.is_none(), "{absent} resolved");
                let listed = core::try_enumerate(&reader.layers, absent)
                    .await
                    .unwrap_or_else(|err| panic!("{absent}: {err}"));
                assert!(listed.is_empty(), "{absent} has entries");
            }
        });
    }

    /// A cold reader served over FUSE, with the directories it uses. Unmounts when dropped.
    #[cfg(target_os = "linux")]
    struct Mounted {
        // Dropped first: the mount goes before the directories under it.
        _session: fuser::BackgroundSession,
        mountpoint: TempDir,
        overlay: TempDir,
    }

    #[cfg(target_os = "linux")]
    fn mount(origin: &Origin, reader: &ColdReader, read_only: bool) -> Mounted {
        use lore_revision::vfs::fuse::LoreFuse;
        use lore_revision::vfs::fuse::VfsMountOptions;

        let mountpoint = generate_tempdir();
        let overlay = generate_tempdir();
        let session = LoreFuse::new(
            reader.repository.clone(),
            reader.state.clone(),
            None,
            Some(overlay.path().to_path_buf()),
            origin.execution.clone(),
        )
        .with_mount_options(VfsMountOptions {
            read_only,
            ..VfsMountOptions::default()
        })
        .spawn(mountpoint.path())
        .expect("Failed to mount FUSE");
        // Wait for the kernel to list the mount, which asks nothing of the projection.
        let path = mountpoint.path().to_string_lossy().into_owned();
        for _ in 0..100 {
            let mounts = std::fs::read_to_string("/proc/self/mounts").unwrap_or_default();
            if mounts
                .lines()
                .any(|line| line.split(' ').nth(1) == Some(path.as_str()))
            {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Mounted {
            _session: session,
            mountpoint,
            overlay,
        }
    }

    #[cfg(target_os = "linux")]
    fn fuse_available() -> bool {
        let available = std::path::Path::new("/dev/fuse").exists();
        if !available {
            eprintln!("skipping FUSE test: /dev/fuse is not available");
        }
        available
    }

    /// The same first reads through a real FUSE mount: several threads read every file to the
    /// end in small sequential reads, so the kernel's readahead issues the parallel requests.
    /// Then a mount whose remote serves one corrupt fragment: the reader gets `EIO`.
    #[cfg(target_os = "linux")]
    #[test]
    fn mounted_first_reads_match_and_corruption_is_an_io_error() {
        use std::io::Read;

        if !fuse_available() {
            return;
        }
        let origin = build_origin();

        for iteration in 0..stress_iterations() {
            let reader = cold_reader(&origin, true, iteration % 2 == 1);
            let mounted = mount(&origin, &reader, true);
            let failures: Mutex<Vec<String>> = Mutex::new(Vec::new());
            std::thread::scope(|scope| {
                for thread in 0..6 {
                    let failures = &failures;
                    let origin = &origin;
                    let root = mounted.mountpoint.path();
                    scope.spawn(move || {
                        let count = origin.files.len();
                        for step in 0..count {
                            let (name, expected) = &origin.files[(thread + step) % count];
                            let mut got = Vec::with_capacity(expected.len());
                            let mut chunk = [0u8; 4096];
                            let outcome =
                                std::fs::File::open(root.join(name)).and_then(|mut file| {
                                    loop {
                                        match file.read(&mut chunk)? {
                                            0 => return Ok(()),
                                            read => got.extend_from_slice(&chunk[..read]),
                                        }
                                    }
                                });
                            let failure = match outcome {
                                Ok(()) => check(name, expected, 0, expected.len().max(1), &got),
                                Err(err) => Some(format!("{name}: {err}")),
                            };
                            if let Some(failure) = failure {
                                failures.lock().push(failure);
                            }
                        }
                    });
                }
            });
            drop(mounted);
            let failures = failures.into_inner();
            assert!(
                failures.is_empty(),
                "iteration {iteration}: {} mounted read(s) went wrong, first: {}",
                failures.len(),
                failures[0]
            );
        }

        let (victim, _, _) = victim_fragment(&origin);
        let reader = cold_reader(&origin, false, false);
        reader.set_tamper(Some(Box::new(move |address, fragment, payload| {
            if address.hash != victim {
                return Ok((fragment, payload));
            }
            let mut bytes = payload.to_vec();
            bytes[0] ^= 0x80;
            Ok((fragment, Bytes::from(bytes)))
        })));
        let mounted = mount(&origin, &reader, true);
        let error = std::fs::read(mounted.mountpoint.path().join("a.bin"))
            .expect_err("a file with a corrupt fragment was read to the end");
        assert_eq!(
            error.raw_os_error(),
            Some(libc::EIO),
            "unexpected error: {error}"
        );
        let other = origin.file("b.bin");
        let bytes = std::fs::read(mounted.mountpoint.path().join("b.bin")).expect("b.bin reads");
        assert_eq!(check("b.bin", other, 0, other.len(), &bytes), None);
    }

    /// A mount first used while its remote cannot be reached answers `EIO` to a listing and to a
    /// `stat`, rather than an empty directory and "no such file". It serves the tree as soon as
    /// the remote is back.
    #[cfg(target_os = "linux")]
    #[test]
    fn mounted_outage_is_an_io_error_not_an_empty_directory() {
        if !fuse_available() {
            return;
        }
        let origin = build_origin();
        let reader = cold_reader(&origin, false, false);
        reader.set_tamper(Some(Box::new(|_address, _fragment, _payload| {
            Err(ProtocolError::from(Disconnected))
        })));
        let mounted = mount(&origin, &reader, true);
        let root = mounted.mountpoint.path();
        let list = || {
            std::fs::read_dir(root).and_then(|entries| {
                let mut names = entries
                    .map(|entry| {
                        entry.map(|entry| entry.file_name().to_string_lossy().into_owned())
                    })
                    .collect::<std::io::Result<Vec<String>>>()?;
                names.sort();
                Ok(names)
            })
        };

        let error = list().expect_err("a directory that could not be read was listed");
        assert_eq!(
            error.raw_os_error(),
            Some(libc::EIO),
            "unexpected error: {error}"
        );
        let error = std::fs::metadata(root.join("a.bin"))
            .expect_err("a file that could not be looked up was found");
        assert_eq!(
            error.raw_os_error(),
            Some(libc::EIO),
            "unexpected error: {error}"
        );

        reader.set_tamper(None);
        assert_eq!(
            list().expect("the root lists"),
            ["a.bin", "assets", "b.bin", "logs", "tiny.txt"]
        );
        let bytes = std::fs::read(root.join("tiny.txt")).expect("tiny.txt reads");
        assert_eq!(bytes, origin.file("tiny.txt"));
    }

    /// Opening a projected file for writing copies it into the overlay first. When that copy
    /// fails the open fails and the overlay holds nothing for the path: a file left there, empty
    /// or partial, would from then on be served as the file's content.
    #[cfg(target_os = "linux")]
    #[test]
    fn failed_copy_up_leaves_nothing_in_the_overlay() {
        if !fuse_available() {
            return;
        }
        let origin = build_origin();
        let expected = origin.file("a.bin");
        let (victim, _, _) = victim_fragment(&origin);
        let reader = cold_reader(&origin, false, false);
        reader.set_tamper(Some(Box::new(move |address, fragment, payload| {
            if address.hash == victim {
                Err(ProtocolError::from(Disconnected))
            } else {
                Ok((fragment, payload))
            }
        })));
        let mounted = mount(&origin, &reader, false);
        let path = mounted.mountpoint.path().join("a.bin");
        let copy = mounted.overlay.path().join("a.bin");
        let open = || std::fs::OpenOptions::new().write(true).open(&path);

        let error = open().expect_err("a.bin opened for writing without its content");
        assert_eq!(
            error.raw_os_error(),
            Some(libc::EIO),
            "unexpected error: {error}"
        );
        assert!(
            !copy.exists(),
            "the failed copy left {} bytes in the overlay",
            std::fs::metadata(&copy).map_or(0, |metadata| metadata.len())
        );

        reader.set_tamper(None);
        drop(open().expect("a.bin opens for writing"));
        let copied = std::fs::read(&copy).expect("the overlay holds a.bin");
        assert_eq!(check("a.bin", expected, 0, expected.len(), &copied), None);
        let served = std::fs::read(&path).expect("a.bin reads");
        assert_eq!(check("a.bin", expected, 0, expected.len(), &served), None);
    }
}
