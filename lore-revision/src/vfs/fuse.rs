// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Linux FUSE backend for the virtual file system.
//!
//! The mount presents a *union* of two sources so that externally-managed files and
//! Lore-projected files coexist in the same directory tree (the "project-into" model the
//! Windows `ProjFS` provider gives by construction):
//!
//! - **pass-through** — entries that exist on a real backing directory (a git working tree,
//!   untracked files, already-materialized files) are served straight from the filesystem;
//! - **projection** — entries that exist in Lore state but not on the backing directory are
//!   presented as lazily-hydrated placeholders resolved through [`crate::vfs::core`].
//!
//! Reads hydrate on demand. This module implements the read side (lazy projection +
//! pass-through); write interception is layered on top separately.

use std::collections::HashMap;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use fuser::BackgroundSession;
use fuser::Errno;
use fuser::FileAttr;
use fuser::FileHandle;
use fuser::FileType;
use fuser::Filesystem;
use fuser::Generation;
use fuser::INodeNo;
use fuser::LockOwner;
use fuser::MountOption;
use fuser::OpenFlags;
use fuser::ReplyAttr;
use fuser::ReplyData;
use fuser::ReplyDirectory;
use fuser::ReplyEntry;
use fuser::Request;
use parking_lot::Mutex;

use lore_base::lore_spawn;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::runtime::runtime;

use crate::interface::ExecutionContext;
use crate::lore::execution_context;
use crate::lore_error;
use crate::lore_info;
use crate::repository::RepositoryContext;
use crate::repository::clone::VirtualLayer;
use crate::state::State;
use crate::util::path::RelativePath;
use crate::vfs::core;
use crate::vfs::core::EntryKind;
use crate::vfs::core::ResolvedNode;

/// The FUSE root inode is fixed by the protocol.
const ROOT_INODE: u64 = 1;
/// Attribute/entry cache lifetime handed to the kernel. The mount serves a single revision,
/// so the tree is stable and a modest TTL is safe.
const TTL: Duration = Duration::from_secs(1);

/// Bidirectional map between kernel inode numbers and repository-relative paths.
///
/// The neutral core operates on repository-relative paths (forward-slash separated, no leading
/// slash, empty string for the root), so the backend only needs to translate the `u64` inodes
/// FUSE works with to and from those paths.
struct InodeTable {
    by_inode: HashMap<u64, String>,
    by_path: HashMap<String, u64>,
    next: u64,
}

impl InodeTable {
    fn new() -> Self {
        let mut by_inode = HashMap::new();
        let mut by_path = HashMap::new();
        by_inode.insert(ROOT_INODE, String::new());
        by_path.insert(String::new(), ROOT_INODE);
        Self {
            by_inode,
            by_path,
            next: ROOT_INODE + 1,
        }
    }

    fn path(&self, inode: u64) -> Option<&str> {
        self.by_inode.get(&inode).map(String::as_str)
    }

    /// Return the inode for a path, allocating a new one on first sight.
    fn intern(&mut self, path: &str) -> u64 {
        if let Some(&inode) = self.by_path.get(path) {
            return inode;
        }
        let inode = self.next;
        self.next += 1;
        self.by_inode.insert(inode, path.to_string());
        self.by_path.insert(path.to_string(), inode);
        inode
    }
}

/// A mounted Lore workspace served over FUSE.
pub struct LoreFuse {
    layers: Vec<VirtualLayer>,
    /// Real directory whose contents pass through the mount (git tree, materialized files).
    backing_dir: Option<PathBuf>,
    execution: Arc<ExecutionContext>,
    /// Revision timestamp (ms since Unix epoch) reported for projected entries. Computed
    /// lazily on first use from inside a callback, so construction never blocks on the async
    /// runtime (which would panic when `serve` is called from an async context).
    revision_ms: OnceLock<u64>,
    inodes: Mutex<InodeTable>,
}

impl LoreFuse {
    /// Build a backend over the given layers. `backing_dir`, when set, is the real directory
    /// whose files pass through the mount and take precedence over projected entries. The
    /// execution context is captured for scoping the async work each callback drives.
    pub fn new(
        repository: Arc<RepositoryContext>,
        state: Arc<State>,
        extra_layer: Option<VirtualLayer>,
        backing_dir: Option<PathBuf>,
        execution: Arc<ExecutionContext>,
    ) -> Self {
        let mut layers = vec![VirtualLayer {
            module: repository,
            module_path: RelativePath::default(),
            layer_path: RelativePath::default(),
            state,
        }];
        if let Some(layer) = extra_layer {
            layers.push(layer);
        }

        Self {
            layers,
            backing_dir,
            execution,
            revision_ms: OnceLock::new(),
            inodes: Mutex::new(InodeTable::new()),
        }
    }

    /// Revision timestamp reported for projected entries, computed once on first use.
    fn revision_ms(&self) -> u64 {
        *self
            .revision_ms
            .get_or_init(|| self.block_on(core::revision_timestamp_ms(&self.layers[0])))
    }

    /// Mount in a background thread, returning a session handle that unmounts on drop.
    pub fn spawn(self, mountpoint: impl AsRef<Path>) -> std::io::Result<BackgroundSession> {
        fuser::spawn_mount(self, mountpoint.as_ref(), &mount_config())
    }

    /// Run the event loop on the current thread until the filesystem is unmounted.
    pub fn run(self, mountpoint: impl AsRef<Path>) -> std::io::Result<()> {
        fuser::mount(self, mountpoint.as_ref(), &mount_config())
    }

    fn block_on<F: std::future::Future>(&self, fut: F) -> F::Output {
        runtime().block_on(LORE_CONTEXT.scope(self.execution.clone(), fut))
    }

    /// Absolute path of a repository-relative path on the backing directory, if configured.
    fn backing_path(&self, relative: &str) -> Option<PathBuf> {
        self.backing_dir.as_ref().map(|base| base.join(relative))
    }

    /// Build attributes for a projected node.
    fn projected_attr(&self, inode: u64, kind: EntryKind, size: u64) -> FileAttr {
        file_attr(inode, kind, size, self.revision_ms())
    }
}

fn mount_config() -> fuser::Config {
    let mut config = fuser::Config::default();
    config.mount_options = vec![MountOption::FSName("lore".to_string())];
    // Bound the worker pool so multiple hydrating reads proceed concurrently rather than
    // serializing on a single event-loop thread.
    let threads = std::thread::available_parallelism().map_or(4, |n| n.get().clamp(4, 16));
    config.n_threads = Some(threads);
    config
}

/// Join a repository-relative parent path with a child name.
fn join_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

/// Milliseconds since the Unix epoch as a `SystemTime`.
fn ms_to_system_time(ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms)
}

/// Build attributes for a projected entry from its kind, size, and revision timestamp.
fn file_attr(inode: u64, kind: EntryKind, size: u64, mtime_ms: u64) -> FileAttr {
    let time = ms_to_system_time(mtime_ms);
    let (kind, perm, nlink) = match kind {
        EntryKind::Directory => (FileType::Directory, 0o755, 2),
        EntryKind::File => (FileType::RegularFile, 0o644, 1),
    };
    FileAttr {
        ino: INodeNo(inode),
        size,
        blocks: size.div_ceil(512),
        atime: time,
        mtime: time,
        ctime: time,
        crtime: time,
        kind,
        perm,
        nlink,
        // Safety: getuid/getgid are always-succeeding syscalls with no preconditions.
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

/// Build attributes for a pass-through entry from its on-disk metadata.
fn attr_from_metadata(inode: u64, metadata: &std::fs::Metadata) -> FileAttr {
    use std::os::unix::fs::MetadataExt;

    let kind = FileType::from_std(metadata.file_type()).unwrap_or(FileType::RegularFile);
    let mtime = metadata.modified().unwrap_or(UNIX_EPOCH);
    FileAttr {
        ino: INodeNo(inode),
        size: metadata.len(),
        blocks: metadata.blocks(),
        atime: mtime,
        mtime,
        ctime: mtime,
        crtime: mtime,
        kind,
        perm: (metadata.mode() & 0o7777) as u16,
        nlink: metadata.nlink() as u32,
        uid: metadata.uid(),
        gid: metadata.gid(),
        rdev: metadata.rdev() as u32,
        blksize: 4096,
        flags: 0,
    }
}

/// Read up to `size` bytes at `offset` from a real backing file.
fn read_backing(path: &Path, offset: u64, size: u32) -> std::io::Result<Vec<u8>> {
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut buffer = Vec::with_capacity(size as usize);
    file.take(u64::from(size)).read_to_end(&mut buffer)?;
    Ok(buffer)
}

fn entry_kind_to_file_type(kind: EntryKind) -> FileType {
    match kind {
        EntryKind::Directory => FileType::Directory,
        EntryKind::File => FileType::RegularFile,
    }
}

impl Filesystem for LoreFuse {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let Some(name) = name.to_str() else {
            reply.error(Errno::ENOENT);
            return;
        };
        let relative = {
            let table = self.inodes.lock();
            let Some(parent_path) = table.path(parent.0) else {
                reply.error(Errno::ENOENT);
                return;
            };
            join_path(parent_path, name)
        };

        // Pass-through wins over projection.
        if let Some(backing) = self.backing_path(&relative)
            && let Ok(metadata) = std::fs::symlink_metadata(&backing)
        {
            let inode = self.inodes.lock().intern(&relative);
            reply.entry(&TTL, &attr_from_metadata(inode, &metadata), Generation(0));
            return;
        }

        match self.block_on(core::resolve(&self.layers, &relative)) {
            Some(resolved) => {
                let inode = self.inodes.lock().intern(&relative);
                reply.entry(
                    &TTL,
                    &self.projected_attr(inode, resolved.kind(), resolved.size()),
                    Generation(0),
                );
            }
            None => reply.error(Errno::ENOENT),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        if ino.0 == ROOT_INODE {
            reply.attr(
                &TTL,
                &self.projected_attr(ROOT_INODE, EntryKind::Directory, 0),
            );
            return;
        }

        let relative = {
            let table = self.inodes.lock();
            let Some(path) = table.path(ino.0) else {
                reply.error(Errno::ENOENT);
                return;
            };
            path.to_string()
        };

        if let Some(backing) = self.backing_path(&relative)
            && let Ok(metadata) = std::fs::symlink_metadata(&backing)
        {
            reply.attr(&TTL, &attr_from_metadata(ino.0, &metadata));
            return;
        }

        match self.block_on(core::resolve(&self.layers, &relative)) {
            Some(resolved) => {
                reply.attr(
                    &TTL,
                    &self.projected_attr(ino.0, resolved.kind(), resolved.size()),
                );
            }
            None => reply.error(Errno::ENOENT),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let relative = {
            let table = self.inodes.lock();
            let Some(path) = table.path(ino.0) else {
                reply.error(Errno::ENOENT);
                return;
            };
            path.to_string()
        };

        let parent_inode = if relative.is_empty() {
            ROOT_INODE
        } else {
            let parent = relative.rsplit_once('/').map_or("", |(head, _)| head);
            self.inodes.lock().intern(parent)
        };

        // "." and ".." lead; the kernel largely ignores their inode numbers.
        let mut listing: Vec<(u64, FileType, String)> = vec![
            (ino.0, FileType::Directory, ".".to_string()),
            (parent_inode, FileType::Directory, "..".to_string()),
        ];

        let mut seen: HashSet<String> = HashSet::new();

        // Pass-through entries from the backing directory take precedence.
        if let Some(backing) = self.backing_path(&relative)
            && let Ok(entries) = std::fs::read_dir(&backing)
        {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if !seen.insert(name.clone()) {
                    continue;
                }
                let kind = entry
                    .file_type()
                    .ok()
                    .and_then(FileType::from_std)
                    .unwrap_or(FileType::RegularFile);
                let child = join_path(&relative, &name);
                let child_inode = self.inodes.lock().intern(&child);
                listing.push((child_inode, kind, name));
            }
        }

        // Projected entries fill in the rest.
        for entry in self.block_on(core::enumerate(&self.layers, &relative)) {
            if !seen.insert(entry.name.clone()) {
                continue;
            }
            let child = join_path(&relative, &entry.name);
            let child_inode = self.inodes.lock().intern(&child);
            listing.push((child_inode, entry_kind_to_file_type(entry.kind), entry.name));
        }

        for (index, (child_inode, kind, name)) in
            listing.into_iter().enumerate().skip(offset as usize)
        {
            // The offset is a resume cookie: the index of the *next* entry to emit.
            if reply.add(INodeNo(child_inode), (index + 1) as u64, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let relative = {
            let table = self.inodes.lock();
            let Some(path) = table.path(ino.0) else {
                reply.error(Errno::ENOENT);
                return;
            };
            path.to_string()
        };

        // Pass-through file: read straight from the backing directory.
        if let Some(backing) = self.backing_path(&relative)
            && backing.is_file()
        {
            match read_backing(&backing, offset, size) {
                Ok(data) => reply.data(&data),
                Err(err) => {
                    lore_error!("vfs pass-through read failed for {relative}: {err}");
                    reply.error(Errno::EIO);
                }
            }
            return;
        }

        // Projected file: hydrate the requested window through the neutral core.
        let Some(resolved) = self.block_on(core::resolve(&self.layers, &relative)) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if resolved.kind() == EntryKind::Directory {
            reply.error(Errno::EISDIR);
            return;
        }

        let mut buffer = vec![0u8; size as usize];
        match self.block_on(read_projected(&resolved, offset, &mut buffer)) {
            Ok(read) => {
                buffer.truncate(read);
                reply.data(&buffer);
            }
            Err(err) => {
                lore_error!("vfs projected read failed for {relative}: {err}");
                reply.error(Errno::EIO);
            }
        }
    }
}

/// Thin async wrapper so the sync `read` callback has a single future to block on.
async fn read_projected(
    resolved: &ResolvedNode,
    offset: u64,
    buffer: &mut [u8],
) -> Result<usize, crate::immutable::ImmutableError> {
    core::read_range(resolved, offset, buffer).await
}

/// Serve a virtual workspace at `mountpoint`, blocking until it is unmounted. Matches the
/// call-site signature used by the clone flow.
pub fn serve(
    mountpoint: impl AsRef<Path>,
    repository: Arc<RepositoryContext>,
    state: Arc<State>,
    layer: Option<VirtualLayer>,
    prefetch: Option<&str>,
) {
    let backing_dir = std::env::var_os("LORE_VFS_BACKING").map(PathBuf::from);

    let fuse = LoreFuse::new(
        repository.clone(),
        state.clone(),
        layer,
        backing_dir,
        execution_context(),
    );

    if let Some(prefetch) = prefetch
        && let Ok(file) = std::fs::File::open(prefetch)
    {
        use std::io::BufRead;
        let paths: Vec<String> = std::io::BufReader::new(file)
            .lines()
            .map_while(Result::ok)
            .collect();
        lore_info!("Prefetching {} files", paths.len());
        let repository = repository.clone();
        let state = state.clone();
        lore_spawn!(async move {
            core::prefetch(repository, state, paths).await;
        });
    }

    lore_info!("Serving Lore FUSE mount at {}", mountpoint.as_ref().display());
    match fuse.run(mountpoint) {
        Ok(()) => lore_info!("Lore FUSE mount exited"),
        Err(err) => lore_error!("Lore FUSE mount failed: {err}"),
    }
}
