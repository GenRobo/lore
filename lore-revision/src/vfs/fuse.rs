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
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use fuser::BackgroundSession;
use fuser::BsdFileFlags;
use fuser::Errno;
use fuser::FileAttr;
use fuser::FileHandle;
use fuser::FileType;
use fuser::Filesystem;
use fuser::FopenFlags;
use fuser::Generation;
use fuser::INodeNo;
use fuser::KernelConfig;
use fuser::LockOwner;
use fuser::MountOption;
use fuser::SessionACL;
use fuser::OpenAccMode;
use fuser::OpenFlags;
use fuser::RenameFlags;
use fuser::ReplyAttr;
use fuser::ReplyCreate;
use fuser::ReplyData;
use fuser::ReplyDirectory;
use fuser::ReplyEmpty;
use fuser::ReplyEntry;
use fuser::ReplyOpen;
use fuser::ReplyWrite;
use fuser::Request;
use fuser::TimeOrNow;
use fuser::WriteFlags;
use parking_lot::Mutex;

use lore_base::lore_spawn;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::runtime::runtime;

use crate::file::dirty::ExplicitDirty;
use crate::interface::ExecutionContext;
use crate::lore::Hash;
use crate::lore::execution_context;
use crate::lore_error;
use crate::lore_info;
use crate::node::ROOT_NODE;
use crate::repository::RepositoryContext;
use crate::repository::clone::VirtualLayer;
use crate::state::State;
use crate::util::path::RelativePath;
use crate::util::path::RelativePathBuf;
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

/// State for a file opened for writing: the path it maps to, whether it has been modified (so the
/// node is reported dirty on release), and whether it was freshly created (already reported as an
/// add, so release must not overwrite that with a modify).
struct WriteHandle {
    relative: String,
    modified: bool,
    is_new: bool,
}

/// Kernel-level mount options for a virtual workspace. All default to off, which reproduces the
/// historical behaviour: a mount only its own uid can traverse. Hosts that serve a mount to other
/// uids (a container runtime, a CSI node driver) need `allow_other`.
#[derive(Debug, Clone, Copy, Default)]
pub struct VfsMountOptions {
    /// Let uids other than the mounting one access the mount (`allow_other`). For a non-root
    /// mounting user this additionally requires `user_allow_other` in `/etc/fuse.conf`.
    pub allow_other: bool,
    /// Let the kernel unmount when the serving process dies (`auto_unmount`), rather than leaving
    /// a mountpoint that returns `ENOTCONN`. FUSE requires `allow_other` (here, or via
    /// `user_allow_other` in `/etc/fuse.conf`) for this to be accepted.
    pub auto_unmount: bool,
    /// Mount read-only (`ro`), rejecting writes at the kernel boundary.
    pub read_only: bool,
}

/// A mounted Lore workspace served over FUSE.
pub struct LoreFuse {
    /// The projection layers. Held behind a lock so the served revision can be swapped when the
    /// branch anchor advances (reconciliation after sync); callbacks clone the inner `Arc` out
    /// of the lock rather than holding it across async work.
    layers: Arc<Mutex<Arc<Vec<VirtualLayer>>>>,
    /// Real directory that backs writes and whose contents pass through the mount (a git tree,
    /// materialized/written files). Writes copy up into this "upper" layer; when absent the
    /// mount is read-only.
    backing_dir: Option<PathBuf>,
    execution: Arc<ExecutionContext>,
    /// Revision timestamp (ms since Unix epoch) reported for projected entries. Computed
    /// lazily on first use from inside a callback, so construction never blocks on the async
    /// runtime (which would panic when `serve` is called from an async context).
    revision_ms: OnceLock<u64>,
    /// Revision currently being served; compared against the branch anchor to detect syncs.
    served_revision: Arc<Mutex<Hash>>,
    inodes: Mutex<InodeTable>,
    /// Open write handles keyed by file handle id.
    open_handles: Mutex<HashMap<u64, WriteHandle>>,
    /// Next file handle id to hand out for a write-opened file (0 is reserved for read-only).
    next_fh: AtomicU64,
    /// Repository-relative paths deleted this session. A projected entry with a whiteout is
    /// hidden so a delete is not undone by the projection reappearing. In-memory (per mount);
    /// deletes are also reported to Lore's dirty tracking, which persists them.
    whiteouts: Arc<Mutex<HashSet<String>>>,
    /// Set on unmount to stop the background reconcile poller.
    reconcile_stop: Arc<AtomicBool>,
    /// Kernel mount options applied when the mount is established.
    mount_options: VfsMountOptions,
}

/// How often the background poller checks whether the branch anchor has advanced.
const RECONCILE_POLL: Duration = Duration::from_secs(3);

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

        let served_revision = layers[0].state.revision();

        Self {
            layers: Arc::new(Mutex::new(Arc::new(layers))),
            backing_dir,
            execution,
            revision_ms: OnceLock::new(),
            served_revision: Arc::new(Mutex::new(served_revision)),
            inodes: Mutex::new(InodeTable::new()),
            open_handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
            whiteouts: Arc::new(Mutex::new(HashSet::new())),
            reconcile_stop: Arc::new(AtomicBool::new(false)),
            mount_options: VfsMountOptions::default(),
        }
    }

    /// Apply kernel mount options to the mount this backend will establish.
    #[must_use]
    pub fn with_mount_options(mut self, options: VfsMountOptions) -> Self {
        self.mount_options = options;
        self
    }

    /// Snapshot of the current projection layers (cloned out of the swap lock).
    fn current_layers(&self) -> Arc<Vec<VirtualLayer>> {
        self.layers.lock().clone()
    }

    /// The base repository module (layer 0).
    fn base_module(&self) -> Arc<RepositoryContext> {
        self.current_layers()[0].module.clone()
    }

    /// Revision timestamp reported for projected entries, computed once on first use.
    fn revision_ms(&self) -> u64 {
        *self
            .revision_ms
            .get_or_init(|| self.block_on(core::revision_timestamp_ms(&self.current_layers()[0])))
    }

    /// Mount in a background thread, returning a session handle that unmounts on drop.
    pub fn spawn(self, mountpoint: impl AsRef<Path>) -> std::io::Result<BackgroundSession> {
        let config = mount_config(self.mount_options);
        fuser::spawn_mount(self, mountpoint.as_ref(), &config)
    }

    /// Run the event loop on the current thread until the filesystem is unmounted.
    pub fn run(self, mountpoint: impl AsRef<Path>) -> std::io::Result<()> {
        let config = mount_config(self.mount_options);
        fuser::mount(self, mountpoint.as_ref(), &config)
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

    /// Current attributes for a path: the backing (overlay) file if present, else the projected
    /// node. Returns `None` when the path exists in neither.
    fn current_attr(&self, inode: u64, relative: &str) -> Option<FileAttr> {
        if let Some(backing) = self.backing_path(relative)
            && let Ok(metadata) = std::fs::symlink_metadata(&backing)
        {
            return Some(attr_from_metadata(inode, &metadata));
        }
        if self.is_whiteout(relative) {
            return None;
        }
        let resolved = self.block_on(core::resolve(&self.current_layers(), relative))?;
        Some(self.projected_attr(inode, resolved.kind(), resolved.size()))
    }

    /// Materialize a projected file into the backing (overlay) directory so writes can modify it
    /// in place. A no-op when the path is already present on the backing directory.
    fn copy_up(&self, relative: &str) -> std::io::Result<()> {
        let Some(backing) = self.backing_dir.as_ref() else {
            return Err(std::io::Error::other(
                "virtual mount is read-only (no backing directory)",
            ));
        };
        let target = backing.join(relative);
        if target.exists() {
            return Ok(());
        }
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        match self.block_on(core::resolve(&self.current_layers(), relative)) {
            Some(resolved) if resolved.kind() == EntryKind::Directory => {
                std::fs::create_dir_all(&target)?;
            }
            Some(resolved) => {
                let mut file = std::fs::File::create(&target)?;
                let size = resolved.size();
                let mut offset = 0u64;
                let mut buffer = vec![0u8; 1024 * 1024];
                while offset < size {
                    let read = self
                        .block_on(core::read_range(&resolved, offset, &mut buffer))
                        .map_err(|err| std::io::Error::other(err.to_string()))?;
                    if read == 0 {
                        break;
                    }
                    file.write_all(&buffer[..read])?;
                    offset += read as u64;
                }
            }
            None => {
                // No projected source (a fresh file): start empty.
                std::fs::File::create(&target)?;
            }
        }
        Ok(())
    }

    /// Report a change to `relative` to Lore's dirty tracking (caller-trusted; no filesystem
    /// re-stat, so it never re-enters the mount).
    fn report_dirty(&self, relative: &str, action: ExplicitDirty) {
        let Ok(path) = RelativePath::new_from_initial_path(relative) else {
            return;
        };
        let repository = self.base_module();
        if let Err(err) =
            self.block_on(crate::file::dirty::dirty_explicit(repository, vec![path], action))
        {
            lore_error!("vfs failed to mark {relative} dirty ({action:?}): {err}");
        }
    }

    /// Whether a path was deleted this session and should be hidden from projection.
    fn is_whiteout(&self, relative: &str) -> bool {
        self.whiteouts.lock().contains(relative)
    }

    /// Repository-relative path of `name` within the directory identified by `parent`.
    fn child_relative(&self, parent: INodeNo, name: &OsStr) -> Option<String> {
        let name = name.to_str()?;
        let table = self.inodes.lock();
        table.path(parent.0).map(|parent_path| join_path(parent_path, name))
    }

    /// Start a background thread that reconciles the mount to the branch anchor whenever it
    /// advances (e.g. after a sync), so a live mount picks up new revisions without a remount.
    fn spawn_reconcile_poller(&self) {
        let layers = self.layers.clone();
        let served = self.served_revision.clone();
        let whiteouts = self.whiteouts.clone();
        let execution = self.execution.clone();
        let repository = self.base_module();
        let stop = self.reconcile_stop.clone();
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                // Sleep in small steps so unmount stops the poller promptly.
                let mut waited = Duration::ZERO;
                while waited < RECONCILE_POLL {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    thread::sleep(Duration::from_millis(250));
                    waited += Duration::from_millis(250);
                }
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                runtime().block_on(LORE_CONTEXT.scope(
                    execution.clone(),
                    reconcile_to_anchor(&layers, &served, &whiteouts, repository.clone()),
                ));
            }
        });
    }
}

impl Drop for LoreFuse {
    fn drop(&mut self) {
        self.reconcile_stop.store(true, Ordering::Relaxed);
    }
}

/// Reload the served revision from the branch anchor if it has advanced, preserving overlay edits
/// (which live in the backing directory) and re-seeding whiteouts from the new staged deletes.
/// Overlay edits are kept as-is; a file that also changed in the new baseline keeps its overlay
/// content and stays dirty, so the divergence surfaces through the normal commit/merge path.
async fn reconcile_to_anchor(
    layers: &Arc<Mutex<Arc<Vec<VirtualLayer>>>>,
    served_revision: &Arc<Mutex<Hash>>,
    whiteouts: &Arc<Mutex<HashSet<String>>>,
    repository: Arc<RepositoryContext>,
) {
    let Ok((current, _staged, _branch)) =
        State::deserialize_current_and_staged(repository.clone()).await
    else {
        return;
    };
    let new_revision = current.revision();
    if new_revision.is_zero() {
        // An unset anchor means "no persisted revision", never "reconcile to empty".
        return;
    }
    if *served_revision.lock() == new_revision {
        return;
    }

    // Swap the base layer's state to the new revision (keeping any extra layers).
    {
        let mut guard = layers.lock();
        let mut updated = (**guard).clone();
        if let Some(base) = updated.first_mut() {
            base.state = current.clone();
        }
        *guard = Arc::new(updated);
    }
    *served_revision.lock() = new_revision;

    reseed_whiteouts(whiteouts, repository).await;

    lore_info!("VFS mount reconciled to revision {new_revision}");
}

/// Re-seed the whiteout set from the persisted staged state's dirty-delete nodes, so deletions
/// are hidden from the projection. Collects the paths (awaiting) before taking the lock, so the
/// lock is never held across an await point.
async fn reseed_whiteouts(
    whiteouts: &Arc<Mutex<HashSet<String>>>,
    repository: Arc<RepositoryContext>,
) {
    let Ok((_current, Some(staged), _branch)) =
        State::deserialize_current_and_staged(repository.clone()).await
    else {
        return;
    };
    let Ok(paths) = staged
        .collect_dirty_paths(repository.clone(), ROOT_NODE, RelativePathBuf::default())
        .await
    else {
        return;
    };
    let mut deleted = Vec::new();
    for path in paths {
        if let Ok(link) = staged.find_node_link(repository.clone(), path.as_str()).await
            && link.is_valid()
            && let Ok(node) = staged.node(repository.clone(), link.node).await
            && node.is_dirty_delete()
        {
            deleted.push(path.as_str().to_string());
        }
    }
    let mut whiteouts = whiteouts.lock();
    for path in deleted {
        whiteouts.insert(path);
    }
}

fn mount_config(options: VfsMountOptions) -> fuser::Config {
    let mut config = fuser::Config::default();
    let mut mount_options = vec![MountOption::FSName("lore".to_string())];
    if options.allow_other {
        // `allow_other` is expressed through the session ACL, not as a mount option: fuser emits
        // `-o allow_other` from it and gates `auto_unmount` on it being something other than
        // `Owner`.
        config.acl = SessionACL::All;
    }
    if options.auto_unmount {
        mount_options.push(MountOption::AutoUnmount);
    }
    if options.read_only {
        mount_options.push(MountOption::RO);
    }
    config.mount_options = mount_options;
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

/// Write `data` at `offset` into a backing file, returning the number of bytes written.
fn write_at(path: &Path, offset: u64, data: &[u8]) -> std::io::Result<usize> {
    let mut file = OpenOptions::new().write(true).open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(data)?;
    Ok(data.len())
}

fn entry_kind_to_file_type(kind: EntryKind) -> FileType {
    match kind {
        EntryKind::Directory => FileType::Directory,
        EntryKind::File => FileType::RegularFile,
    }
}

impl Filesystem for LoreFuse {
    fn init(&mut self, _req: &Request, _config: &mut KernelConfig) -> std::io::Result<()> {
        // `init` runs on the thread driving the mount, which for `run()`/`mount()` is already
        // inside a Tokio runtime — calling `block_on` here would panic. Re-seed whiteouts from
        // the persisted staged state on a scratch thread (so deletions survive a remount) and
        // join it so the seed completes before the first lookup.
        let whiteouts = self.whiteouts.clone();
        let execution = self.execution.clone();
        let repository = self.base_module();
        let _ = thread::spawn(move || {
            runtime().block_on(LORE_CONTEXT.scope(execution, reseed_whiteouts(&whiteouts, repository)));
        })
        .join();
        // Reconcile the served revision if the branch anchor advances while mounted.
        self.spawn_reconcile_poller();
        Ok(())
    }

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

        // A deleted path stays gone even though the projection still knows it.
        if self.is_whiteout(&relative) {
            reply.error(Errno::ENOENT);
            return;
        }

        match self.block_on(core::resolve(&self.current_layers(), &relative)) {
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

        match self.current_attr(ino.0, &relative) {
            Some(attr) => reply.attr(&TTL, &attr),
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

        // Projected entries fill in the rest, minus anything deleted this session.
        for entry in self.block_on(core::enumerate(&self.current_layers(), &relative)) {
            let child = join_path(&relative, &entry.name);
            if self.is_whiteout(&child) {
                continue;
            }
            if !seen.insert(entry.name.clone()) {
                continue;
            }
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

        if self.is_whiteout(&relative) {
            reply.error(Errno::ENOENT);
            return;
        }

        // Projected file: hydrate the requested window through the neutral core.
        let Some(resolved) = self.block_on(core::resolve(&self.current_layers(), &relative)) else {
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

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        // Read-only opens need no per-handle state; reuse the shared read path.
        if flags.acc_mode() == OpenAccMode::O_RDONLY {
            reply.opened(FileHandle(0), FopenFlags::empty());
            return;
        }
        if self.backing_dir.is_none() {
            reply.error(Errno::EROFS);
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
        // Materialize the file so writes modify it in place.
        if let Err(err) = self.copy_up(&relative) {
            lore_error!("vfs copy-up failed for {relative}: {err}");
            reply.error(Errno::EIO);
            return;
        }
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.open_handles.lock().insert(
            fh,
            WriteHandle {
                relative,
                modified: false,
                is_new: false,
            },
        );
        reply.opened(FileHandle(fh), FopenFlags::empty());
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let relative = {
            let handles = self.open_handles.lock();
            let Some(handle) = handles.get(&fh.0) else {
                reply.error(Errno::EBADF);
                return;
            };
            handle.relative.clone()
        };
        let Some(backing) = self.backing_dir.as_ref() else {
            reply.error(Errno::EROFS);
            return;
        };
        match write_at(&backing.join(&relative), offset, data) {
            Ok(written) => {
                if let Some(handle) = self.open_handles.lock().get_mut(&fh.0) {
                    handle.modified = true;
                }
                reply.written(written as u32);
            }
            Err(err) => {
                lore_error!("vfs write failed for {relative}: {err}");
                reply.error(Errno::EIO);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let relative = {
            let table = self.inodes.lock();
            let Some(path) = table.path(ino.0) else {
                reply.error(Errno::ENOENT);
                return;
            };
            path.to_string()
        };

        // Only truncation is materialized; other attribute changes are accepted without
        // persistence (the mount reports revision/overlay metadata). Truncation (including the
        // size=0 the kernel issues for O_TRUNC) copies the file up and resizes it in the overlay.
        if let Some(new_size) = size {
            let Some(backing) = self.backing_dir.as_ref() else {
                reply.error(Errno::EROFS);
                return;
            };
            if let Err(err) = self.copy_up(&relative) {
                lore_error!("vfs copy-up (truncate) failed for {relative}: {err}");
                reply.error(Errno::EIO);
                return;
            }
            match OpenOptions::new()
                .write(true)
                .open(backing.join(&relative))
                .and_then(|file| file.set_len(new_size))
            {
                Ok(()) => self.report_dirty(&relative, ExplicitDirty::Modify),
                Err(err) => {
                    lore_error!("vfs truncate failed for {relative}: {err}");
                    reply.error(Errno::EIO);
                    return;
                }
            }
        }

        match self.current_attr(ino.0, &relative) {
            Some(attr) => reply.attr(&TTL, &attr),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let handle = self.open_handles.lock().remove(&fh.0);
        if let Some(handle) = handle
            && handle.modified
            && !handle.is_new
        {
            // A newly created file was already reported as an add; don't downgrade it to a modify.
            self.report_dirty(&handle.relative, ExplicitDirty::Modify);
        }
        reply.ok();
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        reply.ok();
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let Some(relative) = self.child_relative(parent, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(backing) = self.backing_dir.as_ref() else {
            reply.error(Errno::EROFS);
            return;
        };
        let target = backing.join(&relative);
        if let Some(dir) = target.parent()
            && let Err(err) = std::fs::create_dir_all(dir)
        {
            lore_error!("vfs create (parent dirs) failed for {relative}: {err}");
            reply.error(Errno::EIO);
            return;
        }
        if let Err(err) = std::fs::File::create(&target) {
            lore_error!("vfs create failed for {relative}: {err}");
            reply.error(Errno::EIO);
            return;
        }
        self.whiteouts.lock().remove(&relative);
        self.report_dirty(&relative, ExplicitDirty::Add);

        let inode = self.inodes.lock().intern(&relative);
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.open_handles.lock().insert(
            fh,
            WriteHandle {
                relative: relative.clone(),
                modified: false,
                is_new: true,
            },
        );
        match self.current_attr(inode, &relative) {
            Some(attr) => {
                reply.created(&TTL, &attr, Generation(0), FileHandle(fh), FopenFlags::empty());
            }
            None => reply.error(Errno::EIO),
        }
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let Some(relative) = self.child_relative(parent, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(backing) = self.backing_dir.as_ref() else {
            reply.error(Errno::EROFS);
            return;
        };
        if let Err(err) = std::fs::create_dir_all(backing.join(&relative)) {
            lore_error!("vfs mkdir failed for {relative}: {err}");
            reply.error(Errno::EIO);
            return;
        }
        self.whiteouts.lock().remove(&relative);
        // A directory is not itself a tracked file node; files created under it carry the add.
        let inode = self.inodes.lock().intern(&relative);
        match self.current_attr(inode, &relative) {
            Some(attr) => reply.entry(&TTL, &attr, Generation(0)),
            None => reply.error(Errno::EIO),
        }
    }

    // The overlay is the mount's own scratch/upper layer (not repository-internal storage), so
    // these operate on it with raw filesystem calls; the tracked change is reported through the
    // write-token-gated `dirty_explicit`.
    #[allow(clippy::disallowed_methods)]
    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(relative) = self.child_relative(parent, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if let Some(backing) = self.backing_dir.as_ref() {
            let target = backing.join(&relative);
            if target.is_file()
                && let Err(err) = std::fs::remove_file(&target)
            {
                lore_error!("vfs unlink failed for {relative}: {err}");
                reply.error(Errno::EIO);
                return;
            }
        }
        // Hide the projected entry so the delete sticks, and report it.
        self.whiteouts.lock().insert(relative.clone());
        self.report_dirty(&relative, ExplicitDirty::Delete);
        reply.ok();
    }

    #[allow(clippy::disallowed_methods)] // overlay scratch write; see `unlink`
    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let Some(relative) = self.child_relative(parent, name) else {
            reply.error(Errno::ENOENT);
            return;
        };
        if let Some(backing) = self.backing_dir.as_ref() {
            let target = backing.join(&relative);
            if target.is_dir()
                && let Err(err) = std::fs::remove_dir(&target)
            {
                lore_error!("vfs rmdir failed for {relative}: {err}");
                reply.error(Errno::EIO);
                return;
            }
        }
        self.whiteouts.lock().insert(relative.clone());
        self.report_dirty(&relative, ExplicitDirty::Delete);
        reply.ok();
    }

    #[allow(clippy::disallowed_methods)] // overlay scratch write; see `unlink`
    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let (Some(from), Some(to)) = (
            self.child_relative(parent, name),
            self.child_relative(newparent, newname),
        ) else {
            reply.error(Errno::ENOENT);
            return;
        };
        let Some(backing) = self.backing_dir.as_ref() else {
            reply.error(Errno::EROFS);
            return;
        };
        // Materialize the source so there is a file to move.
        if let Err(err) = self.copy_up(&from) {
            lore_error!("vfs rename copy-up failed for {from}: {err}");
            reply.error(Errno::EIO);
            return;
        }
        let destination = backing.join(&to);
        if let Some(dir) = destination.parent()
            && let Err(err) = std::fs::create_dir_all(dir)
        {
            lore_error!("vfs rename (parent dirs) failed for {to}: {err}");
            reply.error(Errno::EIO);
            return;
        }
        if let Err(err) = std::fs::rename(backing.join(&from), &destination) {
            lore_error!("vfs rename failed for {from} -> {to}: {err}");
            reply.error(Errno::EIO);
            return;
        }
        {
            let mut whiteouts = self.whiteouts.lock();
            whiteouts.insert(from.clone());
            whiteouts.remove(&to);
        }
        // Tracked as a delete of the source path and an add of the destination.
        self.report_dirty(&from, ExplicitDirty::Delete);
        self.report_dirty(&to, ExplicitDirty::Add);
        reply.ok();
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

/// Serve a virtual workspace at `mountpoint`, blocking until it is unmounted. `backing_dir` is
/// the writable overlay/pass-through layer; when `None` it falls back to the `LORE_VFS_BACKING`
/// environment variable.
pub fn serve(
    mountpoint: impl AsRef<Path>,
    repository: Arc<RepositoryContext>,
    state: Arc<State>,
    layer: Option<VirtualLayer>,
    prefetch: Option<&str>,
    backing_dir: Option<PathBuf>,
    mount_options: VfsMountOptions,
) {
    let backing_dir =
        backing_dir.or_else(|| std::env::var_os("LORE_VFS_BACKING").map(PathBuf::from));

    let fuse = LoreFuse::new(
        repository.clone(),
        state.clone(),
        layer,
        backing_dir,
        execution_context(),
    )
    .with_mount_options(mount_options);

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
