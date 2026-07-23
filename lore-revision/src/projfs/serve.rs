// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::collections::HashSet;
use std::fs::File;
use std::io::BufRead;
use std::io::Read;
use std::io::Write;
#[cfg(target_os = "windows")]
use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use dashmap::DashMap;
use lore_base::lore_spawn;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::runtime::runtime;
use parking_lot::Mutex;
use tokio::io::AsyncReadExt;
use tokio::time::Instant;
use windows_sys::Win32;
use windows_sys::Win32::Storage::ProjectedFileSystem;

use crate::change::FileAction;
use crate::file::dirty::ExplicitDirty;
use crate::interface::ExecutionContext;
use crate::lore::Context;
use crate::lore::Hash;
use crate::lore::execution_context;
use crate::lore_debug;
use crate::lore_error;
use crate::lore_info;
use crate::repository::RepositoryContext;
use crate::repository::clone::VirtualLayer;
use crate::state::State;
use crate::util::path::RelativePath;
use crate::vfs::core;
use crate::vfs::core::EntryKind;
use crate::vfs::core::ResolvedNode;

const DOT_PROJFSID: &str = ".projfsid";

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialEq)]
struct Win32Error(u32);

impl Win32Error {
    pub fn from(error_code: i32) -> Self {
        Self(error_code as u32)
    }

    pub fn get_last_error() -> Self {
        Self(unsafe { Win32::Foundation::GetLastError() })
    }
}

impl std::fmt::Display for Win32Error {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let error_code = self.0;
        let mut buffer = Vec::with_capacity(1000);

        let length = unsafe {
            Win32::System::Diagnostics::Debug::FormatMessageW(
                Win32::System::Diagnostics::Debug::FORMAT_MESSAGE_FROM_SYSTEM
                    | Win32::System::Diagnostics::Debug::FORMAT_MESSAGE_IGNORE_INSERTS,
                std::ptr::null(),
                error_code,
                0,
                buffer.as_mut_ptr(),
                buffer.capacity() as u32,
                std::ptr::null_mut(),
            )
        } as usize;

        let message = if length == 0 {
            None
        } else {
            unsafe {
                buffer.set_len(length);
            }
            String::from_utf16(&buffer).ok()
        };

        if let Some(message) = message {
            write!(fmt, "{message} ({error_code:#08x})")
        } else {
            write!(fmt, "{error_code:#08x}")
        }
    }
}

struct InstanceContext {
    execution: std::sync::Arc<ExecutionContext>,
    instance: ProjectedFileSystem::PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
    instance_info: ProjectedFileSystem::PRJ_VIRTUALIZATION_INSTANCE_INFO,
    /// The projection layers. Held behind a lock so the served revision can be swapped when the
    /// branch anchor advances (reconciliation after sync); callbacks clone the inner `Arc` out
    /// of the lock rather than holding it across async work.
    layers: Arc<Mutex<Arc<Vec<VirtualLayer>>>>,
    entry_map: DashMap<Context, EnumerationInstance>,
    file_log: Option<Mutex<File>>,
    /// Files reported as an add when created whose creating handle has not yet closed. Used to
    /// avoid downgrading the add to a modify when that handle closes, matching the FUSE
    /// backend's per-handle `is_new` guard.
    created_pending: Mutex<HashSet<String>>,
    /// Repository-relative paths with a staged delete, hidden from the projection so a delete
    /// performed outside this mount (CLI, sync from elsewhere) is not undone by the projected
    /// entry reappearing. Local deletes through the mount are additionally tombstoned on disk
    /// by ProjFS itself, which is what persists them across remounts.
    hidden: Arc<Mutex<HashSet<String>>>,
}

impl InstanceContext {
    /// Snapshot of the current projection layers (cloned out of the swap lock).
    fn current_layers(&self) -> Arc<Vec<VirtualLayer>> {
        self.layers.lock().clone()
    }

    /// The base repository module (layer 0).
    fn base_module(&self) -> Arc<RepositoryContext> {
        self.current_layers()[0].module.clone()
    }

    /// Whether a repository-relative path has a staged delete and is hidden from projection.
    fn is_hidden(&self, relative: &str) -> bool {
        self.hidden.lock().contains(relative)
    }
}

struct EnumerationEntry {
    entry: core::DirEntry,
    file_name: Vec<u16>,
}

struct EnumerationInstance {
    search: String,
    search_wide: Vec<u16>,
    base_path: String,
    file: Vec<EnumerationEntry>,
    last_index: Option<usize>,
    capture_search: bool,
    done: bool,
}

#[cfg(not(target_os = "windows"))]
fn is_supported() -> bool {
    false
}

#[cfg(target_os = "windows")]
fn is_supported() -> bool {
    true
}

pub fn serve(
    path: impl AsRef<Path>,
    repository: Arc<RepositoryContext>,
    state: Arc<State>,
    layer: Option<VirtualLayer>,
    prefetch: Option<&str>,
) {
    if !is_supported() {
        lore_error!("ProjectedFS not supported");
        return;
    }

    let file_log = if let Ok(value) = std::env::var("LORE_VIRTUAL_FILE_LOG") {
        if value == "1" && prefetch.is_none() {
            std::fs::OpenOptions::new()
                .write(true)
                .read(true)
                .truncate(true)
                .create(true)
                .open("file_access.log")
                .map(|file| Mutex::new(file))
                .ok()
        } else {
            None
        }
    } else {
        None
    };

    let prefetch = if let Some(prefetch) = prefetch {
        if let Ok(prefetch) = std::fs::OpenOptions::new()
            .write(false)
            .read(true)
            .truncate(false)
            .create(false)
            .open(prefetch)
        {
            let lines: Vec<String> = std::io::BufReader::new(prefetch)
                .lines()
                .filter_map(|res| res.ok())
                .collect();
            Some(lines)
        } else {
            None
        }
    } else {
        None
    };

    let repository_path = match repository.require_path() {
        Ok(repository_path) => repository_path,
        Err(err) => {
            lore_error!("Cannot serve ProjectedFS: {err}");
            return;
        }
    };
    if let Err(err) = std::env::set_current_dir(repository_path) {
        lore_error!("Failed to set repository path as current working dir: {err}");
        return;
    }

    let id_path = path
        .as_ref()
        .join(crate::repository::RepositoryFormat::detect(path.as_ref()).dot_dir())
        .join(DOT_PROJFSID);
    let mut uuid = uuid::Uuid::nil();
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .truncate(false)
        .read(true)
        .write(false)
        .create(false)
        .open(id_path.as_path())
    {
        let mut data = [0; 16];
        if let Ok(numread) = file.read(&mut data) {
            if numread == data.len() {
                uuid = uuid::Uuid::from_bytes(data);
            }
        }
    }

    let pathref = path.as_ref();
    let mut path: Vec<u16> = {
        #[cfg(not(target_os = "windows"))]
        {
            pathref
                .as_os_str()
                .to_string_lossy()
                .encode_utf16()
                .collect()
        }
        #[cfg(target_os = "windows")]
        {
            pathref.as_os_str().encode_wide().collect()
        }
    };
    path.push(0);

    if uuid.is_nil() {
        uuid = uuid::Uuid::now_v7();

        // Write the instance id under the dot directory before marking the root: once the root
        // is a placeholder, creating new entries beneath it requires a running provider (a
        // fresh `lore mount` mountpoint has no dot directory yet).
        if let Some(parent) = id_path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let id_write = std::fs::OpenOptions::new()
            .truncate(true)
            .read(true)
            .write(true)
            .create(true)
            .open(id_path.as_path())
            .and_then(|mut file| file.write_all(uuid.as_bytes()));
        if let Err(err) = id_write {
            lore_error!("Failed to write ProjectedFS UUID to file: {err}");
            return;
        }

        // Safety: Win32 API call
        let res = unsafe {
            ProjectedFileSystem::PrjMarkDirectoryAsPlaceholder(
                path.as_ptr(),
                std::ptr::null(),
                std::ptr::null(),
                (&raw const uuid).cast::<windows_sys::core::GUID>(),
            )
        };
        if res != 0 {
            // The id only describes a successfully marked root; a stale one would make the
            // next serve skip the marking and fail to start.
            // Removing a file this module just created; not repository-tracked content.
            #[allow(clippy::disallowed_methods)]
            let _ = std::fs::remove_file(id_path.as_path());
            lore_error!(
                "Failed to mark directory as ProjectedFS placeholder: {}",
                Win32Error::from(res)
            );
            return;
        }
    }

    let callback_table = ProjectedFileSystem::PRJ_CALLBACKS {
        StartDirectoryEnumerationCallback: Some(start_directory_enumeration),
        EndDirectoryEnumerationCallback: Some(end_directory_enumeration),
        GetDirectoryEnumerationCallback: Some(get_directory_enumeration),
        GetPlaceholderInfoCallback: Some(get_placeholder_info),
        GetFileDataCallback: Some(get_file_data),
        QueryFileNameCallback: Some(query_file_name),
        NotificationCallback: Some(notification),
        CancelCommandCallback: None,
    };

    // Watch the whole virtualization root for the write operations Lore tracks. The
    // no-modification close is subscribed only to retire `created_pending` bookkeeping.
    let notification_root: Vec<u16> = vec![0];
    let mut notification_mappings = [ProjectedFileSystem::PRJ_NOTIFICATION_MAPPING {
        NotificationBitMask: ProjectedFileSystem::PRJ_NOTIFY_NEW_FILE_CREATED
            | ProjectedFileSystem::PRJ_NOTIFY_FILE_OVERWRITTEN
            | ProjectedFileSystem::PRJ_NOTIFY_FILE_RENAMED
            | ProjectedFileSystem::PRJ_NOTIFY_FILE_HANDLE_CLOSED_NO_MODIFICATION
            | ProjectedFileSystem::PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_MODIFIED
            | ProjectedFileSystem::PRJ_NOTIFY_FILE_HANDLE_CLOSED_FILE_DELETED,
        NotificationRoot: notification_root.as_ptr(),
    }];

    let capped_core_count = std::cmp::min(
        32,
        std::cmp::max(
            std::thread::available_parallelism()
                .map(|count| count.get())
                .unwrap_or(8),
            8,
        ),
    );
    let instance_options = ProjectedFileSystem::PRJ_STARTVIRTUALIZING_OPTIONS {
        Flags: 0,
        PoolThreadCount: capped_core_count as u32,
        ConcurrentThreadCount: capped_core_count as u32,
        NotificationMappings: notification_mappings.as_mut_ptr(),
        NotificationMappingsCount: notification_mappings.len() as u32,
    };

    let mut layers = vec![VirtualLayer {
        module: repository.clone(),
        module_path: RelativePath::default(),
        layer_path: RelativePath::default(),
        state: state.clone(),
    }];
    if let Some(layer) = layer {
        layers.push(layer);
    }

    // Seed the hidden set from the persisted staged deletes so deletions staged outside this
    // mount stay gone. Runs on a scratch thread: `serve` may be called from inside the async
    // runtime (the clone path), where a direct `block_on` would panic.
    let hidden = Arc::new(Mutex::new(HashSet::new()));
    {
        let hidden = hidden.clone();
        let execution = execution_context();
        let seed_repository = repository.clone();
        let _ = std::thread::spawn(move || {
            runtime().block_on(LORE_CONTEXT.scope(execution, async move {
                let deleted = core::staged_delete_paths(seed_repository).await;
                hidden.lock().extend(deleted);
            }));
        })
        .join();
    }

    let mut instance_context = InstanceContext {
        execution: execution_context(),
        instance: std::ptr::null_mut(),
        instance_info: ProjectedFileSystem::PRJ_VIRTUALIZATION_INSTANCE_INFO {
            InstanceID: windows_sys::core::GUID::from_u128(0),
            WriteAlignment: 0,
        },
        layers: Arc::new(Mutex::new(Arc::new(layers))),
        entry_map: DashMap::default(),
        file_log,
        created_pending: Mutex::new(HashSet::new()),
        hidden,
    };

    // Created before starting virtualization so an `lore unmount` racing the mount startup can
    // still find the event.
    let stop_event_name = stop_event_name(pathref);
    // Safety: Win32 API call with a valid, null-terminated name
    let stop_event = unsafe {
        Win32::System::Threading::CreateEventW(
            std::ptr::null(),
            1, // manual reset: every waiter sees the signal
            0,
            stop_event_name.as_ptr(),
        )
    };

    // Safety: Win32 API call, all passed in raw pointers are valid
    let res = unsafe {
        ProjectedFileSystem::PrjStartVirtualizing(
            path.as_ptr(),
            &callback_table,
            (&raw const instance_context).cast(),
            (&raw const instance_options).cast(),
            (&raw mut instance_context.instance).cast(),
        )
    };
    if res != 0 {
        lore_error!(
            "Failed to start ProjectedFS virtualization: {}",
            Win32Error::from(res)
        );
        return;
    }

    // Safety: Win32 API call, pointers are valid
    unsafe {
        ProjectedFileSystem::PrjGetVirtualizationInstanceInfo(
            instance_context.instance,
            &raw mut instance_context.instance_info,
        );
    }

    lore_info!("Started ProjectedFS service for {}", pathref.display());

    if let Some(prefetch) = prefetch {
        let prefetch_count = prefetch.len();
        lore_info!("Start prefetching {prefetch_count} files");
        lore_spawn!(prefetch_files(repository.clone(), state.clone(), prefetch));
    }

    // Reconcile the served revision if the branch anchor advances while mounted.
    let poller_stop = Arc::new(AtomicBool::new(false));
    let poller = spawn_reconcile_poller(
        InstanceHandle(instance_context.instance),
        instance_context.layers.clone(),
        instance_context.hidden.clone(),
        instance_context.execution.clone(),
        repository.clone(),
        poller_stop.clone(),
    );

    // Block until an unmount signals the per-mountpoint stop event.
    if stop_event.is_null() {
        lore_error!(
            "Failed to create unmount event ({}); mount runs until the process exits",
            Win32Error::get_last_error()
        );
        loop {
            std::thread::sleep(Duration::from_secs(1));
        }
    }
    loop {
        // Safety: Win32 wait on the event created above
        let wait = unsafe { Win32::System::Threading::WaitForSingleObject(stop_event, 1000) };
        match wait {
            Win32::Foundation::WAIT_OBJECT_0 => break,
            Win32::Foundation::WAIT_TIMEOUT => {}
            _ => std::thread::sleep(Duration::from_secs(1)),
        }
    }

    lore_info!("Stopping ProjectedFS service for {}", pathref.display());
    poller_stop.store(true, Ordering::Relaxed);
    let _ = poller.join();
    // Safety: Win32 API calls; the instance was started and the event created above
    unsafe {
        ProjectedFileSystem::PrjStopVirtualizing(instance_context.instance);
        Win32::Foundation::CloseHandle(stop_event);
    }
}

/// Name of the per-mountpoint Win32 event `unmount` uses to signal the serving process.
fn stop_event_name(mountpoint: &Path) -> Vec<u16> {
    let canonical =
        std::fs::canonicalize(mountpoint).unwrap_or_else(|_| mountpoint.to_path_buf());
    let key = canonical.to_string_lossy().to_lowercase();
    let hash = xxhash_rust::xxh3::xxh3_64(key.as_bytes());
    let mut wide: Vec<u16> = format!("Local\\LoreVfsStop-{hash:016x}").encode_utf16().collect();
    wide.push(0);
    wide
}

/// Whether some process is currently serving a ProjFS mount at `mountpoint`: the serving
/// process holds the mount's named stop event open for the mount's lifetime.
pub fn is_serving(mountpoint: impl AsRef<Path>) -> bool {
    let name = stop_event_name(mountpoint.as_ref());
    // Safety: Win32 API calls with a valid, null-terminated name; the handle is closed
    unsafe {
        let event = Win32::System::Threading::OpenEventW(
            Win32::System::Threading::EVENT_MODIFY_STATE,
            0,
            name.as_ptr(),
        );
        if event.is_null() {
            return false;
        }
        Win32::Foundation::CloseHandle(event);
        true
    }
}

/// Signal the process serving a ProjFS mount at `mountpoint` to stop virtualizing and return
/// (the Windows counterpart of `fusermount3 -u`).
pub fn unmount(mountpoint: impl AsRef<Path>) -> Result<(), String> {
    let name = stop_event_name(mountpoint.as_ref());
    // Safety: Win32 API calls with a valid, null-terminated name; the handle is closed below
    unsafe {
        let event = Win32::System::Threading::OpenEventW(
            Win32::System::Threading::EVENT_MODIFY_STATE,
            0,
            name.as_ptr(),
        );
        if event.is_null() {
            return Err(format!(
                "no Lore virtual mount found at {}",
                mountpoint.as_ref().display()
            ));
        }
        let res = Win32::System::Threading::SetEvent(event);
        Win32::Foundation::CloseHandle(event);
        if res == 0 {
            return Err(format!(
                "failed to signal mount at {}: {}",
                mountpoint.as_ref().display(),
                Win32Error::get_last_error()
            ));
        }
    }
    Ok(())
}

/// How often the background poller checks whether the branch anchor has advanced.
const RECONCILE_POLL: Duration = Duration::from_secs(3);

/// ProjFS instance handle usable from the reconcile thread. The handle is an opaque token
/// ProjFS accepts from any thread (callbacks already arrive on a thread pool); it is only
/// dereferenced by ProjFS itself.
#[derive(Clone, Copy)]
struct InstanceHandle(ProjectedFileSystem::PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT);

// Safety: See [`InstanceHandle`] — the raw pointer is an opaque, thread-safe ProjFS token.
unsafe impl Send for InstanceHandle {}

/// Start a background thread that reconciles the mount to the branch anchor whenever it
/// advances (e.g. after a sync), so a live mount picks up new revisions without a remount.
fn spawn_reconcile_poller(
    instance: InstanceHandle,
    layers: Arc<Mutex<Arc<Vec<VirtualLayer>>>>,
    hidden: Arc<Mutex<HashSet<String>>>,
    execution: Arc<ExecutionContext>,
    repository: Arc<RepositoryContext>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            // Sleep in small steps so unmount stops the poller promptly.
            let mut waited = Duration::ZERO;
            while waited < RECONCILE_POLL {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(250));
                waited += Duration::from_millis(250);
            }
            runtime().block_on(LORE_CONTEXT.scope(
                execution.clone(),
                reconcile_to_anchor(instance, &layers, &hidden, repository.clone()),
            ));
        }
    })
}

/// Reload the served revision from the branch anchor if it has advanced. Because ProjFS caches
/// placeholder metadata on disk (unlike FUSE, which serves every request live), the revision
/// diff is walked afterwards to update or remove stale placeholders. Updates and deletes use
/// `PRJ_UPDATE_NONE`, so a locally modified (dirty) file always keeps its local content and
/// stays dirty — the same "overlay wins" behavior as the FUSE backend — and the divergence
/// surfaces through the normal commit/merge path.
async fn reconcile_to_anchor(
    instance: InstanceHandle,
    layers: &Arc<Mutex<Arc<Vec<VirtualLayer>>>>,
    hidden: &Arc<Mutex<HashSet<String>>>,
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
    let old_state = {
        let guard = layers.lock();
        let old = guard[0].state.clone();
        if old.revision() == new_revision {
            return;
        }
        old
    };

    // Swap the base layer's state to the new revision (keeping any extra layers).
    {
        let mut guard = layers.lock();
        let mut updated = (**guard).clone();
        if let Some(base) = updated.first_mut() {
            base.state = current.clone();
        }
        *guard = Arc::new(updated);
    }

    // Deletions staged against the new anchor stay hidden from the projection.
    let deleted = core::staged_delete_paths(repository.clone()).await;
    hidden.lock().extend(deleted);

    // Walk the changes between the previously served and the new revision, refreshing the
    // on-disk placeholders ProjFS has already materialized.
    let (tx, mut rx) = tokio::sync::mpsc::channel(256);
    let diff_repository = repository.clone();
    let diff_old = old_state.clone();
    let diff_new = current.clone();
    let walker = lore_spawn!(async move {
        crate::diff::diff_revision_paths(diff_repository, diff_old, diff_new, None, tx).await
    });

    let snapshot = layers.lock().clone();
    while let Some(change) = rx.recv().await {
        let Ok(change) = change else {
            continue;
        };
        match change.action {
            FileAction::Delete => {
                update_placeholder(instance, &snapshot, change.path.as_str(), true).await;
            }
            FileAction::Move => {
                if let Some(from) = change.from_path.as_ref() {
                    update_placeholder(instance, &snapshot, from.as_str(), true).await;
                }
                update_placeholder(instance, &snapshot, change.path.as_str(), false).await;
            }
            _ => {
                update_placeholder(instance, &snapshot, change.path.as_str(), false).await;
            }
        }
    }
    if let Ok(Err(err)) = walker.await {
        lore_error!("VFS reconcile diff walk failed: {err}");
    }

    lore_info!("VFS mount reconciled to revision {new_revision}");
}

/// Refresh a single on-disk placeholder for the new revision: update its metadata when the
/// path still exists, remove it when the new revision deleted it. Both are no-ops for paths
/// ProjFS has never materialized, and both leave locally modified files untouched
/// (`PRJ_UPDATE_NONE`).
async fn update_placeholder(
    instance: InstanceHandle,
    layers: &Arc<Vec<VirtualLayer>>,
    relative: &str,
    deleted: bool,
) {
    let mut wide: Vec<u16> = relative.replace('/', "\\").encode_utf16().collect();
    wide.push(0);
    let mut failure = ProjectedFileSystem::PRJ_UPDATE_FAILURE_CAUSE_NONE;

    if deleted {
        // Safety: Win32 API call; the instance and path are valid
        let res = unsafe {
            ProjectedFileSystem::PrjDeleteFile(
                instance.0,
                wide.as_ptr(),
                ProjectedFileSystem::PRJ_UPDATE_NONE,
                &mut failure,
            )
        };
        if res != 0 {
            lore_debug!(
                "Reconcile: keeping local {relative} (delete failed: {}, cause {failure})",
                Win32Error::from(res)
            );
        }
        return;
    }

    let Some(resolved) = core::resolve(layers, relative).await else {
        return;
    };
    let placeholder_info = placeholder_info_for(&resolved).await;

    // Safety: Win32 API call; the instance, path, and placeholder info are valid
    let res = unsafe {
        ProjectedFileSystem::PrjUpdateFileIfNeeded(
            instance.0,
            wide.as_ptr(),
            &placeholder_info,
            std::mem::size_of::<ProjectedFileSystem::PRJ_PLACEHOLDER_INFO>() as u32,
            ProjectedFileSystem::PRJ_UPDATE_NONE,
            &mut failure,
        )
    };
    if res != 0 {
        lore_debug!(
            "Reconcile: keeping local {relative} (update failed: {}, cause {failure})",
            Win32Error::from(res)
        );
    }
}

unsafe fn wcslen(str: *const u16) -> usize {
    if str.is_null() {
        return 0;
    }

    // Safety: The given pointer is guaranteed to be zero terminated
    unsafe {
        let mut len = 0;
        while *str.wrapping_add(len) != 0 {
            len += 1;
        }

        len
    }
}

/// Convert milliseconds since Unix Epoch to MS filetime, which is
/// the count of 100‑nanosecond intervals since 1601‑01‑01T00:00:00Z
fn ms_filetime(ms: u64) -> u64 {
    const MS_OFFSET_TIME: u64 = 116444736000000000;
    (ms * 10_000) + MS_OFFSET_TIME
}

fn instance_context(cbdata: &*const ProjectedFileSystem::PRJ_CALLBACK_DATA) -> &InstanceContext {
    // Safety: This is guaranteed to be the existing object we passed it an instance creation
    unsafe { &*(**cbdata).InstanceContext.cast::<InstanceContext>() }
}

fn context_from_guid(guid: *const windows_sys::core::GUID) -> Context {
    // Safety: Only valid pointers from ProjFS are passed to this function.
    // GUID and Context are the same binary size and just raw data.
    unsafe { std::mem::transmute_copy::<windows_sys::core::GUID, Context>(&*guid) }
}

/// Join a repository-relative parent path with a child name.
fn join_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_string()
    } else {
        format!("{parent}/{name}")
    }
}

/// Whether a repository-relative path is repository-internal (inside the `.urc`/`.lore` dot
/// directory). Unlike FUSE, where the repository directory lives outside the mount, the ProjFS
/// virtualization root can be the repository directory itself (`clone --virtually`), so Lore's
/// own writes to the dot directory raise notifications and must not be tracked as workspace
/// changes.
fn is_internal_path(path: &str) -> bool {
    let first = path.split('/').next().unwrap_or(path);
    first.eq_ignore_ascii_case(crate::repository::DOT_URC)
        || first.eq_ignore_ascii_case(crate::repository::DOT_LORE)
}

/// Report a change to `relative` to Lore's dirty tracking (caller-trusted; no filesystem
/// re-stat, so it never re-enters the virtualization root).
fn report_dirty(instance_context: &InstanceContext, relative: &str, action: ExplicitDirty) {
    let Ok(path) = RelativePath::new_from_initial_path(relative) else {
        return;
    };
    let repository = instance_context.base_module();
    let result = runtime().block_on(LORE_CONTEXT.scope(
        instance_context.execution.clone(),
        crate::file::dirty::dirty_explicit(repository, vec![path], action),
    ));
    if let Err(err) = result {
        lore_error!("vfs failed to mark {relative} dirty ({action:?}): {err}");
    }
}

/// Write notifications from ProjFS, mapped to the same dirty reports the FUSE backend makes so
/// Lore integration behaves identically on both platforms: a created file is an add, a handle
/// closed after modification is a modify (unless it is the creating handle, which already
/// carried the add), a delete-on-close is a delete, and a rename is a delete of the source plus
/// an add of the destination.
///
/// ProjFS suppresses notifications for I/O performed by the provider process itself (by
/// design, to avoid recursion), so only external processes' edits arrive here — which is the
/// desired tracking behavior, and means the serving process's own writes (staging,
/// materialization) never loop back as workspace changes.
unsafe extern "system" fn notification(
    cbdata: *const ProjectedFileSystem::PRJ_CALLBACK_DATA,
    is_directory: bool,
    notification: ProjectedFileSystem::PRJ_NOTIFICATION,
    destination_file_name: windows_sys::core::PCWSTR,
    _operation_parameters: *mut ProjectedFileSystem::PRJ_NOTIFICATION_PARAMETERS,
) -> i32 {
    let instance_context = instance_context(&cbdata);

    // Safety: Guaranteed by ProjectedFS API to be valid
    let path = String::from_utf16_lossy(unsafe {
        slice::from_raw_parts((*cbdata).FilePathName, wcslen((*cbdata).FilePathName))
    })
    .replace('\\', "/");

    if is_internal_path(&path) && notification != ProjectedFileSystem::PRJ_NOTIFICATION_FILE_RENAMED
    {
        return 0;
    }

    match notification {
        ProjectedFileSystem::PRJ_NOTIFICATION_NEW_FILE_CREATED => {
            instance_context.hidden.lock().remove(&path);
            // A directory is not itself a tracked file node; files created under it carry the
            // add (parity with the FUSE backend's mkdir).
            if !is_directory {
                instance_context.created_pending.lock().insert(path.clone());
                report_dirty(instance_context, &path, ExplicitDirty::Add);
            }
        }
        ProjectedFileSystem::PRJ_NOTIFICATION_FILE_OVERWRITTEN => {
            report_dirty(instance_context, &path, ExplicitDirty::Modify);
        }
        ProjectedFileSystem::PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION => {
            instance_context.created_pending.lock().remove(&path);
        }
        ProjectedFileSystem::PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED => {
            // A newly created file was already reported as an add; don't downgrade it to a
            // modify when the creating handle closes.
            if !instance_context.created_pending.lock().remove(&path) {
                report_dirty(instance_context, &path, ExplicitDirty::Modify);
            }
        }
        ProjectedFileSystem::PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED => {
            instance_context.created_pending.lock().remove(&path);
            instance_context.hidden.lock().insert(path.clone());
            report_dirty(instance_context, &path, ExplicitDirty::Delete);
        }
        ProjectedFileSystem::PRJ_NOTIFICATION_FILE_RENAMED => {
            // Safety: Guaranteed by ProjectedFS API to be valid (empty when the rename crosses
            // the virtualization root boundary)
            let destination = String::from_utf16_lossy(unsafe {
                slice::from_raw_parts(destination_file_name, wcslen(destination_file_name))
            })
            .replace('\\', "/");

            // Tracked as a delete of the source path and an add of the destination. Either
            // side may be empty (rename across the virtualization root boundary) or
            // repository-internal; only the tracked sides are reported.
            if !path.is_empty() && !is_internal_path(&path) {
                instance_context.created_pending.lock().remove(&path);
                instance_context.hidden.lock().insert(path.clone());
                report_dirty(instance_context, &path, ExplicitDirty::Delete);
            }
            if !destination.is_empty() && !is_internal_path(&destination) {
                instance_context.hidden.lock().remove(&destination);
                report_dirty(instance_context, &destination, ExplicitDirty::Add);
            }
        }
        _ => {}
    }

    0
}

unsafe extern "system" fn start_directory_enumeration(
    cbdata: *const ProjectedFileSystem::PRJ_CALLBACK_DATA,
    enumeration_id: *const windows_sys::core::GUID,
) -> i32 {
    // Safety: Guaranteed by ProjectedFS API to be valid
    let path: &[u16] =
        unsafe { slice::from_raw_parts((*cbdata).FilePathName, wcslen((*cbdata).FilePathName)) };
    let path = String::from_utf16_lossy(path);

    let instance_context = instance_context(&cbdata);

    lore_debug!("Start enumeration: {path}");

    let enum_instance = EnumerationInstance {
        search: String::default(),
        search_wide: Vec::default(),
        base_path: String::default(),
        file: Vec::with_capacity(256),
        last_index: None,
        capture_search: true,
        done: false,
    };

    let context = context_from_guid(enumeration_id);
    instance_context.entry_map.insert(context, enum_instance);

    0
}

unsafe extern "system" fn end_directory_enumeration(
    cbdata: *const ProjectedFileSystem::PRJ_CALLBACK_DATA,
    enumeration_id: *const windows_sys::core::GUID,
) -> i32 {
    let instance_context = instance_context(&cbdata);
    let context = context_from_guid(enumeration_id);
    instance_context.entry_map.remove(&context);
    0
}

unsafe extern "system" fn get_directory_enumeration(
    cbdata: *const ProjectedFileSystem::PRJ_CALLBACK_DATA,
    enumeration_id: *const windows_sys::core::GUID,
    search_expression: *const u16,
    dir_entry_buffer_handle: *mut std::ffi::c_void,
) -> i32 {
    let instance_context = instance_context(&cbdata);
    let context = context_from_guid(enumeration_id);
    let Some(mut enum_instance) = instance_context.entry_map.get_mut(&context) else {
        return 0;
    };

    match runtime().block_on(
        LORE_CONTEXT.scope(instance_context.execution.clone(), unsafe {
            get_directory_enumeration_async(
                instance_context,
                enum_instance.value_mut(),
                &*cbdata,
                search_expression,
                dir_entry_buffer_handle,
            )
        }),
    ) {
        Ok(()) => 0,
        Err(code) => code,
    }
}

async unsafe fn get_directory_enumeration_async(
    instance_context: &InstanceContext,
    enum_instance: &mut EnumerationInstance,
    cbdata: &ProjectedFileSystem::PRJ_CALLBACK_DATA,
    search_expression: *const u16,
    dir_entry_buffer_handle: *mut std::ffi::c_void,
) -> Result<(), i32> {
    if cbdata.Flags & ProjectedFileSystem::PRJ_CB_DATA_FLAG_ENUM_RESTART_SCAN != 0 {
        enum_instance.done = false;
        enum_instance.capture_search = true;
        enum_instance.last_index = None;
        lore_debug!(
            "Get enumeration: RESTART {} ({:x})",
            String::from_utf16_lossy(unsafe {
                slice::from_raw_parts(cbdata.FilePathName, wcslen(cbdata.FilePathName))
            }),
            cbdata.Flags
        );
    } else {
        lore_debug!(
            "Get enumeration: CONTINUE {} ({:x})",
            String::from_utf16_lossy(unsafe {
                slice::from_raw_parts(cbdata.FilePathName, wcslen(cbdata.FilePathName))
            }),
            cbdata.Flags
        );
    }

    if enum_instance.capture_search {
        enum_instance.capture_search = false;
        if search_expression.is_null() || unsafe { search_expression.read() } == 0 {
            lore_debug!("Get enumeration: search <none>");
            enum_instance.search.clear();
        } else {
            enum_instance.search_wide =
                unsafe { slice::from_raw_parts(search_expression, wcslen(search_expression)) }
                    .to_vec();
            enum_instance.search = String::from_utf16_lossy(&enum_instance.search_wide);
            enum_instance.search_wide.push(0);
            lore_debug!("Get enumeration: search {}", enum_instance.search);
        }

        enum_instance.file.clear();

        let file_path = String::from_utf16_lossy(unsafe {
            slice::from_raw_parts(cbdata.FilePathName, wcslen(cbdata.FilePathName))
        });

        let layers = instance_context.current_layers();
        let module_path = layers[0]
            .module
            .require_path()
            .map_err(|_| ERROR_FILE_NOT_FOUND)?;

        // Repository-relative path of the directory being enumerated.
        let mut relative_path =
            RelativePath::new_from_user_path(module_path, file_path.as_str()).unwrap_or_default();

        enum_instance.base_path = String::default();

        if enum_instance.search == "*" || enum_instance.search == "/" {
            // Grab all files
            enum_instance.search_wide.clear();
            enum_instance.search.clear();
        }

        if let Some(sep) = enum_instance.search.rfind('/') {
            if sep > 0 {
                let (directory_path, search) = enum_instance.search.split_at(sep);

                relative_path = RelativePath::new_from_user_path(module_path, directory_path)
                    .unwrap_or_default();

                let search = search.to_string();
                let directory_path = directory_path.to_string();

                lore_debug!("Search: {search}");

                enum_instance.search_wide = search.encode_utf16().collect();
                enum_instance.search_wide.push(0);

                enum_instance.search = search;
                enum_instance.base_path = directory_path;
            }
        }

        // TODO(vri): UCS-19230 - Links: Handle link nodes in ProjFS directory enumeration and find
        enum_instance.file = core::enumerate(&layers, relative_path.as_str())
            .await
            .into_iter()
            .filter(|entry| {
                // Entries with a staged delete stay hidden from the projection.
                !instance_context.is_hidden(&join_path(relative_path.as_str(), &entry.name))
            })
            .map(|entry| {
                let mut file_name: Vec<u16> = entry.name.encode_utf16().collect();
                file_name.push(0);
                EnumerationEntry { entry, file_name }
            })
            .collect();

        enum_instance.file.sort_unstable_by(|lhs, rhs| unsafe {
            let order = ProjectedFileSystem::PrjFileNameCompare(
                lhs.file_name.as_ptr(),
                rhs.file_name.as_ptr(),
            );
            if order < 0 {
                std::cmp::Ordering::Less
            } else if order > 0 {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        });
    }

    let mut index = enum_instance.last_index.unwrap_or(0);
    if index >= enum_instance.file.len() {
        lore_debug!("Get enumeration: done");
        enum_instance.done = true;
        return Ok(());
    }

    let mut added = 0;
    while index < enum_instance.file.len() {
        let file = &enum_instance.file[index];

        if enum_instance.search.is_empty()
            || unsafe {
                ProjectedFileSystem::PrjFileNameMatch(
                    file.file_name.as_ptr(),
                    enum_instance.search_wide.as_ptr(),
                )
            }
        {
            let timestamp = ms_filetime(file.entry.timestamp_ms) as i64;
            let file_info = if file.entry.kind == EntryKind::Directory {
                ProjectedFileSystem::PRJ_FILE_BASIC_INFO {
                    IsDirectory: true,
                    FileSize: 0,
                    CreationTime: timestamp,
                    ChangeTime: timestamp,
                    LastAccessTime: timestamp,
                    LastWriteTime: timestamp,
                    FileAttributes: Win32::Storage::FileSystem::FILE_ATTRIBUTE_DIRECTORY,
                }
            } else {
                ProjectedFileSystem::PRJ_FILE_BASIC_INFO {
                    IsDirectory: false,
                    FileSize: file.entry.size as i64,
                    CreationTime: timestamp,
                    ChangeTime: timestamp,
                    LastAccessTime: timestamp,
                    LastWriteTime: timestamp,
                    FileAttributes: Win32::Storage::FileSystem::FILE_ATTRIBUTE_NORMAL,
                }
            };

            let mut file_path: Vec<u16> = enum_instance.base_path.encode_utf16().collect();
            if file_path.last().is_some_and(|&c| c != ('/' as u16)) {
                file_path.push('/' as u16);
            }
            file_path.extend(&file.file_name);

            let file_path_string = String::from_utf16_lossy(&file_path);

            lore_debug!(
                "Get enumeration: index {} / {} - {} ({} {})",
                index + 1,
                enum_instance.file.len(),
                file_path_string,
                if file_info.IsDirectory { "dir" } else { "file" },
                file_info.FileSize,
            );

            file_path.push(0);

            let res = unsafe {
                ProjectedFileSystem::PrjFillDirEntryBuffer(
                    file_path.as_ptr(),
                    &file_info,
                    dir_entry_buffer_handle,
                )
            };
            if res == ERROR_INSUFFICIENT_BUFFER as i32 {
                enum_instance.last_index = Some(index);

                // According to docs, if it returns HRESULT_FROM_WIN32(ERROR_INSUFFICIENT_BUFFER) for the first entry added
                // during any invocation of a PRJ_GET_DIRECTORY_ENUMERATION_CB callback, the provider must return
                // HRESULT_FROM_WIN32(ERROR_INSUFFICIENT_BUFFER) from the callback.
                if added == 0 {
                    lore_debug!(
                        "Get enumeration: insufficient buffer on first element, return error"
                    );
                    return Err(res);
                }

                // According to docs, if this routine returns HRESULT_FROM_WIN32(ERROR_INSUFFICIENT_BUFFER) when adding
                // an entry to the enumeration, the provider returns S_OK from the callback and waits for the next
                // PRJ_GET_DIRECTORY_ENUMERATION_CB callback.
                lore_debug!("Get enumeration: insufficient buffer");
                return Ok(());
            }
        }

        index += 1;
        added += 1;
    }

    enum_instance.last_index = Some(index);

    lore_debug!("Get enumeration: exit");

    Ok(())
}

const ERROR_FILE_NOT_FOUND: i32 = 0x80070002u32 as i32;
const ERROR_OUTOFMEMORY: i32 = 0x8007000eu32 as i32;
const ERROR_READ_FAULT: i32 = 0x8007001eu32 as i32;
const ERROR_INSUFFICIENT_BUFFER: i32 = 0x8007007au32 as i32;

unsafe extern "system" fn get_placeholder_info(
    cbdata: *const ProjectedFileSystem::PRJ_CALLBACK_DATA,
) -> i32 {
    let instance_context = instance_context(&cbdata);

    // Safety: Guaranteed by ProjectedFS API to be valid
    let path: &[u16] =
        unsafe { slice::from_raw_parts((*cbdata).FilePathName, wcslen((*cbdata).FilePathName)) };
    let path = String::from_utf16_lossy(path);

    match runtime().block_on(
        LORE_CONTEXT.scope(instance_context.execution.clone(), unsafe {
            get_placeholder_info_async(instance_context, cbdata, path)
        }),
    ) {
        Ok(()) => 0,
        Err(code) => code,
    }
}

/// Build the ProjFS placeholder metadata for a resolved node: type, size, revision timestamps,
/// and the provider/content identity ProjFS uses to detect stale placeholders.
async fn placeholder_info_for(resolved: &ResolvedNode) -> ProjectedFileSystem::PRJ_PLACEHOLDER_INFO {
    let timestamp = ms_filetime(resolved.revision_timestamp_ms().await) as i64;

    // Safety: This type is safe to zero initialize but not marked with default in windows_sys
    let mut placeholder_info = ProjectedFileSystem::PRJ_PLACEHOLDER_INFO::default();

    if resolved.kind() == EntryKind::Directory {
        placeholder_info.FileBasicInfo.IsDirectory = true;
        placeholder_info.FileBasicInfo.FileAttributes =
            Win32::Storage::FileSystem::FILE_ATTRIBUTE_DIRECTORY;
    } else {
        placeholder_info.FileBasicInfo.FileSize = resolved.size() as i64;
        placeholder_info.FileBasicInfo.FileAttributes =
            Win32::Storage::FileSystem::FILE_ATTRIBUTE_NORMAL;
    }

    placeholder_info.FileBasicInfo.CreationTime = timestamp;
    placeholder_info.FileBasicInfo.ChangeTime = timestamp;
    placeholder_info.FileBasicInfo.LastAccessTime = timestamp;
    placeholder_info.FileBasicInfo.LastWriteTime = timestamp;

    placeholder_info.VersionInfo.ProviderID[..std::mem::size_of::<Context>()]
        .copy_from_slice(resolved.repository.id.data());
    placeholder_info.VersionInfo.ContentID[..std::mem::size_of::<Hash>()]
        .copy_from_slice(resolved.node.address.hash.data());

    placeholder_info
}

async unsafe fn get_placeholder_info_async(
    instance_context: &InstanceContext,
    cbdata: *const ProjectedFileSystem::PRJ_CALLBACK_DATA,
    path: String,
) -> Result<(), i32> {
    let layers = instance_context.current_layers();
    let module_path = layers[0]
        .module
        .require_path()
        .map_err(|_| ERROR_FILE_NOT_FOUND)?;
    let relative_path =
        RelativePath::new_from_user_path(module_path, path.as_str()).unwrap_or_default();

    if instance_context.is_hidden(relative_path.as_str()) {
        return Err(ERROR_FILE_NOT_FOUND);
    }

    let Some(resolved) = core::resolve(&layers, relative_path.as_str()).await else {
        return Err(ERROR_FILE_NOT_FOUND);
    };

    let placeholder_info = placeholder_info_for(&resolved).await;

    lore_debug!(
        "Get placeholder info: path {path} ({} {})",
        if placeholder_info.FileBasicInfo.IsDirectory {
            "dir"
        } else {
            "file"
        },
        placeholder_info.FileBasicInfo.FileSize
    );

    unsafe {
        ProjectedFileSystem::PrjWritePlaceholderInfo(
            instance_context.instance,
            (*cbdata).FilePathName,
            &placeholder_info,
            std::mem::size_of::<ProjectedFileSystem::PRJ_PLACEHOLDER_INFO>() as u32,
        );
    }

    Ok(())
}

unsafe extern "system" fn query_file_name(
    cbdata: *const ProjectedFileSystem::PRJ_CALLBACK_DATA,
) -> i32 {
    let instance_context = instance_context(&cbdata);

    // Safety: Guaranteed by ProjectedFS API to be valid
    let path: &[u16] =
        unsafe { slice::from_raw_parts((*cbdata).FilePathName, wcslen((*cbdata).FilePathName)) };
    let path = String::from_utf16_lossy(path);
    let layers = instance_context.current_layers();
    let Ok(module_path) = layers[0].module.require_path() else {
        return ERROR_FILE_NOT_FOUND;
    };
    let relative_path =
        RelativePath::new_from_user_path(module_path, path.as_str()).unwrap_or_default();

    if instance_context.is_hidden(relative_path.as_str()) {
        return ERROR_FILE_NOT_FOUND;
    }

    let resolved = runtime().block_on(LORE_CONTEXT.scope(
        instance_context.execution.clone(),
        core::resolve(&layers, relative_path.as_str()),
    ));

    if resolved.is_some() {
        return 0;
    }

    ERROR_FILE_NOT_FOUND
}

unsafe extern "system" fn get_file_data(
    cbdata: *const ProjectedFileSystem::PRJ_CALLBACK_DATA,
    byte_offset: u64,
    length: u32,
) -> i32 {
    let instance_context = instance_context(&cbdata);

    // Safety: Guaranteed by ProjectedFS API to be valid
    let path: &[u16] =
        unsafe { slice::from_raw_parts((*cbdata).FilePathName, wcslen((*cbdata).FilePathName)) };
    let path = String::from_utf16_lossy(path);
    let layers = instance_context.current_layers();
    let Ok(module_path) = layers[0].module.require_path() else {
        return ERROR_FILE_NOT_FOUND;
    };
    let relative_path =
        RelativePath::new_from_user_path(module_path, path.as_str()).unwrap_or_default();

    match runtime().block_on(LORE_CONTEXT.scope(
        instance_context.execution.clone(),
        get_file_data_async(
            instance_context,
            relative_path,
            unsafe { (*cbdata).DataStreamId },
            byte_offset as usize,
            length as usize,
        ),
    )) {
        Ok(_) => 0,
        Err(err) => err,
    }
}

async fn get_file_data_async(
    instance_context: &InstanceContext,
    path: RelativePath,
    data_stream_id: windows_sys::core::GUID,
    byte_offset: usize,
    length: usize,
) -> Result<(), i32> {
    lore_debug!("Get file data: {path}");

    if instance_context.is_hidden(path.as_str()) {
        return Err(ERROR_FILE_NOT_FOUND);
    }

    let layers = instance_context.current_layers();
    let Some(resolved) = core::resolve(&layers, path.as_str()).await else {
        return Err(ERROR_FILE_NOT_FOUND);
    };

    if let Some(log) = instance_context.file_log.as_ref() {
        let mut log = log.lock();
        let _ = writeln!(log, "{}", path.as_str());
    }

    const SINGLE_READ_THRESHOLD: usize = 128 * 1024 * 1024;
    let capacity = std::cmp::min(length, SINGLE_READ_THRESHOLD);
    let mut write_offset = 0;
    let mut offset = byte_offset;
    let mut remain = length;

    // Safety: Call Win32 API - buffer is freed before returning
    let write_buffer = unsafe {
        ProjectedFileSystem::PrjAllocateAlignedBuffer(instance_context.instance, capacity)
    };
    if write_buffer.is_null() {
        lore_error!(
            "Failed to allocate aligned buffer: {}",
            Win32Error::get_last_error()
        );
        return Err(ERROR_OUTOFMEMORY);
    }

    while remain > 0 {
        let to_read = std::cmp::min(remain, capacity);

        // Safety: Ok as buffer is verified non-null and to_read is clamped to its capacity
        let buffer = unsafe { slice::from_raw_parts_mut(write_buffer.cast::<u8>(), to_read) };
        let read = match core::read_range(&resolved, offset as u64, buffer).await {
            Ok(read) => read,
            Err(err) => {
                lore_error!("Failed to read from immutable data: {err}");
                break;
            }
        };
        if read == 0 {
            // The requested window extends past the end of the file.
            break;
        }

        // Safety: Win32 API call above guarantees buffer validity and boundaries
        let res = unsafe {
            ProjectedFileSystem::PrjWriteFileData(
                instance_context.instance,
                &data_stream_id,
                write_buffer,
                write_offset as u64,
                read as u32,
            )
        };
        if res != 0 {
            lore_error!(
                "Failed to write file data to ProjFS: {}",
                Win32Error::from(res)
            );
            break;
        }

        remain -= read;
        offset += read;
        write_offset += read;
    }

    unsafe { ProjectedFileSystem::PrjFreeAlignedBuffer(write_buffer) };

    if remain > 0 {
        return Err(ERROR_READ_FAULT);
    }

    Ok(())
}

fn format_bytes_to_string(bytes: usize) -> String {
    let mut unit = "bytes";

    let converted = if bytes > 1024 * 1024 * 1024 {
        unit = "GiB";
        (bytes / (1024 * 1024)) as f64 / 1024.0
    } else if bytes > 1024 * 1024 {
        unit = "MiB";
        (bytes / 1024) as f64 / 1024.0
    } else if bytes > 1024 {
        unit = "KiB";
        bytes as f64 / 1024.0
    } else {
        bytes as f64
    };

    format!("{converted:.2} {unit}")
}

async fn prefetch_files(
    repository: Arc<RepositoryContext>,
    state: Arc<State>,
    prefetch: Vec<String>,
) {
    const MAX_CONCURRENT_PREFETCH: usize = 10000;
    let mut tasks = tokio::task::JoinSet::new();

    let mut last_file_count = 0;
    let mut last_print = Instant::now();
    let file_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let file_size = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    for path in prefetch {
        lore_spawn!(tasks, {
            let file_count = file_count.clone();
            let file_size = file_size.clone();
            let repository = repository.clone();
            let state = state.clone();
            async move {
                if let Some(size) = core::prefetch_path(repository, state, path.as_str()).await {
                    file_size.fetch_add(size as usize, std::sync::atomic::Ordering::Relaxed);
                }

                if let Ok(mut file) = tokio::fs::OpenOptions::new()
                    .read(true)
                    .write(false)
                    .truncate(false)
                    .create(false)
                    .open(path)
                    .await
                {
                    let mut buffer = [0u8; 32];
                    let _ = file.read(&mut buffer).await;
                    file_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    /*
                    if let Ok(metadata) = file.metadata().await {
                        file_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        file_size.fetch_add(
                            metadata.len() as usize,
                            std::sync::atomic::Ordering::Relaxed,
                        );
                    }
                    */
                }
            }
        });

        if last_print.elapsed().as_secs_f32() > 1.0 {
            let current_file_count = file_count.load(std::sync::atomic::Ordering::Relaxed);
            if current_file_count != last_file_count {
                println!(
                    "Prefetched {} files, {}",
                    current_file_count,
                    format_bytes_to_string(file_size.load(std::sync::atomic::Ordering::Relaxed))
                );
                last_file_count = current_file_count;
                last_print = Instant::now();
            }
        }

        if tasks.len() >= MAX_CONCURRENT_PREFETCH {
            let _ = tasks.join_next().await;
        }

        while tasks.try_join_next().is_some() {}
    }

    while !tasks.is_empty() {
        let _ = tasks.join_next().await;

        if last_print.elapsed().as_secs_f32() > 1.0 {
            let current_file_count = file_count.load(std::sync::atomic::Ordering::Relaxed);
            if current_file_count != last_file_count {
                println!(
                    "Prefetched {} files, {}",
                    current_file_count,
                    format_bytes_to_string(file_size.load(std::sync::atomic::Ordering::Relaxed))
                );
                last_file_count = current_file_count;
                last_print = Instant::now();
            }
        }
    }

    println!(
        "Prefetched done: {} files, {}",
        file_count.load(std::sync::atomic::Ordering::Relaxed),
        format_bytes_to_string(file_size.load(std::sync::atomic::Ordering::Relaxed))
    );
}
