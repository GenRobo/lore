// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Revision transitions for a lazy workspace whose mount is not being served.
//!
//! A lazy workspace (`lore clone --virtual`, or `lore clone --bare` followed by `lore mount`)
//! keeps on disk only what was written through the mount: those files live in the mount's
//! backing overlay, and everything else is projected from the current revision while the mount
//! is served. Unmounted, the overlay is the working tree — a partial one, in which a missing
//! entry means "projected", never "deleted".
//!
//! Moving such a workspace to another revision therefore writes no incoming content at all:
//! the next mount projects it. What has to happen instead is that the overlay stops
//! contradicting the revision it will be laid over, because an overlay entry always wins over
//! the projection:
//!
//! - an overlay file that is an unmodified copy of the old revision, at a path the new
//!   revision changes or deletes, is removed (it would mask the incoming file);
//! - an overlay file at a path the new revision leaves alone stays, edited or not, and keeps
//!   its dirty tracking;
//! - a local edit, addition or deletion at a path the new revision also touches is a conflict
//!   and fails the transition before anything is changed — unless the overlay already holds
//!   the incoming content, `--force` gives the incoming revision precedence, or
//!   `--forward-changes` gives the local change precedence;
//! - `--reset` discards the overlay's workspace entries and all dirty tracking, so the next
//!   mount shows exactly the target revision.
//!
//! [`transition`] validates and reconciles the overlay; the caller advances the anchors only
//! after it has returned, and then calls [`LazyTracking::apply`] to re-base the surviving dirty
//! tracking onto the new revision.

use std::collections::HashSet;
use std::io::ErrorKind;
use std::path::Path;
use std::sync::Arc;

use lore_error_set::prelude::*;

use crate::errors::*;
use crate::file::dirty;
use crate::file::dirty::ExplicitDirty;
use crate::filter::FilterMode;
use crate::immutable;
use crate::layer;
use crate::lore::execution_context;
use crate::lore_info;
use crate::node::Node;
use crate::node::ROOT_NODE;
use crate::repository;
use crate::repository::DOT_LORE;
use crate::repository::DOT_URC;
use crate::repository::RepositoryContext;
use crate::repository::RepositoryWriteToken;
use crate::revision::sync::SyncError;
use crate::revision::sync::SyncOptions;
use crate::state;
use crate::state::State;
use crate::util;
use crate::util::path::RelativePath;

/// Conflicting paths named in the error; the rest are only counted.
const MAX_REPORTED_CONFLICTS: usize = 8;

/// Whether the working tree of `repository` is the recorded backing overlay of a virtual mount
/// that is not being served — the one unserved binding a revision transition can handle.
///
/// The other unserved bindings stay refused: a mountpoint with no recorded overlay (`ProjFS`
/// keeps hydrated files in the mountpoint itself, and an unserved FUSE mountpoint is an empty
/// directory), an overlay that no longer exists, and an overlay that is the repository
/// directory itself (an ordinary checkout may have put its files there).
pub(crate) fn is_unmounted_overlay(repository: &RepositoryContext) -> bool {
    if repository.working_root_binding() != Some(false) {
        return false;
    }
    let (Ok(root), Ok(metadata_root)) = (
        repository.require_path(),
        repository.require_metadata_path(),
    ) else {
        return false;
    };
    if root == metadata_root {
        return false;
    }
    repository::read_mount_binding(&metadata_root.join(repository.format.dot_dir())).is_some_and(
        |binding| {
            binding.backing.as_deref() == Some(root)
                && root.is_dir()
                && !repository::mount_is_live(&binding.mountpoint)
        },
    )
}

/// Decide how a sync treats the working tree: `Ok(false)` for a materialized tree (an ordinary
/// workspace, or a mount that is being served), `Ok(true)` for an unmounted overlay, which
/// takes the lazy transition. Everything a lazy transition cannot do without materializing
/// content is refused here, before the sync resolves or changes anything.
pub(crate) async fn prepare(
    repository: &Arc<RepositoryContext>,
    options: &SyncOptions,
) -> Result<bool, SyncError> {
    if repository.working_root_binding() != Some(false) {
        return Ok(false);
    }
    if !is_unmounted_overlay(repository) {
        // A partial tree with no overlay to reconcile: refuse rather than misread it or write
        // beneath a dead virtualization root.
        return Err(InvalidArguments {
            reason: format!(
                "This workspace's working tree is the virtual mountpoint {}, which is not \
                 currently mounted. Mount it first (`lore mount`)",
                repository.path_for_display()
            ),
        }
        .into());
    }
    if !options.root_files.is_empty() {
        return Err(NotSupported {
            operation: "a dependency-selective sync (--root-file) of a lazy workspace: a sync \
                        materializes nothing there, every file is fetched when it is first read \
                        through the mount"
                .into(),
        }
        .into());
    }
    let layers = layer::list(repository.clone())
        .await
        .forward::<SyncError>("Failed to list the layers of a lazy workspace")?;
    if !layers.is_empty() {
        return Err(NotSupported {
            operation: format!(
                "synchronizing the unmounted lazy workspace at {}: the repository has layers, \
                 and a layer can only be synchronized into a materialized working tree",
                repository.path_for_display()
            ),
        }
        .into());
    }
    Ok(true)
}

/// The refusal for a sync that would have to merge diverged local and remote history: a merge
/// realizes both sides and their conflicts in the working tree, which a lazy workspace does
/// not have.
pub(crate) fn merge_refusal() -> SyncError {
    NotSupported {
        operation: "a lazy workspace cannot merge, and the local branch has diverged from its \
                    remote (a merge needs a fully materialized working tree). Publish the local \
                    commits on another branch (`lore branch create <name>`, then `lore push`), \
                    or drop them with `lore branch reset <revision>` or `lore sync --reset \
                    --force`"
            .into(),
    }
    .into()
}

/// Dirty tracking that outlives a lazy transition. The staged anchor records it relative to
/// the current revision, so it has to be written again once the anchors have moved.
pub(crate) struct LazyTracking {
    /// Whether there is a staged anchor to replace.
    tracked: bool,
    /// Tracked changes backed by an overlay entry.
    edits: Vec<RelativePath>,
    /// Tracked deletions.
    deletes: Vec<RelativePath>,
}

impl LazyTracking {
    fn untouched() -> Self {
        Self {
            tracked: false,
            edits: Vec::new(),
            deletes: Vec::new(),
        }
    }

    /// Replace the staged anchor with one that carries only the surviving tracking, relative
    /// to the revision the current anchor points at.
    ///
    /// This deliberately does not go through [`state::rebase_staged_anchor`], which re-derives
    /// every action from the working tree and takes a path missing from disk for a deletion.
    /// In an overlay a missing path is a projected file, so each surviving entry is re-applied
    /// the way it was recorded: an edit by inspecting the overlay entry that backs it, a
    /// deletion as reported.
    pub(crate) async fn apply(self, repository: Arc<RepositoryContext>) -> Result<(), SyncError> {
        if !self.tracked {
            return Ok(());
        }
        crate::instance::delete_staged_anchor(&repository)
            .await
            .forward::<SyncError>("Failed to reset the staged anchor of a lazy workspace")?;
        if !self.edits.is_empty() {
            dirty::dirty_relative_paths(repository.clone(), self.edits)
                .await
                .forward::<SyncError>("Failed to carry local edits of a lazy workspace over")?;
        }
        if !self.deletes.is_empty() {
            dirty::dirty_explicit(repository, self.deletes, ExplicitDirty::Delete)
                .await
                .forward::<SyncError>("Failed to carry local deletions of a lazy workspace over")?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    File,
    Directory,
    Link,
}

/// What a revision holds at a workspace path.
struct Tracked {
    /// Repository holding the node: the workspace's own, or the linked one the path leads into.
    repository: Arc<RepositoryContext>,
    node: Node,
}

impl Tracked {
    fn kind(&self) -> Kind {
        if self.node.is_file() {
            Kind::File
        } else if self.node.is_link() {
            Kind::Link
        } else {
            Kind::Directory
        }
    }

    /// Whether the overlay entry at `absolute` holds exactly this file's content.
    ///
    /// The content is always hashed, never assumed from size and timestamp, and a failure to
    /// hash is an error: "could not compare" must not be taken for "unmodified", because an
    /// unmodified copy may be deleted. The overlay's mode bits are not compared — the mount
    /// neither projects nor records them.
    async fn held_by(
        &self,
        absolute: &Path,
        metadata: &std::fs::Metadata,
    ) -> Result<bool, SyncError> {
        if self.kind() != Kind::File || !metadata.is_file() {
            return Ok(false);
        }
        let size = util::fs::file_size(metadata);
        if size != self.node.size {
            return Ok(false);
        }
        if size == 0 {
            return Ok(true);
        }
        let hash = immutable::hash_file(
            self.repository.clone(),
            absolute,
            Some(self.node.address),
            Some(size as usize),
        )
        .await
        .internal_with(|| {
            format!(
                "Failed to compare the overlay file {} with its committed content",
                absolute.display()
            )
        })?;
        if hash == self.node.address.hash {
            return Ok(true);
        }
        // The same bytes hash differently when the stored object was chunked under another
        // strategy; only a byte comparison settles that. It answers `false` when it cannot
        // read either side, which keeps the file.
        Ok(size <= state::CONTENT_COMPARE_MAX_SIZE
            && state::is_file_content_equal(
                self.repository.clone(),
                self.node.address,
                absolute,
                size,
            )
            .await)
    }
}

/// Resolve a workspace path in a revision, following links into the repository that holds the
/// node. Only "no such node" means absent; any other failure is an error.
async fn lookup(
    repository: &Arc<RepositoryContext>,
    state: &Arc<State>,
    path: &str,
) -> Result<Option<Tracked>, SyncError> {
    if state.revision().is_zero() {
        return Ok(None);
    }
    let link = match state.find_node_link(repository.clone(), path).await {
        Err(err) if err.is_node_not_found() => return Ok(None),
        link => link.forward::<SyncError>("Failed to resolve a path in a revision")?,
    };
    if !link.is_valid() {
        return Ok(None);
    }
    let (repository, state) = link
        .resolve(repository.clone(), state.clone())
        .await
        .forward::<SyncError>("Failed to resolve a linked repository")?;
    let node = state
        .node(repository.clone(), link.node)
        .await
        .forward::<SyncError>("Failed to load a node")?;
    Ok(Some(Tracked { repository, node }))
}

/// Whether two revisions hold the same thing at a path. Directories compare by kind only:
/// what is inside them is compared entry by entry, or with [`subtree_changed`].
fn unchanged(old: Option<&Tracked>, new: Option<&Tracked>) -> bool {
    match (old, new) {
        (None, None) => true,
        (Some(old), Some(new)) => match (old.kind(), new.kind()) {
            (Kind::File, Kind::File) => {
                old.node.address.hash == new.node.address.hash
                    && old.node.size == new.node.size
                    && !util::fs::mode_changed(old.node.mode, new.node.mode)
            }
            (Kind::Directory, Kind::Directory) => true,
            (Kind::Link, Kind::Link) => old.node.address == new.node.address,
            _ => false,
        },
        _ => false,
    }
}

/// Whether anything at or beneath a directory differs between the two revisions.
async fn subtree_changed(
    repository: &Arc<RepositoryContext>,
    state_current: &Arc<State>,
    state_target: &Arc<State>,
    directory: &RelativePath,
    filter_mode: FilterMode,
) -> Result<bool, SyncError> {
    let changes = state::diff_collect(
        repository.clone(),
        state_current.clone(),
        repository.clone(),
        state_target.clone(),
        Some(directory.clone()),
        filter_mode,
    )
    .await
    .forward::<SyncError>("Failed to compare a directory between revisions")?;
    Ok(!changes.is_empty())
}

/// A non-directory entry of the overlay, with the metadata of the entry itself (a symbolic
/// link is not followed).
struct OverlayFile {
    path: RelativePath,
    metadata: std::fs::Metadata,
}

/// The overlay entries that belong to the workspace. Left out, as in an eager sync: the
/// repository's dot directory, what the ignore and view filters exclude, and files tracked by a
/// git working tree that shares the directory.
#[derive(Default)]
struct Overlay {
    files: Vec<OverlayFile>,
    /// Every directory comes after its parent.
    directories: Vec<RelativePath>,
}

impl Overlay {
    async fn scan(
        repository: &RepositoryContext,
        root: &Path,
        filter_mode: FilterMode,
    ) -> Result<Self, SyncError> {
        let filter_mode = filter_mode | FilterMode::Ignore;
        let git = repository.git_presence();
        let mut overlay = Self::default();
        let mut pending = vec![RelativePath::new()];
        while let Some(directory) = pending.pop() {
            let absolute = directory.to_absolute_path(root);
            let mut entries = tokio::fs::read_dir(&absolute).await.internal_with(|| {
                format!(
                    "Failed to list the overlay directory {}",
                    absolute.display()
                )
            })?;
            while let Some(entry) = entries.next_entry().await.internal_with(|| {
                format!(
                    "Failed to list the overlay directory {}",
                    absolute.display()
                )
            })? {
                let name = entry.file_name();
                // A name that is not valid Unicode cannot be a workspace path.
                let Some(name) = name.to_str() else {
                    continue;
                };
                if name == DOT_URC || name == DOT_LORE {
                    continue;
                }
                let path = directory.push_into_buf(name).freeze();
                let is_directory = entry
                    .file_type()
                    .await
                    .internal_with(|| format!("Failed to inspect the overlay entry {path}"))?
                    .is_dir();
                if repository.filter.excludes(&path, is_directory, filter_mode) {
                    continue;
                }
                if is_directory {
                    overlay.directories.push(path.clone());
                    pending.push(path);
                } else if !git.tracks(path.as_str()) {
                    let metadata = entry
                        .metadata()
                        .await
                        .internal_with(|| format!("Failed to inspect the overlay entry {path}"))?;
                    overlay.files.push(OverlayFile { path, metadata });
                }
            }
        }
        Ok(overlay)
    }

    /// Remove every workspace entry: all files, then every directory left empty.
    async fn discard(&self, root: &Path) -> Result<(), SyncError> {
        for file in &self.files {
            remove_file(&file.path.to_absolute_path(root)).await?;
        }
        for directory in self.directories.iter().rev() {
            remove_directory_if_empty(&directory.to_absolute_path(root)).await?;
        }
        Ok(())
    }
}

async fn remove_file(absolute: &Path) -> Result<(), SyncError> {
    match tokio::fs::remove_file(absolute).await {
        Err(err) if err.kind() != ErrorKind::NotFound => Err(SyncError::internal_with_context(
            err,
            &format!("Failed to remove the overlay file {}", absolute.display()),
        )),
        _ => Ok(()),
    }
}

async fn remove_directory_if_empty(absolute: &Path) -> Result<(), SyncError> {
    let inspected = match tokio::fs::read_dir(absolute).await {
        Ok(mut entries) => entries.next_entry().await.map(|entry| entry.is_none()),
        Err(err) => Err(err),
    };
    let removed = match inspected {
        Ok(true) => tokio::fs::remove_dir(absolute).await,
        Ok(false) => return Ok(()),
        Err(err) => Err(err),
    };
    match removed {
        Err(err) if err.kind() != ErrorKind::NotFound => Err(SyncError::internal_with_context(
            err,
            &format!(
                "Failed to remove the overlay directory {}",
                absolute.display()
            ),
        )),
        _ => Ok(()),
    }
}

/// Which side wins where a local change and the incoming revision touch the same path.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Precedence {
    /// Neither: the transition fails.
    Neither,
    /// The incoming revision (`--force`): the local change is dropped.
    Incoming,
    /// The local change (`--forward-changes`): it stays, over the incoming revision.
    Local,
}

fn conflict_error(conflicts: &[String]) -> SyncError {
    let named = conflicts.len().min(MAX_REPORTED_CONFLICTS);
    let mut reason = format!(
        "Local changes in this lazy workspace conflict with the incoming revision: {}",
        conflicts[..named].join(", ")
    );
    if conflicts.len() > named {
        reason.push_str(&format!(" and {} more", conflicts.len() - named));
    }
    reason.push_str(
        ". Nothing was changed. Commit the local changes first, discard them with `lore sync \
         --reset`, or keep them over the incoming revision with `lore sync --forward-changes`",
    );
    InvalidArguments { reason }.into()
}

/// Take an unmounted lazy workspace from `state_current` to `state_target`: check every overlay
/// entry and every tracked deletion against both revisions, fail on a conflict before changing
/// anything, then remove the overlay entries that would mask the target revision. No incoming
/// content is written. Returns the dirty tracking to re-apply once the anchors have advanced.
///
/// Under `--dry-run` only the checks run. The write token is not used for the overlay itself,
/// which is the mount's scratch layer rather than repository storage; it is required because
/// the caller must hold the repository for writing while overlay entries are removed.
pub(crate) async fn transition(
    repository: Arc<RepositoryContext>,
    _token: &RepositoryWriteToken,
    state_current: Arc<State>,
    state_target: Arc<State>,
    options: &SyncOptions,
) -> Result<LazyTracking, SyncError> {
    let (force, dry_run) = {
        let context = execution_context();
        let globals = context.globals();
        (globals.force(), globals.dry_run())
    };

    let root = repository.require_path()?.to_path_buf();
    let overlay = Overlay::scan(&repository, &root, options.filter_mode).await?;
    let staged = crate::instance::load_staged_revision(&repository)
        .await
        .forward::<SyncError>("Failed to load the staged anchor")?;

    if options.reset {
        lore_info!(
            "Lazy workspace: reset discards {} overlay file(s) and all tracked changes; \
             revision {} is projected as committed",
            overlay.files.len(),
            state_target.revision_number()
        );
        if !dry_run {
            // Tracking goes first: a recorded change whose overlay file is gone would read as
            // a deletion, should the reset be cut short between the two.
            if staged.is_some() {
                crate::instance::delete_staged_anchor(&repository)
                    .await
                    .forward::<SyncError>("Failed to discard the staged anchor")?;
            }
            overlay.discard(&root).await?;
        }
        return Ok(LazyTracking::untouched());
    }

    let precedence = if force {
        Precedence::Incoming
    } else if options.forward_changes {
        Precedence::Local
    } else {
        Precedence::Neither
    };

    let mut conflicts: Vec<String> = Vec::new();

    // Overlay directories at or under a path the incoming revision turns into a file, and for
    // every overlay directory whether it has to go once it is empty: an overlay directory
    // masks an incoming file, and one the incoming revision deleted would linger as an empty
    // directory.
    let mut under_incoming_file: HashSet<&str> = HashSet::new();
    let mut prunable: Vec<bool> = Vec::with_capacity(overlay.directories.len());
    for directory in &overlay.directories {
        let path = directory.as_str();
        let (masks, prune) = if directory
            .parent()
            .is_some_and(|parent| under_incoming_file.contains(parent))
        {
            (true, true)
        } else {
            let old = lookup(&repository, &state_current, path).await?;
            let new = lookup(&repository, &state_target, path).await?;
            let masks = new.as_ref().is_some_and(|new| new.kind() == Kind::File)
                && !unchanged(old.as_ref(), new.as_ref());
            let deleted = new.is_none() && old.is_some_and(|old| old.kind() == Kind::Directory);
            (masks, masks || deleted)
        };
        if masks {
            under_incoming_file.insert(path);
        }
        prunable.push(prune);
    }

    // Overlay files to remove, and (by lowercase path) the ones that stop being a local
    // change: the removed ones, and those that already hold the incoming content.
    let mut remove: Vec<&RelativePath> = Vec::new();
    let mut settled: HashSet<&str> = HashSet::new();
    for file in &overlay.files {
        let path = file.path.as_str();
        let old = lookup(&repository, &state_current, path).await?;
        let new = lookup(&repository, &state_target, path).await?;
        let masks = file
            .path
            .parent()
            .is_some_and(|parent| under_incoming_file.contains(parent));
        // What the incoming revision does at this path; nothing means the entry stays as it
        // is, edited or not, and so does its tracking.
        let incoming_change = if masks {
            "local file under a path that is a file in the incoming revision"
        } else if !unchanged(old.as_ref(), new.as_ref()) {
            match (old.is_some(), new.is_some()) {
                (true, true) => "modified locally and in the incoming revision",
                (true, false) => "modified locally, deleted in the incoming revision",
                _ => "added locally and in the incoming revision",
            }
        } else if old
            .as_ref()
            .is_some_and(|old| old.kind() == Kind::Directory)
            && subtree_changed(
                &repository,
                &state_current,
                &state_target,
                &file.path,
                options.filter_mode,
            )
            .await?
        {
            // A local file where both revisions have a directory hides that directory: a
            // collision once the incoming revision changes anything inside it.
            "local file replaces a directory the incoming revision changes"
        } else {
            continue;
        };

        let absolute = file.path.to_absolute_path(&root);
        let clean = match old.as_ref() {
            Some(old) => old.held_by(&absolute, &file.metadata).await?,
            None => false,
        };
        if clean {
            // An unmodified copy of what the incoming revision replaces or deletes.
            remove.push(&file.path);
            settled.insert(file.path.as_lowercase_str());
            continue;
        }
        let incoming = match new.as_ref() {
            Some(new) => new.held_by(&absolute, &file.metadata).await?,
            None => false,
        };
        if incoming {
            // Already the incoming content: it stays, and is no longer a local change.
            settled.insert(file.path.as_lowercase_str());
            continue;
        }
        match precedence {
            Precedence::Incoming => {
                remove.push(&file.path);
                settled.insert(file.path.as_lowercase_str());
            }
            // A local file cannot be kept beneath a path that is itself an incoming file.
            Precedence::Local if !masks => {}
            _ => conflicts.push(format!("{path} ({incoming_change})")),
        }
    }

    // Dirty tracking recorded against the old revision: what survives, and whether any lapses.
    let mut tracking = LazyTracking {
        tracked: staged.is_some(),
        edits: Vec::new(),
        deletes: Vec::new(),
    };
    let mut lapsed = false;
    if let Some(staged) = staged {
        let staged = State::deserialize(repository.clone(), staged)
            .await
            .forward::<SyncError>("Failed to deserialize staged state")?;
        let mut dirty_paths = Vec::new();
        state::collect_dirty_paths(
            staged.clone(),
            repository.clone(),
            ROOT_NODE,
            RelativePath::new(),
            &mut dirty_paths,
        )
        .await
        .forward::<SyncError>("Failed to collect the tracked changes of a lazy workspace")?;

        for path in dirty_paths {
            let node = staged
                .find_node(repository.clone(), path.as_str())
                .await
                .forward::<SyncError>("Failed to load a tracked node")?;
            let on_disk = match tokio::fs::symlink_metadata(path.to_absolute_path(&root)).await {
                Ok(metadata) => Some(metadata),
                Err(err)
                    if matches!(err.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) =>
                {
                    None
                }
                Err(err) => {
                    return Err(SyncError::internal_with_context(
                        err,
                        &format!("Failed to inspect the overlay entry {path}"),
                    ));
                }
            };
            let old = lookup(&repository, &state_current, path.as_str()).await?;

            // A deletion stands unless a file was created again where a file (or nothing) was
            // deleted; that one is an edit of the path.
            let recreated = on_disk.as_ref().is_some_and(|metadata| !metadata.is_dir())
                && old.as_ref().is_none_or(|old| old.kind() != Kind::Directory);
            if node.is_dirty_delete() && !recreated {
                let new = lookup(&repository, &state_target, path.as_str()).await?;
                let (Some(old), Some(new)) = (old, new) else {
                    // Nothing left to delete: the incoming revision deleted it as well.
                    lapsed = true;
                    continue;
                };
                // Re-applying the deletion of a directory deletes whatever the new revision
                // holds beneath it, so the whole subtree has to be unchanged.
                let untouched = if old.kind() == Kind::Directory && new.kind() == Kind::Directory {
                    !subtree_changed(
                        &repository,
                        &state_current,
                        &state_target,
                        &path,
                        options.filter_mode,
                    )
                    .await?
                } else {
                    unchanged(Some(&old), Some(&new))
                };
                if untouched || precedence == Precedence::Local {
                    tracking.deletes.push(path);
                } else if precedence == Precedence::Incoming {
                    lapsed = true;
                } else {
                    conflicts.push(format!(
                        "{path} (deleted locally, changed in the incoming revision)"
                    ));
                }
                continue;
            }

            let survives = match on_disk {
                // Nothing in the overlay backs the change. Absence is not a deletion here.
                None => false,
                // Re-inspecting a tracked directory marks every child the revision has there,
                // and the overlay lacks, as deleted: only safe where no revision has anything.
                Some(metadata) if metadata.is_dir() => {
                    old.is_none()
                        && !path
                            .parent()
                            .is_some_and(|parent| under_incoming_file.contains(parent))
                        && lookup(&repository, &state_target, path.as_str())
                            .await?
                            .is_none()
                }
                Some(_) => !settled.contains(path.as_lowercase_str()),
            };
            if survives {
                tracking.edits.push(path);
            } else {
                lapsed = true;
            }
        }
    }

    if !conflicts.is_empty() {
        return Err(conflict_error(&conflicts));
    }

    lore_info!(
        "Lazy workspace: nothing is materialized. Overlay files superseded by revision {2}: \
         {0} of {1}; tracked local changes kept: {3}",
        remove.len(),
        overlay.files.len(),
        state_target.revision_number(),
        tracking.edits.len() + tracking.deletes.len()
    );
    if dry_run {
        return Ok(LazyTracking::untouched());
    }

    // Tracking of a path that is about to lose its overlay file goes first: a recorded change
    // with no file behind it would read as a deletion to the next stage, should the transition
    // be cut short before the anchors move. The survivors are written against the revision the
    // workspace still stands at, and once more by the caller against the one it moves to.
    if lapsed {
        LazyTracking {
            tracked: true,
            edits: tracking.edits.clone(),
            deletes: tracking.deletes.clone(),
        }
        .apply(repository.clone())
        .await?;
    }
    for path in remove {
        remove_file(&path.to_absolute_path(&root)).await?;
    }
    for (directory, prune) in overlay.directories.iter().zip(prunable).rev() {
        if prune {
            remove_directory_if_empty(&directory.to_absolute_path(&root)).await?;
        }
    }

    Ok(tracking)
}

#[cfg(test)]
mod tests {
    // The refusal of a diverged lazy workspace has to name both ways out, since `lore sync`
    // itself (the usual answer to "diverged") is the command being refused.
    #[test]
    fn merge_refusal_names_both_ways_out() {
        let message = super::merge_refusal().to_string();
        assert!(message.contains("cannot merge"), "{message}");
        assert!(message.contains("`lore branch create <name>`"), "{message}");
        assert!(
            message.contains("`lore branch reset <revision>`"),
            "{message}"
        );
        assert!(
            message.contains("--reset") && message.contains("--force"),
            "{message}"
        );
    }
}
