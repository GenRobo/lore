// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Platform-neutral virtual file system core.
//!
//! Both the Windows `ProjFS` provider (`crate::projfs`) and the Linux FUSE backend
//! (`crate::vfs::fuse`) present a Lore workspace as an on-demand-hydrated file system.
//! The logic that resolves a repository-relative path to a node, enumerates a directory,
//! and streams file content out of the content-addressed store is identical on every
//! platform. This module owns that logic so each backend is only a thin translation shim
//! between its OS filesystem API and these operations.
//!
//! All functions operate on repository-relative paths: forward-slash separated, with no
//! leading slash (the empty string denotes the workspace root). Each backend is
//! responsible for turning its own path representation into that form.

use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use futures::future::try_join_all;
use lore_base::lore_spawn;
use lore_storage::options::ReadOptions;

use crate::fragment::FragmentFlags;
use crate::immutable;
use crate::immutable::ImmutableError;
use crate::lore::Address;
use crate::lore::FragmentReference;
use crate::lore::TypedBytes;
use crate::lore_error;
use crate::node::Node;
use crate::node::NodeID;
use crate::node::ROOT_NODE;
use crate::repository::RepositoryContext;
use crate::repository::clone::VirtualLayer;
use crate::state::State;
use crate::state::StateError;
use crate::store::StoreMatch;
use crate::util::path::RelativePathBuf;

/// Kind of a filesystem entry surfaced by the virtual file system.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    File,
    Directory,
}

/// A single child of a directory, resolved from Lore state.
#[derive(Debug, Clone)]
pub struct DirEntry {
    /// Leaf name (no path separators).
    pub name: String,
    pub kind: EntryKind,
    pub size: u64,
    pub node: NodeID,
    /// Modification time in milliseconds since the Unix epoch (the revision timestamp).
    pub timestamp_ms: u64,
}

/// A repository-relative path resolved to a concrete node, following any link nodes into
/// the repository that actually holds the content.
pub struct ResolvedNode {
    pub repository: Arc<RepositoryContext>,
    pub state: Arc<State>,
    pub node: Node,
    pub node_id: NodeID,
}

impl ResolvedNode {
    pub fn kind(&self) -> EntryKind {
        if self.node.is_directory() {
            EntryKind::Directory
        } else {
            EntryKind::File
        }
    }

    pub fn size(&self) -> u64 {
        self.node.size
    }

    /// Revision timestamp (milliseconds since the Unix epoch) of the state this node resolved
    /// into (which, after following a link, may differ from the layer it was looked up in),
    /// falling back to the current time when the revision metadata cannot be read.
    pub async fn revision_timestamp_ms(&self) -> u64 {
        match self.state.revision_metadata(self.repository.clone()).await {
            Ok(metadata) => metadata.timestamp,
            Err(_) => now_ms(),
        }
    }
}

/// Current wall-clock time in milliseconds since the Unix epoch.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

/// Revision timestamp (milliseconds since the Unix epoch) for a layer, falling back to the
/// current time when the revision metadata cannot be read.
pub async fn revision_timestamp_ms(layer: &VirtualLayer) -> u64 {
    match layer.state.revision_metadata(layer.module.clone()).await {
        Ok(metadata) => metadata.timestamp,
        Err(_) => now_ms(),
    }
}

/// Resolve a repository-relative path within one layer. Link nodes are followed to the
/// repository/state that hold the content, and staged-deleted nodes count as absent. The empty
/// path resolves to the layer root.
///
/// `Ok(None)` means the layer holds no live node at that path. An error means the layer could
/// not be read (a tree block that did not load, a link whose target did not), which says
/// nothing about whether the node exists.
async fn resolve_in_layer(
    layer: &VirtualLayer,
    relative_path: &str,
) -> Result<Option<ResolvedNode>, StateError> {
    if relative_path.is_empty() {
        let node = layer.state.node(layer.module.clone(), ROOT_NODE).await?;
        return Ok(Some(ResolvedNode {
            repository: layer.module.clone(),
            state: layer.state.clone(),
            node,
            node_id: ROOT_NODE,
        }));
    }

    let node_link = match layer
        .state
        .find_node_link(layer.module.clone(), relative_path)
        .await
    {
        Ok(node_link) => node_link,
        Err(err) if err.is_node_not_found() => return Ok(None),
        Err(err) => return Err(err),
    };
    if !node_link.is_valid() {
        return Ok(None);
    }

    // Follow link nodes into the repository/state that actually hold the content.
    let (repository, state) = node_link
        .resolve(layer.module.clone(), layer.state.clone())
        .await?;

    let node = state.node(repository.clone(), node_link.node).await?;
    if node.is_staged_delete() {
        return Ok(None);
    }

    Ok(Some(ResolvedNode {
        repository,
        state,
        node,
        node_id: node_link.node,
    }))
}

/// Resolve a repository-relative path to a node, searching layers in order and returning the
/// first live match. Link nodes are followed to the repository/state that hold the content.
/// Staged-deleted nodes are skipped, matching the providers' behavior. The empty path
/// resolves to the layer root.
///
/// A layer that cannot be read counts here as not holding the path, so `None` does not prove
/// the path absent. Callers that must tell the two apart use [`try_resolve`].
pub async fn resolve(layers: &[VirtualLayer], relative_path: &str) -> Option<ResolvedNode> {
    for layer in layers {
        if let Ok(Some(resolved)) = resolve_in_layer(layer, relative_path).await {
            return Some(resolved);
        }
    }

    None
}

/// [`resolve`], except that a layer that cannot be read is an error instead of a miss:
/// `Ok(None)` is returned only when every layer was read and none holds a live node at the
/// path. A later layer is never consulted past one that failed, since the failed one might
/// hold the path.
pub async fn try_resolve(
    layers: &[VirtualLayer],
    relative_path: &str,
) -> Result<Option<ResolvedNode>, StateError> {
    for layer in layers {
        if let Some(resolved) = resolve_in_layer(layer, relative_path).await? {
            return Ok(Some(resolved));
        }
    }

    Ok(None)
}

/// Add one layer's children of a directory to `entries`, skipping the names in `seen` (the
/// first layer to provide a name wins) and recording the ones added. A layer without that
/// directory adds nothing. An error means the layer's children could not all be read; entries
/// added before it stay.
async fn enumerate_layer(
    layer: &VirtualLayer,
    relative_path: &str,
    seen: &mut std::collections::HashSet<String>,
    entries: &mut Vec<DirEntry>,
) -> Result<(), StateError> {
    // Resolve this layer's base directory node.
    let base_node = if relative_path.is_empty() {
        ROOT_NODE
    } else {
        match layer
            .state
            .find_node_link(layer.module.clone(), relative_path)
            .await
        {
            Ok(node_link) if node_link.is_valid() => node_link.node,
            Ok(_) => return Ok(()),
            Err(err) if err.is_node_not_found() => return Ok(()),
            Err(err) => return Err(err),
        }
    };

    let timestamp_ms = revision_timestamp_ms(layer).await;

    let children = layer
        .state
        .collect_named_children_unsorted(layer.module.clone(), base_node, false, false)
        .await?;

    for child in children.children {
        if seen.contains(&child.name_string) {
            // First layer wins on name collisions.
            continue;
        }
        let node = layer.state.node(layer.module.clone(), child.node).await?;
        let kind = if node.is_directory() {
            EntryKind::Directory
        } else {
            EntryKind::File
        };
        seen.insert(child.name_string.clone());
        entries.push(DirEntry {
            name: child.name_string,
            kind,
            size: node.size,
            node: child.node,
            timestamp_ms,
        });
    }

    Ok(())
}

/// Enumerate the children of a directory identified by a repository-relative path. Layers are
/// merged in order and the first layer to provide a given name wins. Deleted and link nodes are
/// excluded. The empty path enumerates the workspace root.
///
/// A layer whose children cannot be read contributes what was read before the failure, so the
/// listing may be short without saying so. Callers that must not present a short listing as
/// complete use [`try_enumerate`].
pub async fn enumerate(layers: &[VirtualLayer], relative_path: &str) -> Vec<DirEntry> {
    let mut entries: Vec<DirEntry> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    for layer in layers {
        let _ = enumerate_layer(layer, relative_path, &mut seen, &mut entries).await;
    }

    entries
}

/// [`enumerate`], except that a layer whose children cannot be read fails the enumeration
/// instead of shortening it.
pub async fn try_enumerate(
    layers: &[VirtualLayer],
    relative_path: &str,
) -> Result<Vec<DirEntry>, StateError> {
    let mut entries: Vec<DirEntry> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    for layer in layers {
        enumerate_layer(layer, relative_path, &mut seen, &mut entries).await?;
    }

    Ok(entries)
}

/// Deepest nesting of fragment lists a read follows. One list references 6553 fragments of up
/// to 256 KiB each, so three levels already span petabytes; a deeper tree is malformed.
const MAX_FRAGMENT_DEPTH: usize = 8;

/// Read up to `buf.len()` bytes of a resolved file starting at `offset`, hydrating from the
/// content-addressed store (fetching remotely and decompressing as needed) while honoring the
/// repository's cache configuration. Returns the number of bytes written into `buf`: `0` past
/// the end of the file, otherwise the whole part of the requested window that lies inside the
/// file. A read is never short and never partially filled.
///
/// The window is assembled here, fragment by fragment, so that no byte reaches the caller
/// unproven. Every fragment the window touches (the node's root fragment, any fragment lists
/// below it, and the leaves) is loaded with its payload hashed against its content address, and
/// must hold exactly the content span that the node's size, or the list referencing it, assigns
/// to it. A fragment failing either check fails the whole read with an error that names its
/// address; nothing is returned in its place. Backends report that to the reader as an I/O
/// error.
///
/// The store's own ranged read ([`immutable::read_into`]) hashes fragments the same way but
/// takes their sizes on trust: it cuts a fragment longer than its span short, and reports a
/// window past the content the root fragment claims as read without writing it.
pub async fn read_range(
    resolved: &ResolvedNode,
    offset: u64,
    buf: &mut [u8],
) -> Result<usize, ImmutableError> {
    if buf.is_empty() {
        return Ok(0);
    }

    let size = resolved.node.size;
    if offset >= size {
        return Ok(0);
    }

    let want = std::cmp::min(buf.len() as u64, size - offset) as usize;

    let options = immutable::read_options_from_repository(&resolved.repository)
        .with_decompress()
        .with_remote()
        .with_verify();

    read_fragment(
        &resolved.repository,
        resolved.node.address,
        0..size,
        offset,
        &mut buf[..want],
        options,
        0,
    )
    .await?;

    Ok(want)
}

/// Fill `buf` with the content bytes starting at `offset` from the fragment at `address`, which
/// must hold exactly the content `span`. Both are offsets into the whole file, and
/// `offset..offset + buf.len()` is a non-empty window inside `span`.
///
/// A leaf is copied from; a fragment list is followed into the entries the window touches, each
/// read concurrently into its own part of `buf`. Boxed because lists nest.
fn read_fragment<'a>(
    repository: &'a Arc<RepositoryContext>,
    address: Address,
    span: Range<u64>,
    offset: u64,
    buf: &'a mut [u8],
    options: ReadOptions,
    depth: usize,
) -> Pin<Box<dyn Future<Output = Result<(), ImmutableError>> + Send + 'a>> {
    Box::pin(async move {
        // Hashes the payload against `address` (after decompression), from the local store or the
        // remote alike, and fails rather than return a payload that does not match.
        let (fragment, payload) = immutable::load_raw(repository.clone(), address, options)
            .await
            .inspect_err(|err| lore_error!("vfs read: fragment {address} did not load: {err}"))?;

        let span_size = span.end - span.start;
        if fragment.size_content != span_size {
            return Err(refuse(
                address,
                format_args!(
                    "holds {} bytes of content where {span_size} are expected",
                    fragment.size_content
                ),
            ));
        }

        if (fragment.flags & FragmentFlags::PayloadFragmented) != FragmentFlags::PayloadFragmented {
            if payload.len() as u64 != span_size {
                return Err(refuse(
                    address,
                    format_args!(
                        "has a payload of {} bytes where {span_size} are expected",
                        payload.len()
                    ),
                ));
            }
            let start = (offset - span.start) as usize;
            buf.copy_from_slice(&payload[start..start + buf.len()]);
            return Ok(());
        }

        if depth == MAX_FRAGMENT_DEPTH {
            return Err(refuse(
                address,
                format_args!("nests fragment lists deeper than {MAX_FRAGMENT_DEPTH} levels"),
            ));
        }
        if payload.is_empty() || !payload.len().is_multiple_of(size_of::<FragmentReference>()) {
            return Err(refuse(
                address,
                format_args!("is a fragment list of {} bytes", payload.len()),
            ));
        }
        let payload = payload.to_aligned::<FragmentReference>();
        let references = payload.as_type_slice::<FragmentReference>();
        if references[0].offset_content != span.start {
            return Err(refuse(
                address,
                format_args!(
                    "is a fragment list starting at content offset {} where {} is expected",
                    references[0].offset_content, span.start
                ),
            ));
        }

        // Start at the last entry beginning at or before the window, then take each entry's part
        // of the window in turn. An entry spans from its own offset to the next entry's (the
        // list's end for the last), and must hold the next byte to read: that rejects offsets
        // that do not strictly increase, or that leave the list's span, before anything is read.
        let end = offset + buf.len() as u64;
        let mut index = references
            .partition_point(|reference| reference.offset_content <= offset)
            .saturating_sub(1);
        let mut cursor = offset;
        let mut rest = buf;
        let mut reads = Vec::new();
        while cursor < end {
            let entry_start = references.get(index).map(|entry| entry.offset_content);
            let entry_end = references
                .get(index + 1)
                .map_or(span.end, |next| next.offset_content);
            let Some(entry_start) = entry_start.filter(|&entry_start| {
                entry_start <= cursor && cursor < entry_end && entry_end <= span.end
            }) else {
                return Err(refuse(
                    address,
                    format_args!(
                        "is a fragment list spanning {}..{} whose entry {index} does not hold content offset {cursor}",
                        span.start, span.end
                    ),
                ));
            };
            let take = (entry_end.min(end) - cursor) as usize;
            let (head, tail) = std::mem::take(&mut rest).split_at_mut(take);
            rest = tail;
            reads.push(read_fragment(
                repository,
                Address {
                    context: address.context,
                    hash: references[index].hash,
                },
                entry_start..entry_end,
                cursor,
                head,
                options,
                depth + 1,
            ));
            cursor += take as u64;
            index += 1;
        }

        try_join_all(reads).await?;
        Ok(())
    })
}

/// Fail a read on a fragment that passed its hash check but does not fit the place the node, or
/// the list referencing it, assigns to it. Logged with the address; never served from.
fn refuse(address: Address, detail: std::fmt::Arguments<'_>) -> ImmutableError {
    lore_error!("vfs read refused: fragment {address} {detail}");
    ImmutableError::internal(format!("fragment {address} {detail}"))
}

/// Repository-relative paths of all staged dirty-delete nodes: files deleted since the served
/// revision through any interface (the mount itself, the CLI, or a sync from elsewhere).
/// Backends hide these from the projection so a delete is not undone by the projected entry
/// reappearing — the FUSE backend seeds its whiteout set from this, the ProjFS backend its
/// hidden set (local deletes there are additionally tombstoned on disk by the OS).
pub async fn staged_delete_paths(repository: Arc<RepositoryContext>) -> Vec<String> {
    let Ok((_current, Some(staged), _branch)) =
        State::deserialize_current_and_staged(repository.clone()).await
    else {
        return Vec::new();
    };
    let Ok(paths) = staged
        .collect_dirty_paths(repository.clone(), ROOT_NODE, RelativePathBuf::default())
        .await
    else {
        return Vec::new();
    };
    let mut deleted = Vec::new();
    for path in paths {
        if let Ok(link) = staged
            .find_node_link(repository.clone(), path.as_str())
            .await
            && link.is_valid()
            && let Ok(node) = staged.node(repository.clone(), link.node).await
            && node.is_dirty_delete()
        {
            deleted.push(path.as_str().to_string());
        }
    }
    deleted
}

/// Resolve a single repository-relative path (following link nodes) and warm the content cache
/// for its data. Returns the stored fragment size when content was cached, `None` when the path
/// does not resolve to cacheable content.
pub async fn prefetch_path(
    repository: Arc<RepositoryContext>,
    state: Arc<State>,
    path: &str,
) -> Option<u64> {
    let node_link = state.find_node_link(repository.clone(), path).await.ok()?;
    if !node_link.is_valid() {
        return None;
    }
    let (repository, state) = node_link.resolve(repository, state).await.ok()?;
    let node = state.node(repository.clone(), node_link.node).await.ok()?;
    if node.address.hash.is_zero() {
        return None;
    }
    let _ = immutable::cache(repository.clone(), vec![node.address], true).await;
    let result = repository
        .immutable_store()
        .query(repository.id, node.address, StoreMatch::MatchHash)
        .await
        .ok()?;
    Some(result.fragment.size_content)
}

/// Warm the content cache for a list of repository-relative paths, resolving each and caching
/// its fragmented content. Runs the resolutions concurrently. Backends may additionally trigger
/// OS-level hydration (e.g. by reading through the mount), which is platform-specific and left
/// to the caller.
pub async fn prefetch(repository: Arc<RepositoryContext>, state: Arc<State>, paths: Vec<String>) {
    const MAX_CONCURRENT_PREFETCH: usize = 10_000;
    let mut tasks = tokio::task::JoinSet::new();

    for path in paths {
        lore_spawn!(tasks, {
            let repository = repository.clone();
            let state = state.clone();
            async move {
                let _ = prefetch_path(repository, state, path.as_str()).await;
            }
        });

        if tasks.len() >= MAX_CONCURRENT_PREFETCH {
            let _ = tasks.join_next().await;
        }
        while tasks.try_join_next().is_some() {}
    }

    while tasks.join_next().await.is_some() {}
}
