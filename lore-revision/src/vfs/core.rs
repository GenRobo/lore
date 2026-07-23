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

use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use lore_base::lore_spawn;
use lore_storage::FRAGMENT_SIZE_THRESHOLD;

use crate::immutable;
use crate::immutable::ImmutableError;
use crate::node::Node;
use crate::node::NodeID;
use crate::node::ROOT_NODE;
use crate::repository::RepositoryContext;
use crate::repository::clone::VirtualLayer;
use crate::state::State;
use crate::store::StoreMatch;

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

/// Resolve a repository-relative path to a node, searching layers in order and returning the
/// first live match. Link nodes are followed to the repository/state that hold the content.
/// Staged-deleted nodes are skipped, matching the providers' behavior. The empty path
/// resolves to the layer root.
pub async fn resolve(layers: &[VirtualLayer], relative_path: &str) -> Option<ResolvedNode> {
    for layer in layers {
        if relative_path.is_empty() {
            // Root of this layer; always present when the layer has any state.
            if let Ok(node) = layer.state.node(layer.module.clone(), ROOT_NODE).await {
                return Some(ResolvedNode {
                    repository: layer.module.clone(),
                    state: layer.state.clone(),
                    node,
                    node_id: ROOT_NODE,
                });
            }
            continue;
        }

        let Ok(node_link) = layer
            .state
            .find_node_link(layer.module.clone(), relative_path)
            .await
        else {
            continue;
        };
        if !node_link.is_valid() {
            continue;
        }

        // Follow link nodes into the repository/state that actually hold the content.
        let Ok((repository, state)) = node_link
            .resolve(layer.module.clone(), layer.state.clone())
            .await
        else {
            continue;
        };

        let Ok(node) = state.node(repository.clone(), node_link.node).await else {
            continue;
        };
        if node.is_staged_delete() {
            continue;
        }

        return Some(ResolvedNode {
            repository,
            state,
            node,
            node_id: node_link.node,
        });
    }

    None
}

/// Enumerate the children of a directory identified by a repository-relative path. Layers are
/// merged in order and the first layer to provide a given name wins. Deleted and link nodes are
/// excluded. The empty path enumerates the workspace root.
pub async fn enumerate(layers: &[VirtualLayer], relative_path: &str) -> Vec<DirEntry> {
    let mut entries: Vec<DirEntry> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    for layer in layers {
        // Resolve this layer's base directory node.
        let base_node = if relative_path.is_empty() {
            ROOT_NODE
        } else {
            let Ok(node_link) = layer
                .state
                .find_node_link(layer.module.clone(), relative_path)
                .await
            else {
                continue;
            };
            if !node_link.is_valid() {
                continue;
            }
            node_link.node
        };

        let timestamp_ms = revision_timestamp_ms(layer).await;

        let Ok(children) = layer
            .state
            .collect_named_children_unsorted(layer.module.clone(), base_node, false, false)
            .await
        else {
            continue;
        };

        for child in children.children {
            if seen.contains(&child.name_string) {
                // First layer wins on name collisions.
                continue;
            }
            let Ok(node) = layer.state.node(layer.module.clone(), child.node).await else {
                continue;
            };
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
    }

    entries
}

/// Read up to `buf.len()` bytes of a resolved file starting at `offset`, hydrating from the
/// content-addressed store (fetching remotely, decompressing, and verifying as needed) while
/// honoring the repository's cache configuration. Returns the number of bytes written into
/// `buf`, which is `0` past the end of the file.
///
/// The store supports ranged reads only for fragmented objects (those larger than
/// [`FRAGMENT_SIZE_THRESHOLD`]); for a smaller single-fragment object it ignores the range and
/// yields the whole fragment. This function therefore reads the whole object for a small file
/// (bounded by the 256 KiB threshold) and slices out the requested window, and passes a range
/// through only for large fragmented files.
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

    // Whole-object read: works for any fragment type via the store's fast path.
    if offset == 0 && want as u64 == size {
        immutable::read_into(
            resolved.repository.clone(),
            resolved.node.address,
            None,
            &mut buf[..want],
            options,
        )
        .await?;
        return Ok(want);
    }

    // Partial read of a small, non-fragmented object: the store ignores the range and requires
    // the whole fragment, so read the whole (<= 256 KiB) object and copy out the window.
    if (size as usize) <= FRAGMENT_SIZE_THRESHOLD {
        let mut whole = vec![0u8; size as usize];
        immutable::read_into(
            resolved.repository.clone(),
            resolved.node.address,
            None,
            &mut whole,
            options,
        )
        .await?;
        let start = offset as usize;
        buf[..want].copy_from_slice(&whole[start..start + want]);
        return Ok(want);
    }

    // Partial read of a large fragmented object: the store honors the range directly.
    immutable::read_into(
        resolved.repository.clone(),
        resolved.node.address,
        Some((offset as usize)..(offset as usize + want)),
        &mut buf[..want],
        options,
    )
    .await?;

    Ok(want)
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
