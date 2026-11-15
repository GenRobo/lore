// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::any::Any;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use lore_base::lore_spawn;
use lore_base::types::BranchPoint;
use lore_error_set::prelude::*;
use lore_transport::ProtocolError;
use lore_transport::StorageSession;
use serde::Deserialize;
use serde::Serialize;
use tokio::task::JoinError;
use tokio::task::JoinSet;
use tokio_util::task::AbortOnDropHandle;

use crate::branch;
use crate::branch::BranchLatestStatus;
use crate::errors::*;
use crate::event;
use crate::event::EventError;
use crate::fragment;
use crate::history;
use crate::immutable;
use crate::interface::LoreError;
use crate::interface::LoreString;
use crate::layer;
use crate::lore::Address;
use crate::lore::BranchId;
use crate::lore::Context;
use crate::lore::Hash;
use crate::lore::RepositoryId;
use crate::lore::execution_context;
use crate::lore_debug;
use crate::lore_info;
use crate::node::Node;
use crate::node::NodeID;
use crate::node::ROOT_NODE;
use crate::repository;
use crate::repository::RepositoryContext;
use crate::repository::RepositoryWriteToken;
use crate::state;
use crate::state::State;
use crate::state::StateNodeChildrenWithNameIterator;
use crate::store;
use crate::util::serde::u8_as_bool;

/// Data for the event sent when a branch push starts.
#[repr(C)]
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoreBranchPushEventData {
    /// The remote being pushed to.
    pub remote: LoreString,
    /// The repository being pushed.
    pub repository: RepositoryId,
    /// The branch being pushed.
    pub branch: BranchId,
    /// The name of the branch being pushed.
    pub branch_name: LoreString,
    /// The latest revision of the branch on the remote.
    pub remote_revision: Hash,
    /// The latest revision of the branch in the local repository.
    pub local_revision: Hash,
    /// The number of revisions on the remote that are not present locally.
    pub remote_history: u64,
    /// The number of local revisions to push.
    pub local_history: u64,
    /// Set when the local revision is already present on the remote.
    #[serde(with = "u8_as_bool")]
    pub flag_already_pushed: u8,
    /// Set when the branch is the repository's default branch.
    #[serde(with = "u8_as_bool")]
    pub flag_default: u8,
    /// Set when the repository is a linked repository.
    #[serde(with = "u8_as_bool")]
    pub flag_link: u8,
    /// Set when the repository is a layer.
    #[serde(with = "u8_as_bool")]
    pub flag_layer: u8,
}

/// Data for the event sent before a revision's parent is rewritten during push.
#[repr(C)]
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoreBranchPushRevisionUpdateBeginEventData {
    /// The revision being updated.
    pub revision: Hash,
    /// The previous parent revision.
    pub old_parent: Hash,
    /// The new parent revision.
    pub new_parent: Hash,
}

/// Data for the event sent after a revision's parent is rewritten during push.
#[repr(C)]
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoreBranchPushRevisionUpdateEndEventData {
    /// The updated revision.
    pub revision: Hash,
}

/// Data for the event sent before fragments are transferred during push.
#[repr(C)]
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoreBranchPushFragmentBeginEventData {
    /// The number of fragments to transfer.
    pub fragments: u64,
    /// The total number of bytes to transfer.
    pub bytes_total: u64,
}

/// Data for the event sent as fragments are transferred during push.
#[repr(C)]
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoreBranchPushFragmentProgressEventData {
    /// The number of fragments transferred so far.
    pub complete: u64,
    /// The total number of fragments to transfer.
    pub count: u64,
    /// The number of bytes transferred so far.
    pub bytes_transferred: u64,
    /// The total number of bytes to transfer.
    pub bytes_total: u64,
}

/// Data for the event sent after fragments are transferred during push.
#[repr(C)]
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoreBranchPushFragmentEndEventData {
    /// The number of fragments transferred.
    pub fragments: u64,
    /// The number of bytes transferred.
    pub bytes_transferred: u64,
}

/// Data for the event sent before a branch is created on the remote.
#[repr(C)]
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoreBranchPushBranchCreateBeginEventData {
    /// The local revision the branch starts from.
    pub local_revision: Hash,
}

/// Data for the event sent after a branch is created on the remote.
#[repr(C)]
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoreBranchPushBranchCreateEndEventData {
    /// The revision the branch points to on the remote.
    pub remote_revision: Hash,
}

/// Data for the event sent before a revision is pushed to the remote.
#[repr(C)]
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoreBranchPushRevisionPushBeginEventData {
    /// The latest revision of the branch on the remote.
    pub remote_revision: Hash,
    /// The local revision being pushed.
    pub local_revision: Hash,
}

/// Data for the event sent when the remote assigns a pushed revision a new identity.
#[repr(C)]
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoreBranchPushRevisionPushUpdateEventData {
    /// The revision before the remote reassigned it.
    pub old_revision: Hash,
    /// The revision the remote assigned.
    pub new_revision: Hash,
    /// The sequential number of the new revision.
    pub new_revision_number: u64,
}

/// Data for the event sent after a revision is pushed to the remote.
#[repr(C)]
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoreBranchPushRevisionPushEndEventData {
    /// The branch revision on the remote before the push.
    pub old_remote_revision: Hash,
    /// The branch revision on the remote after the push.
    pub new_remote_revision: Hash,
    /// The sequential number of the new remote revision.
    pub new_remote_revision_number: u64,
    /// A message returned by the remote for the push.
    pub message: LoreString,
    /// Set when the remote performed a fast-forward merge.
    #[serde(with = "u8_as_bool")]
    pub fast_forward_merged: u8,
}

#[error_set]
pub enum PushError {
    NodeNotFound,
    LinkNotFound,
    NotFound,
    FileNotFound,
    RevisionNotFound,
    WriteRequired,
    Oversized,
    InvalidPath,
    InvalidNodeHierarchy,
    AddressNotFound,
    PayloadNotFound,
    Disconnected,
    InvalidArguments,
    AlreadyLinked,
    LayerNotFound,
    SlowDown,
    NotAuthorized,
    NotAuthenticated,
    Maintenance,
    NoRemote,
    NotSupported,
    BranchAdvanced,
    BranchAlreadyExists,
    BranchNotFound,
    Conflict,
    DeleteCurrent,
    DeleteDefault,
    DeleteProtected,
    Divergent,
    IdenticalMetadata,
    LinkPathNotFound,
    LocalModifications,
    LockNotFound,
    LockNotOwned,
    MaxHistorySearchDepth,
    NotALayer,
    NotALink,
    NotConnected,
    NothingStaged,
    RepositoryAlreadyExists,
    RepositoryNotFound,
    SharedStoreNotFound,
    TokenNotFound,
    MissingIdentity,
}

#[derive(Clone, Debug)]
pub struct PushOptions {
    /// Branch to push, default to current branch if not set
    pub branch: Option<String>,
    /// Allow the server to fast-forward merge if the target branch head has moved
    pub fast_forward_merge: bool,
}

impl EventError for PushError {
    fn translated(&self) -> LoreError {
        match self {
            PushError::Disconnected(_) => LoreError::Connection,
            PushError::SlowDown(_) => LoreError::SlowDown,
            PushError::Oversized(_) => LoreError::Oversized,
            PushError::FileNotFound(_) => LoreError::FileNotFound,
            PushError::NotFound(_)
            | PushError::LayerNotFound(_)
            | PushError::RevisionNotFound(_) => LoreError::NotFound,
            PushError::AddressNotFound(_) => LoreError::AddressNotFound,
            PushError::PayloadNotFound(_) => LoreError::PayloadNotFound,
            PushError::InvalidPath(_) | PushError::InvalidArguments(_) => {
                LoreError::InvalidArguments
            }
            _ => LoreError::Internal,
        }
    }

    fn inner(&self) -> String {
        self.to_string()
    }
}

#[derive(Default)]
pub(crate) struct PushStatistics {
    pub fragment_count: AtomicUsize,
    pub fragment_complete: AtomicUsize,
    pub bytes_transferred: AtomicU64,
    pub bytes_total: AtomicU64,
}

pub async fn push(
    repository: Arc<RepositoryContext>,
    token: &RepositoryWriteToken,
    options: PushOptions,
) -> Result<(), PushError> {
    let branch;
    let local_latest;
    if let Some(branch_identifier) = &options.branch {
        let status = branch::resolve(repository.clone(), branch_identifier.as_str())
            .await
            .forward::<PushError>("resolving branch identifier")?;
        if !status.local {
            return Err(PushError::internal(
                "Unable to push a branch that does not exist in local repository",
            ));
        }
        branch = status.id;
        local_latest = status.latest;
    } else {
        (local_latest, branch) = crate::instance::load_current_anchor(&repository)
            .await
            .forward::<PushError>("loading current anchor")?;
    }

    let state_current = State::deserialize(repository.clone(), local_latest)
        .await
        .forward::<PushError>("deserializing current state")?;

    // Where the files of the repository are, for content the remote has to be given again
    let working_root = repository.path.clone();

    collect_fragments_and_push(
        repository.clone(),
        token,
        options.clone(),
        state_current,
        branch,
        local_latest,
        working_root.clone(),
    )
    .await?;

    if let Ok(layers) = layer::list(repository.clone()).await {
        for layer in layers {
            let repository = Arc::new(repository.to_layer_context(layer.repository).await);
            let state_current = State::deserialize(repository.clone(), layer.current)
                .await
                .forward::<PushError>("deserializing layer state")?;

            collect_fragments_and_push(
                repository.clone(),
                token,
                options.clone(),
                state_current,
                branch,
                layer.current,
                working_root.clone(),
            )
            .await?;
        }
    }

    let state_current = State::deserialize(repository.clone(), local_latest)
        .await
        .forward::<PushError>("re-deserializing current state for links")?;
    if let Ok(link_list) = state_current.link_list(repository.clone()).await {
        for link_reference in link_list.iter() {
            let link_repository =
                Arc::new(repository.to_link_context(link_reference.repository).await);
            let link_branch_id = link_reference.resolve_branch(branch);
            let link_local_latest = branch::load_latest(link_repository.clone(), link_branch_id)
                .await
                .unwrap_or_default();
            if link_local_latest.is_zero() {
                continue;
            }
            let link_state = State::deserialize(link_repository.clone(), link_local_latest)
                .await
                .forward::<PushError>("deserializing link state")?;

            let link_working_root = link_working_root(
                &repository,
                &state_current,
                link_reference.local_node,
                working_root.as_deref(),
            )
            .await;

            // The repository's own revision is on the remote by now, and pins these heads. So
            // any note of a head that waits for its pin is settled too, also one of an earlier
            // push that was answered too late to drop it.
            let link_moved = collect_fragments_and_push(
                link_repository.clone(),
                token,
                options.clone(),
                link_state,
                link_branch_id,
                link_local_latest,
                link_working_root,
            )
            .await?;
            settle_link_heads(link_moved).await;
            if !execution_context().globals().dry_run() {
                branch::clear_unpinned_push(link_repository, link_branch_id).await;
            }
        }
    }

    Ok(())
}

async fn collect_fragments_and_push(
    repository: Arc<RepositoryContext>,
    token: &RepositoryWriteToken,
    options: PushOptions,
    state: Arc<State>,
    branch: BranchId,
    local_latest: Hash,
    working_root: Option<PathBuf>,
) -> Result<Vec<LinkHeadMove>, PushError> {
    let mut moved = vec![];
    match push_revisions(
        repository,
        token,
        options,
        state,
        branch,
        local_latest,
        working_root,
        &mut moved,
    )
    .await
    {
        Ok(()) => Ok(moved),
        Err(err) => {
            withdraw_link_heads(moved).await;
            Err(err)
        }
    }
}

/// Push the revisions of one repository that its remote branch lacks, oldest first, each
/// after the links it pins. `moved` collects the heads of link branches this moves on the
/// remote and that the remote holds no pin for yet, see [`LinkHeadMove`].
#[allow(clippy::too_many_arguments)]
async fn push_revisions(
    repository: Arc<RepositoryContext>,
    token: &RepositoryWriteToken,
    options: PushOptions,
    state: Arc<State>,
    branch: BranchId,
    local_latest: Hash,
    working_root: Option<PathBuf>,
    moved: &mut Vec<LinkHeadMove>,
) -> Result<(), PushError> {
    let remote = repository
        .remote()
        .await
        .forward::<PushError>("acquiring remote")?;

    let revision_protocol = remote
        .revision(repository.id)
        .await
        .forward::<PushError>("acquiring revision protocol")?;

    let correlation_id = execution_context().globals().correlation_id.to_string();
    let storage_protocol = remote
        .session(repository.id, &correlation_id)
        .await
        .forward::<PushError>("opening storage session")?;

    let repository_metadata = repository::metadata_hash(repository.clone())
        .await
        .forward::<PushError>("loading repository metadata hash")?;
    let repository_metadata = repository::metadata(repository.clone(), repository_metadata)
        .await
        .forward::<PushError>("loading repository metadata")?;
    let default_branch = repository_metadata.default_branch;

    let mut full_local_history = vec![];
    let mut full_remote_history = vec![];
    let mut current_branch_remote_history = vec![];
    let mut remote_revision = None;
    let mut current_branch = branch;
    let mut current_revision = local_latest;

    // Get remote branch info
    let (mut remote_latest, remote_metadata, remote_deleted) = match branch::load_remote(
        remote.clone(),
        repository.id,
        current_branch,
    )
    .await
    {
        Ok(status) => (status.latest, status.metadata, status.deleted),
        Err(err) if err.is_branch_not_found() => (Hash::default(), Hash::default(), false),
        Err(err) => {
            lore_debug!(
                "Failed to load remote branch info, assuming branch does not exist on remote: {err}"
            );
            (Hash::default(), Hash::default(), false)
        }
    };

    while remote_revision.is_none() {
        let current_remote_latest = if current_branch != branch {
            match branch::load_remote(remote.clone(), repository.id, current_branch).await {
                Ok(status) => status.latest,
                Err(err) if err.is_branch_not_found() => Hash::default(),
                Err(err) => {
                    lore_debug!(
                        "Failed to load remote branch info for {current_branch}, assuming branch does not exist on remote: {err}"
                    );
                    Hash::default()
                }
            }
        } else {
            remote_latest
        };

        let branch_metadata = branch::metadata(repository.clone(), current_branch)
            .await
            .forward::<PushError>("loading branch metadata")?;
        let branch_metadata =
            branch::branch_metadata(repository.clone(), current_branch, &branch_metadata)
                .await
                .forward::<PushError>("loading branch metadata")?;

        let default_branch_point = BranchPoint::default();

        lore_debug!("Walking history for branch {current_branch} at revision {current_revision}");

        if current_remote_latest.is_zero() && (current_branch != default_branch) {
            lore_debug!("Remote latest is zero, collect revisions and continue");
            let branch_point = branch_metadata
                .stack
                .first()
                .map_or(&default_branch_point, |parent| parent);
            if branch_point.revision.is_zero() {
                return Err(PushError::internal(
                    "Invalid branch data, unknown branch point",
                ));
            }

            let branch_point_state = State::deserialize(repository.clone(), branch_point.revision)
                .await
                .forward::<PushError>("deserializing branch point state")?;

            let mut local_revision = current_revision;
            while local_revision != branch_point.revision {
                let revision_state = State::deserialize(repository.clone(), local_revision)
                    .await
                    .forward::<PushError>("deserializing revision state")?;

                if revision_state.revision_number() < branch_point_state.revision_number() {
                    return Err(PushError::internal("Local branch metadata is out of date"));
                }

                full_local_history.push(local_revision);
                local_revision = revision_state.parent_self();
            }

            current_revision = branch_point.revision;
            current_branch = branch_point.branch;

            // Early out - if the parent branch latest revision is convergent, it is known
            // to have been pushed and validated at some point. We don't need to iterate further
            // in that case, since there are no potentially missing fragments from this point
            if !branch::load_latest_divergent(repository.clone(), current_branch)
                .await
                .unwrap_or(true)
            {
                lore_debug!(
                    "Parent branch is known to be convergent, stop iterating revisions to push"
                );
                break;
            }
        } else if (current_remote_latest != state.parent_self()
            && current_remote_latest != state.parent_other())
            || current_remote_latest.is_zero()
        {
            lore_debug!("Found remote latest or reached initial branch");

            let (_branch_point, remote_history, local_history) = history::find_branch_point(
                repository.clone(),
                current_remote_latest,
                current_revision,
            )
            .await
            .forward::<PushError>("reconciling branch history")?;

            full_local_history.extend(local_history.clone());
            full_remote_history.extend(remote_history.clone());

            if current_branch == branch {
                current_branch_remote_history = remote_history;
            }

            // Either the remote latest was found or there is none
            if !current_remote_latest.is_zero() {
                remote_revision = Some(current_remote_latest);
                lore_debug!("Found remote latest {remote_latest}");
            } else {
                lore_debug!("Remote latest is zero, reached initial branch");
                break;
            }
        } else {
            lore_debug!("Only single revision to push");

            full_local_history.push(current_revision);
            remote_revision = Some(current_remote_latest);
        }
    }

    // Check if revision is already pushed and there is nothing to do
    let already_pushed = remote_latest == local_latest;

    let branch_metadata = branch::metadata(repository.clone(), branch)
        .await
        .forward::<PushError>("loading branch metadata")?;
    let branch_metadata = branch::branch_metadata(repository.clone(), branch, &branch_metadata)
        .await
        .forward::<PushError>("loading branch metadata")?;

    event::LoreEvent::BranchPush(LoreBranchPushEventData {
        remote: remote.remote_url().into(),
        repository: repository.id,
        branch,
        branch_name: branch_metadata.name.as_str().into(),
        remote_revision: remote_revision.unwrap_or_default(),
        local_revision: local_latest,
        remote_history: full_remote_history.len() as u64,
        local_history: full_local_history.len() as u64,
        flag_already_pushed: already_pushed.into(),
        flag_default: (branch == default_branch).into(),
        flag_link: repository.is_link().into(),
        flag_layer: repository.is_layer().into(),
    })
    .send();

    let dry_run = execution_context().globals().dry_run();

    // A push uploads what its revisions add to the revision below them and takes the remote
    // to hold that one. Check that it does, and put back what it lost, before building on it.
    if !dry_run {
        let base = match full_local_history.last() {
            Some(oldest) => State::deserialize(repository.clone(), *oldest)
                .await
                .forward::<PushError>("deserializing revision state")?
                .parent_self(),
            None => local_latest,
        };

        // The remote reports a head older than the one this workspace last saw there: it lost
        // state it had acknowledged, and what it kept may be incomplete too.
        let last_sync = branch::load_last_sync(repository.clone(), branch)
            .await
            .unwrap_or_default();
        let remote_rolled_back = !last_sync.is_zero() && full_local_history.contains(&last_sync);

        restore_remote_revisions(
            &repository,
            &storage_protocol,
            remote.environment.max_query_batch(),
            working_root.as_deref(),
            base,
            remote_rolled_back || execution_context().globals().force(),
        )
        .await?;
    }

    // If the revision is already pushed and the branch still exists, early out.
    // If the branch was deleted, restore it via branch_create before returning.
    if already_pushed {
        if remote_deleted && !dry_run {
            lore_debug!("Branch deleted on server with same latest, restoring via branch_create");
            revision_protocol
                .branch_create(
                    branch,
                    branch_metadata.name.as_str(),
                    branch_metadata.category.as_str(),
                    branch_metadata.creator.as_str(),
                    &branch_metadata.stack,
                )
                .await
                .forward::<PushError>("creating branch on remote")?;
        }
        return Ok(());
    }

    // The remote head of a link's branch can be a revision this workspace pushed for a push
    // the remote then did not accept, and never took back. Nothing pins it. When what is
    // pushed now does not build on it, it is replaced: left there, it refuses this push and
    // every later one. See [`LinkHeadMove`].
    let unpinned = if repository.is_link() && !dry_run {
        branch::load_unpinned_push(repository.clone(), branch).await
    } else {
        None
    };
    let replaces_unpinned = unpinned.is_some_and(|(_, pushed)| {
        pushed == remote_latest && !current_branch_remote_history.is_empty()
    });
    if replaces_unpinned {
        lore_debug!(
            "Remote head {remote_latest} of this link's branch is an earlier push of this workspace that was not accepted, replacing it"
        );
    }

    // If the branch diverged, early out (unless fast-forward merge is enabled,
    // in which case let the server attempt to resolve the divergence)
    let force = execution_context().globals().force() || replaces_unpinned;
    if !current_branch_remote_history.is_empty()
        && !force
        && !options.fast_forward_merge
        && !repository.is_link()
    {
        lore_debug!(
            "Branch divergence detected, {} remote changes",
            current_branch_remote_history.len()
        );
        return Err(PushError::internal(
            "Branch has diverged, sync to merge remote changes",
        ));
    }

    // If force pushing a current revision that's already pushed, add it
    if full_local_history.is_empty() && !local_latest.is_zero() && force {
        lore_debug!(
            "Branch push of old revision detected, {} remote changes",
            full_remote_history.len()
        );
        full_local_history.push(local_latest);
    }

    // If the branch was deleted on the server, restore it via branch_create
    if remote_deleted && !dry_run {
        lore_debug!("Branch deleted on server, restoring via branch_create before push");
        revision_protocol
            .branch_create(
                branch,
                branch_metadata.name.as_str(),
                branch_metadata.category.as_str(),
                branch_metadata.creator.as_str(),
                &branch_metadata.stack,
            )
            .await
            .forward::<PushError>("creating branch on remote")?;
    }

    // If this is the initial push of a branch, create it
    if remote_metadata.is_zero() {
        let branch_point = if let Some(parent) = branch_metadata.stack.first() {
            parent.revision
        } else {
            Hash::default()
        };

        event::LoreEvent::BranchPushBranchCreateBegin(LoreBranchPushBranchCreateBeginEventData {
            local_revision: branch_point,
        })
        .send();

        if !dry_run {
            remote_latest = revision_protocol
                .branch_create(
                    branch,
                    branch_metadata.name.as_str(),
                    branch_metadata.category.as_str(),
                    branch_metadata.creator.as_str(),
                    &branch_metadata.stack,
                )
                .await
                .forward::<PushError>("creating branch on remote")?;

            if remote_latest != branch_point {
                return Err(PushError::internal(format!(
                    "Failed to create branch {}, remote latest now at {}",
                    branch_metadata.name.clone(),
                    remote_latest
                )));
            }

            branch::store_last_sync(repository.clone(), branch, branch_point).await;
        } else {
            // Report the revision the branch creation would yield.
            remote_latest = branch_point;
        }

        event::LoreEvent::BranchPushBranchCreateEnd(LoreBranchPushBranchCreateEndEventData {
            remote_revision: remote_latest,
        })
        .send();
    }

    // Where the remote head of a link's branch goes back to if this push is not accepted
    let remote_before = remote_latest;
    let link_base = match unpinned {
        Some((base, _)) if replaces_unpinned => base,
        _ => remote_before,
    };

    let mut current_latest = Hash::default();
    let mut fast_forward_merged = false;
    for current_revision in full_local_history.iter().rev() {
        let mut current_revision = *current_revision;

        let state = State::deserialize(repository.clone(), current_revision)
            .await
            .forward::<PushError>("deserializing revision state")?;

        // Push links
        if let Ok(link_list) = state.link_list(repository.clone()).await {
            // TODO(vri): UCS-17135 - Push links in individual tasks
            for link_reference in link_list.iter() {
                let link_id = link_reference.repository;
                let link_repository = Arc::new(repository.to_link_context(link_id).await);
                let link_signature = link_reference.signature;
                let link_state = State::deserialize(link_repository.clone(), link_signature)
                    .await
                    .forward::<PushError>("deserializing link state")?;

                let link_branch_id = link_reference.resolve_branch(branch);

                lore_debug!(
                    "Pushing link changes for link ID {link_id} on branch {link_branch_id} at revision {link_signature}"
                );

                let link_working_root = link_working_root(
                    &repository,
                    &state,
                    link_reference.local_node,
                    working_root.as_deref(),
                )
                .await;

                match collect_fragments_and_push_recurse(
                    link_repository,
                    token.share(),
                    options.clone(),
                    link_state,
                    link_branch_id,
                    link_reference.signature,
                    link_working_root,
                )
                .await
                {
                    Ok(link_moved) => moved.extend(link_moved),
                    Err(err) => {
                        return Err(PushError::internal(format!(
                            "Failed to push link with ID {link_id}: {err}"
                        )));
                    }
                }
            }
        }

        if !current_latest.is_zero() && state.parent_self() != current_latest {
            // Rebase on new latest revision
            // TODO(mjansson): This only handles revision number rewrite for now, implement proper
            //                 automatic rebase if the push resulted in a clean rebase
            // ...

            event::LoreEvent::BranchPushRevisionUpdateBegin(
                LoreBranchPushRevisionUpdateBeginEventData {
                    revision: state.revision(),
                    old_parent: state.parent_self(),
                    new_parent: current_latest,
                },
            )
            .send();

            state.set_parent_self(current_latest);
            current_revision = state
                .serialize(repository.clone(), token)
                .await
                .forward::<PushError>("serializing state")?;

            event::LoreEvent::BranchPushRevisionUpdateEnd(
                LoreBranchPushRevisionUpdateEndEventData {
                    revision: current_revision,
                },
            )
            .send();
        }

        // Check missing fragments on server
        let stats = Arc::new(PushStatistics::default());
        let fragments = missing_fragments(
            &repository,
            &storage_protocol,
            remote.environment.max_query_batch(),
            working_root.as_deref(),
            &state,
            !dry_run,
        )
        .await?;

        event::LoreEvent::BranchPushFragmentBegin(LoreBranchPushFragmentBeginEventData {
            fragments: fragments.len() as u64,
            bytes_total: 0,
        })
        .send();

        let ticker_stats = stats.clone();
        let ticker = AbortOnDropHandle::new(lore_spawn!(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_millis(100));
            loop {
                ticker.tick().await;
                event::LoreEvent::BranchPushFragmentProgress(
                    LoreBranchPushFragmentProgressEventData {
                        complete: ticker_stats.fragment_complete.load(Ordering::Relaxed) as u64,
                        count: ticker_stats.fragment_count.load(Ordering::Relaxed) as u64,
                        bytes_transferred: ticker_stats.bytes_transferred.load(Ordering::Relaxed),
                        bytes_total: ticker_stats.bytes_total.load(Ordering::Relaxed),
                    },
                )
                .send();
            }
        }));

        if !dry_run {
            push_fragments(
                repository.clone(),
                storage_protocol.clone(),
                fragments,
                stats.clone(),
            )
            .await?;
        }

        drop(ticker);

        // Emit a final progress event with the completed values now that the
        // ticker has been dropped and push_fragments has finished.
        event::LoreEvent::BranchPushFragmentProgress(LoreBranchPushFragmentProgressEventData {
            complete: stats.fragment_complete.load(Ordering::Relaxed) as u64,
            count: stats.fragment_count.load(Ordering::Relaxed) as u64,
            bytes_transferred: stats.bytes_transferred.load(Ordering::Relaxed),
            bytes_total: stats.bytes_total.load(Ordering::Relaxed),
        })
        .send();

        event::LoreEvent::BranchPushFragmentEnd(LoreBranchPushFragmentEndEventData {
            fragments: stats.fragment_complete.load(Ordering::Relaxed) as u64,
            bytes_transferred: stats.bytes_transferred.load(Ordering::Relaxed),
        })
        .send();

        // We don't want to push revisions from any other branch than the current one,
        // so we will early out here
        if state.branch(repository.clone()).await != branch {
            continue;
        };

        event::LoreEvent::BranchPushRevisionPushBegin(LoreBranchPushRevisionPushBeginEventData {
            remote_revision: remote_latest,
            local_revision: current_revision,
        })
        .send();

        // Push new latest to remote
        let current_remote = remote_latest;
        let current_number;
        let mut response_message = None;

        if !dry_run && remote_latest != current_revision {
            if repository.is_link() {
                // Noted in the workspace before the remote is asked, and written out: this
                // push may not live to see the answer, and a note still in memory dies with it
                branch::store_unpinned_push(repository.clone(), branch, link_base, current_revision)
                    .await;
                let _ = repository.flush(false).await;
            }

            let push_result = revision_protocol
                .branch_push(branch, current_revision, force, options.fast_forward_merge)
                .await;

            // If the server returns NotFound, the branch was deleted on the server.
            // Recreate it via branch_create and retry the push.
            let response = match push_result {
                Err(ProtocolError::NotFound(_)) => {
                    lore_debug!("Branch push returned NotFound, recreating branch on server");

                    event::LoreEvent::BranchPushBranchCreateBegin(
                        LoreBranchPushBranchCreateBeginEventData {
                            local_revision: remote_latest,
                        },
                    )
                    .send();

                    revision_protocol
                        .branch_create(
                            branch,
                            branch_metadata.name.as_str(),
                            branch_metadata.category.as_str(),
                            branch_metadata.creator.as_str(),
                            &branch_metadata.stack,
                        )
                        .await
                        .forward::<PushError>("creating branch on remote")?;

                    event::LoreEvent::BranchPushBranchCreateEnd(
                        LoreBranchPushBranchCreateEndEventData {
                            remote_revision: remote_latest,
                        },
                    )
                    .send();

                    revision_protocol
                        .branch_push(branch, current_revision, force, options.fast_forward_merge)
                        .await
                        .forward::<PushError>("pushing branch to remote")?
                }
                result => result.forward::<PushError>("pushing branch to remote")?,
            };
            if response.fast_forward_merged {
                // Server performed a fast-forward merge — push succeeded with a new revision.
                // Store the server-created revision as local latest (marked divergent since the
                // local working directory still reflects the original merge revision).
                branch::store_latest(
                    repository.clone(),
                    branch,
                    response.revision,
                    BranchLatestStatus::Divergent,
                )
                .await
                .forward::<PushError>("setting new latest revision for branch")?;
                branch::store_last_sync(repository.clone(), branch, response.revision).await;

                remote_latest = response.revision;
                current_latest = response.revision;
                current_number = response.revision_number;

                event::LoreEvent::BranchPushRevisionPushEnd(
                    LoreBranchPushRevisionPushEndEventData {
                        old_remote_revision: current_remote,
                        new_remote_revision: current_latest,
                        new_remote_revision_number: current_number,
                        message: response.message.unwrap_or_default().into(),
                        fast_forward_merged: 1,
                    },
                )
                .send();

                // Skip the normal post-push processing — do not update anchor
                // or working directory. A subsequent `urc sync` will handle that.
                fast_forward_merged = true;
                if repository.is_link() {
                    note_link_head_move(moved, &repository, branch, link_base, remote_latest).await;
                } else {
                    settle_link_heads(moved.drain(..)).await;
                }
                continue;
            }
            if response.revision_number == 0 {
                if options.fast_forward_merge {
                    return Err(PushError::internal(
                        "Fast-forward merge failed due to conflicts, sync and merge locally to resolve",
                    ));
                }
                return Err(PushError::internal(format!(
                    "Remote latest has moved to {} and automatic rebase not possible",
                    response.revision
                )));
            }
            if response.revision != current_revision {
                event::LoreEvent::BranchPushRevisionPushUpdate(
                    LoreBranchPushRevisionPushUpdateEventData {
                        old_revision: current_revision,
                        new_revision: response.revision,
                        new_revision_number: response.revision_number,
                    },
                )
                .send();
            }
            response_message = response.message;

            remote_latest = response.revision;
            if repository.is_link() {
                // Its caller pins this head, or takes it back
                note_link_head_move(moved, &repository, branch, link_base, remote_latest).await;
            }
            current_latest = response.revision;
            current_number = State::deserialize(repository.clone(), current_latest)
                .await
                .forward::<PushError>("deserializing current latest state")?
                .revision_number();
        } else {
            current_latest = current_revision;
            current_number = State::deserialize(repository.clone(), current_latest)
                .await
                .forward::<PushError>("deserializing current latest state")?
                .revision_number();
        }

        event::LoreEvent::BranchPushRevisionPushEnd(LoreBranchPushRevisionPushEndEventData {
            old_remote_revision: current_remote,
            new_remote_revision: current_latest,
            new_remote_revision_number: current_number,
            message: response_message.unwrap_or_default().into(),
            fast_forward_merged: 0,
        })
        .send();

        if !dry_run {
            branch::store_last_sync(repository.clone(), branch, current_latest).await;

            // This revision is on the remote: so is what pins the heads of its links
            if !repository.is_link() {
                settle_link_heads(moved.drain(..)).await;
            }
        }
    }

    lore_debug!(
        "All revisions pushed, updating current local latest to {}",
        current_latest
    );
    if !current_latest.is_zero()
        && !repository.is_layer()
        && !repository.is_link()
        && !fast_forward_merged
        && !dry_run
    {
        branch::store_latest(
            repository.clone(),
            branch,
            current_latest,
            BranchLatestStatus::Convergent,
        )
        .await
        .forward::<PushError>("setting new latest revision for branch")?;

        branch::store_last_sync(repository.clone(), branch, current_latest).await;
    }

    Ok(())
}

fn collect_fragments_and_push_recurse(
    repository: Arc<RepositoryContext>,
    token: RepositoryWriteToken,
    options: PushOptions,
    state: Arc<State>,
    branch: BranchId,
    local_latest: Hash,
    working_root: Option<PathBuf>,
) -> Pin<Box<dyn Future<Output = Result<Vec<LinkHeadMove>, PushError>> + Send>> {
    Box::pin(async move {
        collect_fragments_and_push(
            repository,
            &token,
            options,
            state,
            branch,
            local_latest,
            working_root,
        )
        .await
    })
}

/// The branch of a linked repository that a push moved on the remote, from `base` to `pushed`,
/// before the remote accepted the revision that pins `pushed`: the one of the repository that
/// holds the link.
///
/// The links of a revision are pushed before the revision itself. If the remote then refuses
/// the revision (its branch moved in between, the branch is protected, a file is locked), or
/// the push stops there, the link's branch is left on a head that nothing on the remote pins
/// and that no workspace but this one builds on. Every later push of the link, by anyone, is
/// then not a fast-forward and is refused. So such a move is taken back when the push fails
/// ([`withdraw_link_heads`]), and it is noted in the workspace
/// ([`branch::store_unpinned_push`]) until the revision that pins it is accepted: a push that
/// could not take its move back (it was killed, or lost the connection) leaves the note, and
/// the next push of that link from this workspace replaces the head instead of building on it.
struct LinkHeadMove {
    repository: Arc<RepositoryContext>,
    branch: BranchId,
    base: Hash,
    pushed: Hash,
}

/// Note that the remote head of this link's branch is `pushed`, or will be once the push in
/// flight is answered, and that it was `base` before. The latest move replaces an earlier one
/// of the same branch in `moved`.
async fn note_link_head_move(
    moved: &mut Vec<LinkHeadMove>,
    repository: &Arc<RepositoryContext>,
    branch: BranchId,
    base: Hash,
    pushed: Hash,
) {
    branch::store_unpinned_push(repository.clone(), branch, base, pushed).await;
    moved.retain(|link| link.repository.id != repository.id || link.branch != branch);
    moved.push(LinkHeadMove {
        repository: repository.clone(),
        branch,
        base,
        pushed,
    });
}

/// The remote accepted the revision that pins these link heads: they are part of its history.
async fn settle_link_heads(moved: impl IntoIterator<Item = LinkHeadMove>) {
    for link in moved {
        branch::clear_unpinned_push(link.repository, link.branch).await;
    }
}

/// Put the branches of links back where they were, after a push that moved them and then
/// failed before the revision that pins their new heads was accepted.
///
/// A head is only moved back while it is still the one this push put there. Best effort: a
/// move that cannot be taken back now stays noted in the workspace, see [`LinkHeadMove`].
async fn withdraw_link_heads(moved: Vec<LinkHeadMove>) {
    for link in moved.into_iter().rev() {
        let Ok(remote) = link.repository.remote().await else {
            continue;
        };
        let Ok(status) =
            branch::load_remote(remote.clone(), link.repository.id, link.branch).await
        else {
            continue;
        };
        if status.latest != link.pushed {
            // The head was never moved, or somebody has built on it since
            branch::clear_unpinned_push(link.repository, link.branch).await;
            continue;
        }
        if link.base.is_zero() {
            continue;
        }
        let Ok(revision_protocol) = remote.revision(link.repository.id).await else {
            continue;
        };

        match revision_protocol
            .branch_push(link.branch, link.base, true, false)
            .await
        {
            Ok(_) => {
                lore_debug!(
                    "Took back the push of link {} branch {}: its head is {} again, not {}",
                    link.repository.id,
                    link.branch,
                    link.base,
                    link.pushed
                );
                branch::store_last_sync(link.repository.clone(), link.branch, link.base).await;
                branch::clear_unpinned_push(link.repository, link.branch).await;
            }
            Err(err) => {
                lore_debug!(
                    "Could not take back the push of link {} branch {} to {}: {err}",
                    link.repository.id,
                    link.branch,
                    link.pushed
                );
            }
        }
    }
}

/// Directory of the working tree the files of a linked repository are in: where the link sits
/// in the tree of the repository that holds it, below that repository's own directory.
async fn link_working_root(
    repository: &Arc<RepositoryContext>,
    state: &State,
    link_node: NodeID,
    working_root: Option<&Path>,
) -> Option<PathBuf> {
    let mount_path = state.node_path(repository.clone(), link_node).await.ok()?;
    Some(working_root?.join(mount_path))
}

/// The fragments a revision adds to its parent revision: what a remote that holds the parent
/// needs in order to hold the revision too.
///
/// Fragments the local store has flagged as durably stored are part of the list. The flag
/// records that an upload was acknowledged, not that the remote still has the fragment, and a
/// remote that lost state after acknowledging it is exactly the case in which a push has to
/// upload again. Which of these fragments are missing is the remote's to say, see
/// [`push_query`].
async fn collect_revision_fragments(
    repository: &Arc<RepositoryContext>,
    state: &Arc<State>,
) -> Result<Vec<Address>, PushError> {
    let state_parent = State::deserialize(repository.clone(), state.parent_self())
        .await
        .forward::<PushError>("deserializing parent state")?;

    lore_debug!(
        "Calculating new fragments from {} to {}",
        state_parent.revision(),
        state.revision()
    );
    let mut fragments = state::collect_new_fragments(
        repository.clone(),
        state_parent,
        state.clone(),
        false, /* Do not ignore fragments flagged as durably stored, the remote decides */
    )
    .await
    .forward::<PushError>("collecting new fragments")?;

    if !state.parent_other().is_zero() {
        fragments.push(Address::zero_context_hash(state.parent_other()));
    }

    Ok(fragments)
}

/// The fragments of a revision a push has to upload: those of
/// [`collect_revision_fragments`] the remote reports missing.
///
/// With `restore`, the ones the local store holds no payload for are first uploaded again
/// from the working tree (see [`restore_content_from_working_tree`]), so that everything
/// returned can be read from the local store.
async fn missing_fragments(
    repository: &Arc<RepositoryContext>,
    storage: &Arc<StorageSession>,
    max_query_batch: Option<usize>,
    working_root: Option<&Path>,
    state: &Arc<State>,
    restore: bool,
) -> Result<Vec<Address>, PushError> {
    // A fragmented file comes back one level per round: its fragment list has to be on the
    // remote again before the fragments it lists can be asked for
    const MAX_RESTORE_ROUNDS: usize = 8;

    let mut restored = vec![];
    for _ in 0..=MAX_RESTORE_ROUNDS {
        let fragments = collect_revision_fragments(repository, state).await?;
        let missing = push_query(storage.clone(), fragments, max_query_batch).await?;
        if !restore {
            return Ok(missing);
        }

        let lost = fragments_without_payload(repository, &missing).await;
        if lost.is_empty() {
            return Ok(missing);
        }
        if lost == restored {
            break;
        }

        restore_content_from_working_tree(repository, working_root, state, &lost).await?;
        restored = lost;
    }

    Err(PushError::internal(format!(
        "The remote does not hold all of revision {}, and uploading the content of its files again from the working tree did not complete it",
        state.revision()
    )))
}

/// Of `addresses`, the ones the local store cannot supply the payload of.
///
/// The content of a file is in this state from the moment its upload is acknowledged: the
/// local store then keeps the fragment, flagged as durably stored, and drops the payload.
async fn fragments_without_payload(
    repository: &Arc<RepositoryContext>,
    addresses: &[Address],
) -> Vec<Address> {
    let mut lost = vec![];
    for address in addresses {
        // As for an upload, the payload of the same content stored under another address will do
        let has_payload = repository
            .immutable_store()
            .query(repository.id, *address, store::StoreMatch::MatchFull)
            .await
            .is_ok_and(|query| {
                query.match_made != store::StoreMatch::MatchNone
                    && query.fragment.flags & fragment::FragmentFlags::PayloadStoredLocal != 0
            });
        if !has_payload {
            lost.push(*address);
        }
    }
    lost
}

/// Upload again, from the files of the working tree, content of `state` that neither the
/// remote nor the local store holds: the fragments in `lost`.
///
/// The context of a content address is the id of the file it belongs to, which is how the
/// file is found in the tree of `state`. A file is only used if it still has the content the
/// revision recorded for it. Its fragments in `lost` then stop being flagged as durably
/// stored, which they were not, and the file is written the way a commit writes it: that
/// uploads the fragments not flagged.
///
/// Fails if a fragment in `lost` is not the content of a file, or the file is not in the
/// working tree with that content any more. Such state is not recoverable from this
/// workspace.
async fn restore_content_from_working_tree(
    repository: &Arc<RepositoryContext>,
    working_root: Option<&Path>,
    state: &Arc<State>,
    lost: &[Address],
) -> Result<(), PushError> {
    let unrecoverable = |address: &Address| {
        PushError::internal(format!(
            "The remote does not hold fragment {address} of revision {}, and neither does the local store. It is not the content of a file in the working tree, so it cannot be uploaded again from this workspace",
            state.revision()
        ))
    };

    // The flag can only be taken back in a local store; any other kind keeps what it holds
    let local_store: Arc<dyn Any + Send + Sync> = repository.immutable_store();
    let (Some(working_root), Ok(local_store)) = (
        working_root,
        local_store.downcast::<lore_storage::LocalImmutableStore>(),
    ) else {
        return Err(unrecoverable(&lost[0]));
    };

    let file_ids: HashSet<Context> = lost.iter().map(|address| address.context).collect();
    let files = files_with_ids(repository, state, &file_ids).await?;
    if let Some(address) = lost.iter().find(|address| {
        !files
            .iter()
            .any(|(_, node)| node.address.context == address.context)
    }) {
        return Err(unrecoverable(address));
    }

    for (relative_path, node) in &files {
        let path = working_root.join(relative_path);
        let changed = || {
            PushError::internal(format!(
                "The remote does not hold the content of {} as committed in revision {}, and the file in the working tree no longer has that content. It cannot be uploaded again from this workspace",
                relative_path.display(),
                state.revision()
            ))
        };

        // Only the content the revision recorded may be stored under its address
        let hash = immutable::hash_file(
            repository.clone(),
            &path,
            Some(node.address),
            Some(node.size as usize),
        )
        .await;
        if !hash.is_ok_and(|hash| hash == node.address.hash) {
            return Err(changed());
        }

        lore_info!(
            "Remote no longer holds the content of {}, uploading it again from the working tree",
            relative_path.display()
        );

        for address in lost {
            if address.context == node.address.context {
                local_store
                    .mark_as_not_durably_stored(repository.id, *address)
                    .await;
            }
        }

        let (address, _) = immutable::write_from_file(
            repository.clone(),
            &path,
            node.address.context,
            immutable::write_options_from_repository(repository.clone()),
        )
        .await
        .forward::<PushError>("uploading file content from the working tree")?;
        if address != node.address {
            return Err(changed());
        }
    }

    Ok(())
}

/// The files in the tree of `state` whose file id is in `file_ids`, with their paths relative
/// to the root of the repository. Links are not followed: a linked repository is pushed as a
/// repository of its own.
async fn files_with_ids(
    repository: &Arc<RepositoryContext>,
    state: &Arc<State>,
    file_ids: &HashSet<Context>,
) -> Result<Vec<(PathBuf, Node)>, PushError> {
    let mut files = vec![];
    let mut directories = vec![(ROOT_NODE, PathBuf::new())];
    while let Some((directory, directory_path)) = directories.pop() {
        let mut children =
            StateNodeChildrenWithNameIterator::new(state.clone(), repository.clone(), directory)
                .await
                .forward::<PushError>("reading the tree of a revision")?;
        while let Some((child_id, child, name)) = children
            .next()
            .await
            .forward::<PushError>("reading the tree of a revision")?
        {
            // The name holds a lock on its node block, which reading the next child takes too
            let path = directory_path.join(name.freeze());
            if child.is_directory() {
                directories.push((child_id, path));
            } else if child.is_file() && file_ids.contains(&child.address.context) {
                files.push((path, child));
            }
        }
    }
    Ok(files)
}

/// Put back the revisions the remote is taken to hold and does not.
///
/// `base` is the revision a push builds on: the remote's head as it reports it, the point a
/// new branch starts from, or the local head when there is nothing to push. A remote that lost
/// state it had acknowledged, stopping before its stores reached the disk, may no longer be
/// able to load that revision, and nothing else would ever upload it again: every later push
/// is then rejected.
///
/// Walks back from `base` for as long as the remote lacks the revision, then uploads those
/// revisions oldest first, each as the fragments it adds to its parent that the remote reports
/// missing. With `verify_base`, the fragments of `base` are checked even if its revision is
/// there, for a remote known to have lost state: it may hold `base` in part.
///
/// Only the revisions found missing (and `base` with `verify_base`) have their fragments
/// checked. A remote that kept the revision fragment of an older revision but lost fragments
/// beneath it is not detected here.
async fn restore_remote_revisions(
    repository: &Arc<RepositoryContext>,
    storage: &Arc<StorageSession>,
    max_query_batch: Option<usize>,
    working_root: Option<&Path>,
    base: Hash,
    verify_base: bool,
) -> Result<(), PushError> {
    let mut lost = vec![];
    let mut revision = base;
    let mut verify = verify_base;
    while !revision.is_zero() {
        if !verify
            && push_query(
                storage.clone(),
                vec![Address::zero_context_hash(revision)],
                max_query_batch,
            )
            .await?
            .is_empty()
        {
            break;
        }
        verify = false;

        let state = State::deserialize(repository.clone(), revision)
            .await
            .forward::<PushError>("loading a revision the remote no longer holds")?;
        revision = state.parent_self();
        lost.push(state);
    }

    for state in lost.iter().rev() {
        let fragments = missing_fragments(
            repository,
            storage,
            max_query_batch,
            working_root,
            state,
            true,
        )
        .await?;
        if fragments.is_empty() {
            continue;
        }

        lore_debug!(
            "Remote lacks {} fragments of revision {}, uploading them again",
            fragments.len(),
            state.revision()
        );
        event::LoreEvent::BranchPushFragmentBegin(LoreBranchPushFragmentBeginEventData {
            fragments: fragments.len() as u64,
            bytes_total: 0,
        })
        .send();

        let stats = Arc::new(PushStatistics::default());
        push_fragments(
            repository.clone(),
            storage.clone(),
            fragments,
            stats.clone(),
        )
        .await?;

        event::LoreEvent::BranchPushFragmentEnd(LoreBranchPushFragmentEndEventData {
            fragments: stats.fragment_complete.load(Ordering::Relaxed) as u64,
            bytes_transferred: stats.bytes_transferred.load(Ordering::Relaxed),
        })
        .send();
    }

    Ok(())
}

pub const RETRY_START_DURATION: u64 = 100;
pub const RETRY_MAX_DURATION: u64 = 10_000;
pub const RETRY_MAX_ATTEMPTS: usize = 10;

pub(crate) async fn push_query(
    storage: Arc<StorageSession>,
    addresses: Vec<Address>,
    max_batch_size: Option<usize>,
) -> Result<Vec<Address>, PushError> {
    if addresses.is_empty() {
        return Ok(addresses);
    }

    let address_count = addresses.len();

    const MAX_TASK_COUNT: usize = 1000;

    let mut tasks = JoinSet::new();
    let mut remain = addresses;

    let mut failure = None;
    let mut missing = vec![];
    let mut retry =
        crate::util::time::retry(RETRY_START_DURATION, RETRY_MAX_DURATION, RETRY_MAX_ATTEMPTS);
    let max_batch_size = if let Some(max_batch_size) = max_batch_size
        && max_batch_size > 0
    {
        max_batch_size
    } else {
        1000
    };

    fn handle_join_result(
        result: Result<(Vec<Address>, Result<Bytes, ProtocolError>), JoinError>,
        remain: &mut Vec<Address>,
        missing: &mut Vec<Address>,
    ) -> Result<(), PushError> {
        let (mut batch, result) = result.internal("query task panicked")?;
        match result {
            Ok(result) => {
                for (index, value) in result.iter().enumerate() {
                    if *value != 0 && index < batch.len() {
                        missing.push(batch[index]);
                    }
                }
                Ok(())
            }
            Err(ProtocolError::SlowDown(_)) => {
                remain.append(&mut batch);
                Ok(())
            }
            err => err
                .map(|_| ())
                .forward::<PushError>("querying server for existing fragments"),
        }
    }

    while !remain.is_empty() {
        while !remain.is_empty() && failure.is_none() {
            let mut batch = remain.split_off(remain.len().saturating_sub(max_batch_size));

            let storage = storage.clone();
            lore_spawn!(tasks, async move {
                batch.sort_unstable();
                batch.dedup();

                let result = storage.query(batch.as_slice()).await;
                (batch, result)
            });

            while failure.is_none()
                && tasks.len() > MAX_TASK_COUNT
                && let Some(result) = tasks.join_next().await
            {
                failure = handle_join_result(result, remain.as_mut(), missing.as_mut()).err();
            }
        }

        while let Some(result) = tasks.join_next().await {
            if failure.is_none() {
                failure = handle_join_result(result, remain.as_mut(), missing.as_mut()).err();
            }
        }

        if let Some(failure) = failure {
            return Err(failure);
        }

        if !remain.is_empty() && !retry.wait().await {
            return Err(PushError::internal(
                "Failed to query server for existing fragments",
            ));
        }
    }

    missing.sort_unstable();
    missing.dedup();

    lore_debug!(
        "Queried {} fragments, {} missing",
        address_count,
        missing.len()
    );

    Ok(missing)
}

pub(crate) async fn push_fragments(
    repository: Arc<RepositoryContext>,
    storage: Arc<StorageSession>,
    fragments: Vec<Address>,
    stats: Arc<PushStatistics>,
) -> Result<(), PushError> {
    if fragments.is_empty() {
        return Ok(());
    }

    let fragment_count = fragments.len();

    stats
        .fragment_count
        .store(fragments.len(), Ordering::Relaxed);

    const MAX_PARALLEL_PUT: usize = 10000;

    let mut tasks: JoinSet<Result<(), PushError>> = JoinSet::new();
    let mut failure = None;
    for address in fragments {
        if address.hash.is_zero() {
            debug_assert!(
                !address.hash.is_zero(),
                "Zero hash address in list of fragments to push"
            );
            continue;
        }

        let repository = repository.clone();
        let storage = storage.clone();
        let stats = stats.clone();
        lore_spawn!(tasks, async move {
            let (fragment, payload) = match immutable::load_raw_store_retry(
                repository.immutable_store(),
                repository.id,
                address,
                store::StoreMatch::MatchFull,
            )
            .await
            {
                Ok((fragment, payload)) => (fragment, payload),
                Err(ref e) if e.is_address_not_found() || e.is_payload_not_found() => {
                    immutable::load_raw_store_retry(
                        repository.immutable_store(),
                        repository.id,
                        address,
                        store::StoreMatch::MatchHash,
                    )
                    .await
                    .forward::<PushError>("loading fragment payload")?
                }
                Err(err) => Err(err).forward::<PushError>("loading fragment payload")?,
            };

            let payload_size = payload.len() as u64;
            stats.bytes_total.fetch_add(payload_size, Ordering::Relaxed);

            immutable::store_raw_remote_retry(storage.clone(), address, fragment, Some(payload))
                .await
                .map_err(|err| {
                    if err.is_disconnected() {
                        PushError::from(Disconnected)
                    } else {
                        PushError::internal_with_context(err, "putting fragment to remote")
                    }
                })?;

            stats
                .bytes_transferred
                .fetch_add(payload_size, Ordering::Relaxed);

            // Mark as durably stored in local store
            let mut fragment = fragment;
            fragment.flags |= fragment::FragmentFlags::PayloadStoredDurable;
            let _ = repository
                .immutable_store()
                .put(repository.id, address, fragment, None, false)
                .await;

            stats.fragment_complete.fetch_add(1, Ordering::Relaxed);

            Ok(())
        });

        while let Some(result) = tasks.try_join_next() {
            failure = failure.or(result
                .map_err(|e| PushError::internal_with_context(e, "fragment task panicked"))
                .flatten()
                .err());
        }
        while tasks.len() > MAX_PARALLEL_PUT
            && let Some(result) = tasks.join_next().await
        {
            failure = failure.or(result
                .map_err(|e| PushError::internal_with_context(e, "fragment task panicked"))
                .flatten()
                .err());
        }
        if failure.is_some() {
            break;
        }
    }

    while let Some(result) = tasks.join_next().await {
        failure = failure.or(result
            .map_err(|e| PushError::internal_with_context(e, "fragment task panicked"))
            .flatten()
            .err());
    }

    if let Some(err) = failure {
        return Err(err);
    }

    lore_debug!("Pushed {} fragments", fragment_count);

    Ok(())
}
