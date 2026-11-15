// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::net::IpAddr;
use std::sync::Arc;

use lore_base::error::WriteRequired;
use lore_base::lore_spawn;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Hash;
use lore_error_set::prelude::*;
use lore_proto::BranchPushRequest;
use lore_proto::BranchPushResponse;
use lore_revision::branch;
use lore_revision::branch::BranchError;
use lore_revision::branch::LATEST;
use lore_revision::branch::PROTECT;
use lore_revision::branch::load_latest;
use lore_revision::branch::metadata;
use lore_revision::branch::push;
use lore_revision::lore::BranchId;
use lore_revision::lore::RepositoryId;
use lore_revision::notification::NotificationSender;
use lore_revision::repository;
use lore_revision::repository::RepositoryContext;
use lore_revision::state;
use lore_revision::state::State;
use lore_storage::StoreError;
use lore_storage::StoreMatch;
use lore_telemetry::InstrumentProvider;
use lore_telemetry::tracing::fields::BRANCH_ID;
use lore_telemetry::tracing::fields::REVISION;
use tokio::task::JoinSet;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::Instrument;
use tracing::Level;
use tracing::debug;
use tracing::instrument;
use tracing::span;
use tracing::warn;

use crate::cache;
use crate::grpc::FilterSlowDownExt;
use crate::grpc::ServerResultExt;
use crate::grpc::extract_correlation_id;
use crate::grpc::get_authorization;
use crate::grpc::get_repository;
use crate::grpc::get_user_id;
use crate::grpc::get_write_token;
use crate::grpc::handlers::push_locks::PushLocks;
use crate::grpc::hook_error_to_status;
use crate::grpc::warn_error_to_status;
use crate::hooks::HookContext;
use crate::hooks::HookDispatcher;
use crate::hooks::HookPoint;
use crate::util::setup_execution;

pub(crate) fn extract_client_ip<T>(request: &Request<T>) -> Option<IpAddr> {
    // try to get the LAST entry from XFF metadata header (injected by ALB)
    if let Some(ip_str) = request
        .metadata()
        .get("x-forwarded-for")
        .and_then(|header_value| header_value.to_str().ok())
        .and_then(|ip_list| ip_list.rsplit(',').next()) // use the last value, if multiple are present
        .map(str::trim)
        && let Ok(ip) = ip_str.parse::<IpAddr>()
    {
        return Some(ip);
    }

    // if XFF is not available, fallback to using remote_addr()
    request.remote_addr().map(|socket_addr| socket_addr.ip())
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(name = "BranchPush::handle", skip_all)]
pub async fn handler(
    request: Request<BranchPushRequest>,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    notification: Arc<dyn NotificationSender>,
    hook_dispatcher: &HookDispatcher,
    history_step_size: u64,
    acceleration: crate::grpc::server::RevisionListAcceleration,
    instrument_provider: &impl InstrumentProvider,
) -> Result<Response<BranchPushResponse>, Status> {
    let user_info = get_authorization(request.extensions());
    let user_id = get_user_id(request.extensions());
    let correlation_id = extract_correlation_id(&request).unwrap_or_default();
    let repository = get_repository(request.metadata())?;

    // TODO(mjansson): Once we have authz permission model with read/write/admin
    // this should be upgraded to check for the correct permission rather than
    // hardwired to service accounts. For now used to protect while allowing mirroring
    let mut bypass_protection = false;
    if let Ok(user_info) = user_info
        && user_info.is_service_account.unwrap_or_default()
    {
        bypass_protection = true;
    }

    let client_ip: Option<String> = extract_client_ip(&request).map(|ip_addr| ip_addr.to_string());
    let req = request.into_inner();
    let branch = BranchId::from(req.branch);
    let revision = Hash::from(req.revision);
    let force = req.force;
    let fast_forward_merge = req.fast_forward_merge;

    if revision.is_zero() {
        warn!("Invalid branch push request, revision is zero");
        return Err(Status::invalid_argument("Invalid revision"));
    }

    debug!({REVISION} = %revision, bypass_protection, {BRANCH_ID} = %branch, force, fast_forward_merge,
        "Handling branch push request",
    );

    let repository = Arc::new(RepositoryContext::new_server_context(
        immutable_store,
        mutable_store,
        repository,
    ));

    let repository_id: RepositoryId = repository.id;

    let execution = setup_execution(module_path!(), correlation_id.clone(), user_id.clone());

    LORE_CONTEXT
        .scope(execution, async move {
            let mut ctx_builder = HookContext::builder()
                .correlation_id(correlation_id.clone())
                .hook_point(HookPoint::BranchPush)
                .repository(repository_id)
                .user(user_id.clone())
                .branch(branch)
                .revision(revision);

            if let Some(ip) = client_ip {
                ctx_builder = ctx_builder.metadata("client_ip", ip);
            }

            let mut hook_ctx = ctx_builder.build();

            hook_dispatcher
                .dispatch_pre(HookPoint::BranchPush, &hook_ctx)
                .map_err(hook_error_to_status)?;

            let PushResult {
                success,
                fast_forward_merged,
                revision,
                revision_number,
            } = push(
                repository.clone(),
                branch,
                revision,
                bypass_protection,
                force,
                fast_forward_merge,
                history_step_size,
                acceleration,
            )
            .await?;

            if success {
                lore_spawn!({
                    let user_id = user_id.clone();
                    async move {
                        notification
                            .branch_pushed(
                                repository_id,
                                branch,
                                &user_id,
                                revision,
                                revision_number,
                            )
                            .instrument(span!(Level::DEBUG, "publish_notification"))
                            .await;
                    }
                    .in_current_span()
                });

                // Post-hook dispatch (async, non-blocking)
                hook_ctx.set_revision_number(revision_number);
                hook_dispatcher.spawn_post(HookPoint::BranchPush, hook_ctx);
            }

            let num_branches_pushed = instrument_provider.counter("num_branches_pushed");
            num_branches_pushed.add(1, &[]);

            let message = if success {
                dispatch_response_message(
                    hook_dispatcher,
                    &correlation_id,
                    &user_id,
                    repository_id,
                    branch,
                    revision,
                    repository.clone(),
                )
                .await
            } else {
                None
            };

            Ok(Response::new(BranchPushResponse {
                success,
                fast_forward_merged,
                revision: revision.into(),
                revision_number,
                message,
            }))
        })
        .await
}

/// Pre-computes repository and branch metadata, then dispatches response hooks
/// to generate an optional message for the client.
///
/// Metadata lookup failures are silently ignored — absent metadata keys cause
/// response hooks to return an empty response.
pub(crate) async fn dispatch_response_message(
    hook_dispatcher: &HookDispatcher,
    correlation_id: &str,
    user_id: &str,
    repository_id: RepositoryId,
    branch: BranchId,
    revision: Hash,
    repository: Arc<RepositoryContext>,
) -> Option<String> {
    let mut builder = HookContext::builder()
        .correlation_id(correlation_id)
        .hook_point(HookPoint::BranchPush)
        .repository(repository_id)
        .user(user_id)
        .branch(branch)
        .revision(revision);

    if let Ok(metadata_hash) = repository::metadata_hash(repository.clone()).await
        && let Ok(repository_metadata) =
            repository::metadata(repository.clone(), metadata_hash).await
    {
        builder = builder
            .metadata("repository_name", repository_metadata.name.clone())
            .metadata(
                "default_branch_name",
                &repository_metadata.default_branch_name,
            )
            .metadata(
                "is_default_branch",
                if repository_metadata.default_branch == branch {
                    "true"
                } else {
                    "false"
                },
            );
    }

    if let Ok(branch_meta) = branch::metadata(repository.clone(), branch).await
        && let Ok(branch_meta) =
            branch::branch_metadata(repository.clone(), branch, &branch_meta).await
    {
        builder = builder.metadata("branch_name", branch_meta.name);
    }

    let response_ctx = builder.build();
    hook_dispatcher
        .dispatch_response(HookPoint::BranchPush, &response_ctx)
        .message
}

pub struct PushResult {
    pub success: bool,
    pub fast_forward_merged: bool,
    pub revision: Hash,
    pub revision_number: u64,
}

/// Push without lock enforcement, for callers that have no lock store.
#[allow(clippy::too_many_arguments)]
pub async fn push(
    repository: Arc<RepositoryContext>,
    branch: BranchId,
    latest: Hash,
    bypass_protection: bool,
    force: bool,
    fast_forward_merge: bool,
    history_step_size: u64,
    acceleration: crate::grpc::server::RevisionListAcceleration,
) -> Result<PushResult, Status> {
    push_with_locks(
        repository,
        branch,
        latest,
        bypass_protection,
        force,
        fast_forward_merge,
        history_step_size,
        acceleration,
        PushLocks::disabled(),
    )
    .await
}

/// Move the head of `branch` to `latest`, unless `locks` finds that the move changes a path
/// somebody else holds a lock on.
#[allow(clippy::too_many_arguments)]
#[instrument(level = "debug", skip_all, fields(branch))]
pub async fn push_with_locks(
    repository: Arc<RepositoryContext>,
    branch: BranchId,
    latest: Hash,
    bypass_protection: bool,
    force: bool,
    fast_forward_merge: bool,
    history_step_size: u64,
    acceleration: crate::grpc::server::RevisionListAcceleration,
    locks: PushLocks<'_>,
) -> Result<PushResult, Status> {
    // Zero hash is never valid to push as latest pointer
    if latest.is_zero() {
        return Err(Status::failed_precondition("invalid revision signature"));
    }

    // Check if branch is protected
    let branch_metadata = metadata(repository.clone(), branch)
        .await
        .warn_map_err(|err| Status::internal(format!("Failed to load branch metadata: {err}")))?;

    if branch_metadata.get_bool(PROTECT).unwrap_or_default() {
        if bypass_protection {
            debug!("Bypass branch protection and push to protected branch");
        } else {
            warn!("Branch push failed, branch is protected");
            return Err(Status::permission_denied("protected"));
        }
    }

    // Verify the branch has not been deleted by checking the name→id mapping
    if let Ok(branch_name) = branch::name(&branch_metadata)
        && !branch_name.is_empty()
    {
        let is_mapped = branch::load_name_to_id_local(repository.clone(), branch_name)
            .await
            .is_ok_and(|id| id == branch);
        if !is_mapped {
            debug!("Branch push rejected, name-to-id mapping missing for deleted branch");
            return Err(Status::not_found("Branch not found"));
        }
    }

    let mut current_head = load_latest(repository.clone(), branch)
        .await
        .unwrap_or_default();

    // Verify the validity of the revision to push to latest
    let state = State::deserialize(repository.clone(), latest)
        .await
        .filter_slow_down()?
        .warn_map_err(|err| {
            Status::internal(format!("Failed to load incoming revision state: {err}"))
        })?;

    // If the incoming revision is already the latest revision the push is a no-op
    if current_head == latest {
        // No-op for the branch, not for the answer: the push that moved the head here may
        // have failed between the swap and making it durable, and this is its retry. Success
        // means the same as on the first attempt.
        flush_published(&repository).await.warn_map_err(|err| {
            Status::internal(format!("Failed to store new latest pointer: {err}"))
        })?;

        return Ok(PushResult {
            success: true,
            fast_forward_merged: false,
            revision: current_head,
            revision_number: state.revision_number(),
        });
    }

    let mut new_head = latest;
    loop {
        // Verify the current latest revision is parent of the incoming revision unless the push is forced
        if current_head != state.parent_self() && !force {
            if current_head.is_zero() {
                warn!("Branch push failed, branch does not exist");
                return Err(Status::not_found("Branch not found"));
            }

            if !fast_forward_merge {
                return Ok(PushResult {
                    success: false,
                    fast_forward_merged: false,
                    revision: current_head,
                    revision_number: 0,
                });
            }

            // Fast-forward merge: the incoming revision's parent_self no longer matches
            // the branch head. Attempt to create a new merge revision with
            // parent_self=current_head and parent_other=incoming_revision.
            return try_fast_forward_merge(
                repository.clone(),
                branch,
                state.clone(),
                current_head,
                history_step_size,
                acceleration,
                locks,
            )
            .await;
        }

        let state_parent = State::deserialize(repository.clone(), state.parent_self())
            .await
            .filter_slow_down()?
            .warn_map_err(|err| {
                Status::internal(format!(
                    "Failed to load parent state of incoming revision: {err}"
                ))
            })?;

        // Verify that all new fragments exist
        let mut state_other = None;
        if !state.parent_other().is_zero() {
            let state_parent = State::deserialize(repository.clone(), state.parent_other())
                .await
                .filter_slow_down()?
                .warn_map_err(|err| {
                    Status::internal(format!("Failed to load other parent state: {err}"))
                })?;
            state_other = Some(state_parent);
        }

        verify_fragments(repository.clone(), state_parent.clone(), state.clone()).await?;
        locks
            .ensure_unlocked(
                &repository,
                branch,
                current_head,
                Some(&state_parent),
                &state,
            )
            .await?;

        // Verify that the revision number is valid
        let revision_number = next_revision_number(
            state_parent.revision_number(),
            state_other.as_ref().map_or(0, |s| s.revision_number()),
        );

        if state.revision_number() != revision_number {
            // Rewrite the revision with a correct revision number
            state.set_revision_number(revision_number);
            let write_token = get_write_token();
            new_head = state
                .serialize(repository.clone(), &write_token)
                .await
                .warn_map_err(|err| {
                    Status::internal(format!("Failed to serialize state: {err}"))
                })?;
        }

        let previous_head = try_store_latest(repository.clone(), branch, current_head, new_head)
            .await
            .warn_map_err(|err| {
                Status::internal(format!("Failed to store new latest pointer: {err}"))
            })?;

        // Check if the compare-and-swap was successful by checking match with expected value
        if previous_head == current_head {
            // If equal it means the value was swapped, i.e the push was successful. Set the
            // new latest revision signature and break out of the loop to return success
            current_head = new_head;

            store_history_step(
                repository.clone(),
                branch,
                state_parent.revision_number(),
                history_step_size,
                acceleration,
                state.clone(),
            )
            .await;

            break;
        }

        // Latest pointer moved during the processing of this push call, loop and try again
        current_head = previous_head;
    }

    Ok(PushResult {
        success: true,
        fast_forward_merged: false,
        revision: current_head,
        revision_number: state.revision_number(),
    })
}

/// Attempts a server-side fast-forward merge when the target branch head has moved
/// since the client created the merge revision.
///
/// Creates a new merge revision with:
/// - `parent_self` = current branch head (target branch)
/// - `parent_other` = the incoming merge revision
///
/// Uses a three-way diff between the original merge base, the incoming revision,
/// and the current head. If conflicts are detected, returns failure so the client
/// can resolve locally.
///
/// Retries via CAS loop if the branch head moves again during processing.
#[instrument(level = "debug", skip_all)]
async fn try_fast_forward_merge(
    repository: Arc<RepositoryContext>,
    branch: BranchId,
    incoming_state: Arc<State>,
    mut current_head: Hash,
    history_step_size: u64,
    acceleration: crate::grpc::server::RevisionListAcceleration,
    locks: PushLocks<'_>,
) -> Result<PushResult, Status> {
    let incoming_revision = incoming_state.revision();
    let original_base = incoming_state.parent_self();

    debug!(
        %incoming_revision, %original_base, %current_head,
        "Attempting fast-forward merge"
    );

    // Verify that all new fragments from the incoming revision exist in the store,
    // matching the verification done in the normal push path. Without this check a
    // client could reference fragments that were never fully uploaded.
    let base_state = State::deserialize(repository.clone(), original_base)
        .await
        .filter_slow_down()?
        .warn_map_err(|err| {
            Status::internal(format!(
                "Failed to load base state for fragment verification: {err}"
            ))
        })?;

    verify_fragments(repository.clone(), base_state, incoming_state.clone()).await?;

    loop {
        // Three-way diff: base=original merge target, source=incoming merge, target=current head
        debug!(
            %original_base, %incoming_revision, %current_head,
            "Computing diff3 for fast-forward merge"
        );
        let diff_result = lore_revision::revision::diff3_collect(
            repository.clone(),
            original_base,
            incoming_revision,
            current_head,
            None,
            false,
        )
        .await
        .warn_map_err(|err| {
            Status::internal(format!(
                "Failed to compute diff3 for fast-forward merge: {err}"
            ))
        })?;
        debug!(
            changes = diff_result.changes.len(),
            conflicts = diff_result.conflicts.len(),
            "diff3 result for fast-forward merge"
        );

        if !diff_result.conflicts.is_empty() {
            debug!(
                conflicts = diff_result.conflicts.len(),
                "Fast-forward merge has conflicts, rejecting"
            );
            return Ok(PushResult {
                success: false,
                fast_forward_merged: false,
                revision: current_head,
                revision_number: 0,
            });
        }

        // Deserialize the current head state to use as base for the new merge revision
        let state_current = State::deserialize(repository.clone(), current_head)
            .await
            .filter_slow_down()?
            .warn_map_err(|err| {
                Status::internal(format!(
                    "Failed to deserialize current head for fast-forward merge: {err}"
                ))
            })?;

        // Apply the non-conflicting changes to the current head state
        state::apply_tree_changes(
            repository.clone(),
            state_current.clone(),
            &diff_result.changes,
        )
        .await
        .warn_map_err(|err| {
            Status::internal(format!(
                "Failed to apply tree changes for fast-forward merge: {err}"
            ))
        })?;

        // Set parents: self=current head (target branch), other=incoming merge revision
        state_current.set_parent_self(current_head);
        state_current.set_parent_other(incoming_revision);

        // Compute revision number from both parents
        let state_current_number = {
            let parent_state = State::deserialize(repository.clone(), current_head)
                .await
                .filter_slow_down()?
                .warn_map_err(|err| {
                    Status::internal(format!("Failed to load current head state: {err}"))
                })?;
            parent_state.revision_number()
        };
        let revision_number =
            next_revision_number(state_current_number, incoming_state.revision_number());
        state_current.set_revision_number(revision_number);

        // Copy metadata from the incoming revision and set merged-by to "server"
        let incoming_metadata_hash = incoming_state.metadata_hash();
        if !incoming_metadata_hash.is_zero() {
            let mut metadata = lore_revision::metadata::Metadata::deserialize(
                repository.clone(),
                incoming_metadata_hash,
            )
            .await
            .warn_map_err(|err| {
                Status::internal(format!("Failed to load incoming revision metadata: {err}"))
            })?;

            metadata
                .set_branch(branch)
                .warn_map_err(|_| Status::internal("Failed to set branch in metadata"))?;
            // Preserve the existing merged-by field if set, otherwise fall back to "server"
            if metadata
                .get_string(lore_revision::metadata::MERGED_BY)
                .is_err()
            {
                metadata
                    .set_string(lore_revision::metadata::MERGED_BY, "server")
                    .warn_map_err(|_| Status::internal("Failed to set merged-by in metadata"))?;
            }
            metadata
                .set_u64(lore_revision::metadata::FAST_FORWARD_MERGE, 1)
                .warn_map_err(|_| {
                    Status::internal("Failed to set fast-forward-merge in metadata")
                })?;

            let metadata_hash = metadata
                .serialize(repository.clone())
                .await
                .warn_map_err(|_| Status::internal("Failed to serialize metadata"))?;
            state_current.set_metadata_hash(metadata_hash);
        }

        // Serialize the new merge state
        let write_token = get_write_token();
        let new_revision = state_current
            .serialize(repository.clone(), &write_token)
            .await
            .warn_map_err(|err| {
                Status::internal(format!(
                    "Failed to serialize fast-forward merge state: {err}"
                ))
            })?;

        locks
            .ensure_unlocked(&repository, branch, current_head, None, &state_current)
            .await?;

        // CAS: attempt to set the new revision as branch head
        let previous_head =
            try_store_latest(repository.clone(), branch, current_head, new_revision)
                .await
                .warn_map_err(|err| {
                    Status::internal(format!("Failed to store fast-forward merge latest: {err}"))
                })?;

        if previous_head == current_head {
            // CAS succeeded
            debug!(
                %new_revision, revision_number,
                "Fast-forward merge succeeded"
            );

            // Store acceleration index if needed
            store_history_step(
                repository.clone(),
                branch,
                state_current_number,
                history_step_size,
                acceleration,
                state_current.clone(),
            )
            .await;

            return Ok(PushResult {
                success: true,
                fast_forward_merged: true,
                revision: new_revision,
                revision_number,
            });
        }

        // CAS failed — branch head moved again, retry with updated head
        debug!(
            %previous_head, %current_head,
            "Fast-forward merge CAS failed, retrying"
        );
        current_head = previous_head;
    }
}

/// Compute the next revision number from the parent revision numbers.
/// The revision number is one greater than the maximum of the two parents.
fn next_revision_number(parent_self_number: u64, parent_other_number: u64) -> u64 {
    std::cmp::max(parent_self_number, parent_other_number) + 1
}

/// Store the history-step skip pointer (if a boundary was crossed) and any
/// revision-list cache entries for segments newly closed by this push.
///
/// A segment `B` (= `N * history_step_size`) is *closed* by this push iff
/// `parent_revision_number <= B < revision_number`. A single push can close
/// multiple segments (e.g. a merge that jumps past several boundaries). For
/// each closed segment we walk `parent_self` from `state` and persist the
/// items whose number falls in `(B - step, B]`.
///
/// Errors are ignored — this is purely an acceleration construct and will be
/// recreated on the next lookup if any step fails.
async fn store_history_step(
    repository: Arc<RepositoryContext>,
    branch: BranchId,
    parent_revision_number: u64,
    history_step_size: u64,
    acceleration: crate::grpc::server::RevisionListAcceleration,
    state: Arc<State>,
) {
    let revision_number = state.revision_number();
    let revision = state.revision();

    if acceleration.step_keys
        && parent_revision_number / history_step_size != revision_number / history_step_size
    {
        let (key, key_type) = branch::revision_step_key(
            repository::SALT_LORE,
            repository.id,
            branch,
            revision_number,
            history_step_size,
        );
        let write_token = get_write_token();
        let _ = repository
            .clone()
            .write_mutable_store(&write_token)
            .store(repository.id, key, revision, key_type)
            .await;
    }

    if !acceleration.list_cache {
        return;
    }

    // Determine which segment boundaries are *newly closed* by this push.
    // A boundary B (multiple of history_step_size) is newly closed iff
    // P <= B < N (where P = parent_revision_number, N = revision_number).
    let lowest_b = parent_revision_number.div_ceil(history_step_size) * history_step_size;
    let highest_b = if revision_number > 0 {
        ((revision_number - 1) / history_step_size) * history_step_size
    } else {
        return;
    };
    if lowest_b == 0 || lowest_b > highest_b {
        return;
    }

    // Walk parent chain from the new revision until we cross below the lowest
    // closed segment, capturing items for each closed boundary.
    let stop_below = lowest_b.saturating_sub(history_step_size);
    let span_segments = (highest_b.saturating_sub(lowest_b) / history_step_size) + 1;
    let max_items = (span_segments as usize)
        .saturating_mul(history_step_size as usize)
        // Allow a small overshoot so partial segments above the closed range
        // (the still-open one containing N) and the one terminator item can
        // still be walked.
        .saturating_add(history_step_size as usize)
        .saturating_add(1);

    let walk =
        cache::revision::walk_segment_revisions(&repository, revision, stop_below, max_items).await;

    if !walk.reached_terminator {
        // Walk was bounded by max_items; the last segment may be partial.
        // Skip cache writes — next reader will rebuild them via backfill.
        return;
    }

    let segments = cache::revision::partition_into_segments(&walk.items, history_step_size);
    for (segment_b, list) in segments {
        if segment_b >= lowest_b && segment_b <= highest_b {
            cache::revision::store_cached_list(
                &repository,
                branch,
                segment_b,
                history_step_size,
                &list,
            )
            .await;
        }
    }
}

/// Verify that all new fragments between `parent_state` and `state` exist in the
/// immutable store. Also includes the other parent hash if the state is a merge.
/// Returns an error if any fragment is missing.
async fn verify_fragments(
    repository: Arc<RepositoryContext>,
    parent_state: Arc<State>,
    state: Arc<State>,
) -> Result<(), Status> {
    let mut new_fragments = state::collect_new_fragments(
        repository.clone(),
        parent_state.clone(),
        state.clone(),
        true, /* Ignore already durably stored fragments */
    )
    .instrument(span!(Level::DEBUG, "collect_new_fragments"))
    .await
    .warn_map_err(|err| {
        if let Some(converted_error) = err.as_address_not_found() {
            return Status::not_found(format!(
                "Failed to collect new fragments for verification. Missing address '{converted_error}'"
            ));
        }

        Status::internal(format!(
            "Failed to collect new fragments for verification: {err}"
        ))
    })?;

    if !state.parent_other().is_zero() {
        new_fragments.push(Address::zero_context_hash(state.parent_other()));
    }

    new_fragments.sort_unstable();
    new_fragments.dedup();

    let mut retry = lore_revision::util::time::retry(
        push::RETRY_START_DURATION,
        push::RETRY_MAX_DURATION,
        push::RETRY_MAX_ATTEMPTS,
    );

    let max_batch_size = repository
        .immutable_store()
        .max_query_batch()
        .unwrap_or(1000)
        .clamp(100, 10000);

    let mut tasks = JoinSet::new();
    while !new_fragments.is_empty() || !tasks.is_empty() {
        let batch_span = span!(
            Level::DEBUG,
            "exist_batch",
            items = new_fragments.len(),
            batch_size = max_batch_size
        );

        batch_span.in_scope(|| {
            while !new_fragments.is_empty() {
                let repository = repository.clone();
                let batch =
                    new_fragments.split_off(new_fragments.len().saturating_sub(max_batch_size));
                lore_spawn!(
                    tasks,
                    async move {
                        let result = repository
                            .immutable_store()
                            .exist_batch(repository.id, batch.as_slice(), StoreMatch::MatchFull)
                            .await;
                        (batch, result)
                    }
                    .in_current_span()
                );
            }
        });

        let mut num_slow_downs = 0;
        while let Some(result) = tasks.join_next().await {
            let (mut batch, result) =
                result.warn_map_err(|err| Status::internal(format!("Query task failed: {err}")))?;
            match result {
                Ok(result) => {
                    if result.iter().enumerate().any(|(pos, match_found)| {
                        if *match_found != StoreMatch::MatchFull {
                            warn!("Branch push failed, fragment not found for {}", batch[pos]);
                            true
                        } else {
                            false
                        }
                    }) {
                        return Err(Status::failed_precondition("Missing fragments"));
                    }
                }
                Err(StoreError::SlowDown(_)) => {
                    new_fragments.append(&mut batch);
                    num_slow_downs += 1;
                }
                Err(err) => {
                    let response = warn_error_to_status(&err, |err| {
                        Status::internal(format!("Store query failed: {err}"))
                    });
                    return Err(response);
                }
            }
        }

        if !new_fragments.is_empty() && !retry.wait().await {
            warn!("Exhausted {num_slow_downs} fragment exist retries");
            return Err(Status::resource_exhausted("Slow down"));
        }
    }

    Ok(())
}

/// First half of the push commit barrier: everything the head about to be published leads
/// to has reached the storage media, i.e. the revision, its trees and the payload uploaded
/// for it. A head never points at state a restarted server cannot load.
async fn flush_pushed_state(repository: &RepositoryContext) -> Result<(), BranchError> {
    repository
        .immutable_store()
        .flush(true)
        .await
        .forward::<BranchError>("Failed to make the pushed state durable")
}

/// Both halves of the push commit barrier, in order, for a head that is already in place.
async fn flush_published(repository: &RepositoryContext) -> Result<(), BranchError> {
    flush_pushed_state(repository).await?;
    flush_latest(repository).await
}

/// Second half of the push commit barrier: the head itself has reached the storage media.
/// Only then may the push be acknowledged.
async fn flush_latest(repository: &RepositoryContext) -> Result<(), BranchError> {
    repository
        .try_write_mutable_store()
        .ok_or_else(|| BranchError::from(WriteRequired))?
        .flush(true)
        .await
        .forward::<BranchError>("Failed to make the new latest pointer durable")
}

/// Publish `new_latest` as the head of `branch` if the head still is
/// `current_expected_latest`; returns the head found (equal to `current_expected_latest` when
/// the swap was made).
///
/// The swap sits inside the commit barrier: the pushed state is durable before the head can
/// point at it, and a head that was swapped is durable before this returns. An error from
/// either flush fails the push; after a failed second flush the head has moved in memory, and
/// the retry of the push goes through the same flushes before it reports success.
#[instrument(level = "debug", skip_all, fields(branch))]
pub async fn try_store_latest(
    repository: Arc<RepositoryContext>,
    branch: BranchId,
    current_expected_latest: Hash,
    new_latest: Hash,
) -> Result<Hash, BranchError> {
    flush_pushed_state(&repository).await?;

    let previous_latest = lore_revision::branch::mutable_try_store(
        repository.clone(),
        LATEST,
        branch,
        current_expected_latest,
        new_latest,
    )
    .await?;

    if previous_latest == current_expected_latest {
        flush_latest(&repository).await?;
    }

    Ok(previous_latest)
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::SocketAddr;

    use tonic::Request;
    use tonic::metadata::MetadataValue;
    use tonic::transport::server::TcpConnectInfo;

    use super::*;

    #[test]
    fn use_x_forwarded_when_available() {
        let mut req = Request::new(());

        let xff_metadata_value: MetadataValue<_> = "10.0.0.1, 10.0.0.2".parse().unwrap();
        req.metadata_mut()
            .insert("x-forwarded-for", xff_metadata_value);

        // set remote address to make sure it's NOT used in presence of the XFF header
        let peer_addr = SocketAddr::from(([192, 168, 1, 42], 4242));
        req.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(peer_addr),
        });

        assert_eq!(
            extract_client_ip(&req),
            Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)))
        );
    }

    #[test]
    fn dont_use_xff_when_it_contains_invalid_value() {
        let mut req = Request::new(());

        let xff_metadata_value: MetadataValue<_> = "10.0.0.lol, 10.0.0.wat".parse().unwrap();
        req.metadata_mut()
            .insert("x-forwarded-for", xff_metadata_value);

        let peer_addr = SocketAddr::from(([192, 168, 1, 42], 4242));
        req.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(peer_addr),
        });

        assert_eq!(
            extract_client_ip(&req),
            Some(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 42)))
        );
    }

    #[test]
    fn still_uses_last_ip_when_xff_contains_invalid_value_in_chain() {
        let mut req = Request::new(());

        let xff_metadata_value: MetadataValue<_> =
            "10.0.0.lol, 10.0.0.wat, 10.0.0.42".parse().unwrap();
        req.metadata_mut()
            .insert("x-forwarded-for", xff_metadata_value);

        let peer_addr = SocketAddr::from(([192, 168, 1, 42], 4242));
        req.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(peer_addr),
        });

        assert_eq!(
            extract_client_ip(&req),
            Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 42)))
        );
    }

    #[test]
    fn fallback_to_remote_addr() {
        let mut req = Request::new(());

        let peer_addr = SocketAddr::from(([192, 168, 1, 42], 31415));
        req.extensions_mut().insert(TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(peer_addr),
        });

        assert_eq!(
            extract_client_ip(&req),
            Some(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 42)))
        );
    }

    /// The push commit barrier: what is flushed, in which order, around the head swap.
    mod commit_barrier {
        use std::sync::Mutex;
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering;
        use std::time::Duration;

        use async_trait::async_trait;
        use bytes::Bytes;
        use lore_revision::branch::DEFAULT_HISTORY_STEP_SIZE;
        use lore_storage::Context;
        use lore_storage::Fragment;
        use lore_storage::ImmutableStore;
        use lore_storage::KeyType;
        use lore_storage::KeyValueStream;
        use lore_storage::MutableStore;
        use lore_storage::Partition;
        use lore_storage::StoreObliterateStats;
        use lore_storage::StoreQueryResult;
        use lore_storage::gc_event::GcEventSinkRef;
        use rand::random;

        use super::super::*;
        use crate::store::test_store_create;

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        enum StoreCall {
            ImmutableFlush,
            HeadSwap,
            MutableFlush,
        }

        /// The durable flushes and the swaps the stores were asked for, in order.
        #[derive(Default)]
        struct Recorder {
            calls: Mutex<Vec<StoreCall>>,
            fail_immutable_flush: AtomicBool,
            fail_mutable_flush: AtomicBool,
        }

        impl Recorder {
            fn record(&self, call: StoreCall) {
                self.calls.lock().unwrap().push(call);
            }

            fn take(&self) -> Vec<StoreCall> {
                std::mem::take(&mut *self.calls.lock().unwrap())
            }
        }

        struct RecordingImmutableStore {
            inner: Arc<dyn ImmutableStore>,
            recorder: Arc<Recorder>,
        }

        #[async_trait]
        impl ImmutableStore for RecordingImmutableStore {
            fn is_local(&self) -> bool {
                self.inner.is_local()
            }

            async fn is_available(self: Arc<Self>, timeout: Duration) -> bool {
                self.inner.clone().is_available(timeout).await
            }

            async fn exist(
                self: Arc<Self>,
                partition: Partition,
                address: Address,
                match_requested: StoreMatch,
            ) -> Result<StoreMatch, StoreError> {
                self.inner
                    .clone()
                    .exist(partition, address, match_requested)
                    .await
            }

            async fn exist_batch(
                self: Arc<Self>,
                partition: Partition,
                addresses: &[Address],
                match_requested: StoreMatch,
            ) -> Result<Vec<StoreMatch>, StoreError> {
                self.inner
                    .clone()
                    .exist_batch(partition, addresses, match_requested)
                    .await
            }

            async fn query(
                self: Arc<Self>,
                partition: Partition,
                address: Address,
                match_requested: StoreMatch,
            ) -> Result<StoreQueryResult, StoreError> {
                self.inner
                    .clone()
                    .query(partition, address, match_requested)
                    .await
            }

            async fn get(
                self: Arc<Self>,
                partition: Partition,
                address: Address,
                match_required: StoreMatch,
            ) -> Result<(Fragment, Bytes), StoreError> {
                self.inner
                    .clone()
                    .get(partition, address, match_required)
                    .await
            }

            async fn put(
                self: Arc<Self>,
                partition: Partition,
                address: Address,
                fragment: Fragment,
                payload: Option<Bytes>,
                force: bool,
            ) -> Result<(), StoreError> {
                self.inner
                    .clone()
                    .put(partition, address, fragment, payload, force)
                    .await
            }

            async fn obliterate(
                self: Arc<Self>,
                partition: Partition,
                address: Address,
                stats: Arc<StoreObliterateStats>,
            ) -> Result<(), StoreError> {
                self.inner
                    .clone()
                    .obliterate(partition, address, stats)
                    .await
            }

            async fn evict(
                self: Arc<Self>,
                max_capacity: usize,
                sync_data: bool,
                sink: Option<GcEventSinkRef>,
            ) -> Result<usize, StoreError> {
                self.inner
                    .clone()
                    .evict(max_capacity, sync_data, sink)
                    .await
            }

            async fn compact(
                self: Arc<Self>,
                max_size: usize,
                at: Option<usize>,
                sync_data: bool,
                sink: Option<GcEventSinkRef>,
            ) -> Result<Option<usize>, StoreError> {
                self.inner
                    .clone()
                    .compact(max_size, at, sync_data, sink)
                    .await
            }

            async fn compact_resume_at(self: Arc<Self>) -> Option<usize> {
                self.inner.clone().compact_resume_at().await
            }

            async fn compact_stop(self: Arc<Self>) {
                self.inner.clone().compact_stop().await;
            }

            fn max_query_batch(&self) -> Option<usize> {
                self.inner.max_query_batch()
            }

            async fn flush(self: Arc<Self>, sync_data: bool) -> Result<(), StoreError> {
                if sync_data {
                    self.recorder.record(StoreCall::ImmutableFlush);
                    if self.recorder.fail_immutable_flush.load(Ordering::SeqCst) {
                        return Err(StoreError::internal(
                            "immutable store cannot reach the disk",
                        ));
                    }
                }
                self.inner.clone().flush(sync_data).await
            }

            async fn fragment_count(self: Arc<Self>) -> Option<usize> {
                self.inner.clone().fragment_count().await
            }

            async fn verify(self: Arc<Self>, heal: bool) -> Result<(), StoreError> {
                self.inner.clone().verify(heal).await
            }

            async fn copy(
                self: Arc<Self>,
                source_partition: Partition,
                source_address: Address,
                destination_partition: Partition,
                destination_context: Context,
                durable: bool,
            ) -> Result<(), StoreError> {
                self.inner
                    .clone()
                    .copy(
                        source_partition,
                        source_address,
                        destination_partition,
                        destination_context,
                        durable,
                    )
                    .await
            }
        }

        struct RecordingMutableStore {
            inner: Arc<dyn MutableStore>,
            recorder: Arc<Recorder>,
        }

        #[async_trait]
        impl MutableStore for RecordingMutableStore {
            async fn load(
                self: Arc<Self>,
                partition: Partition,
                key: Hash,
                key_type: KeyType,
            ) -> Result<Hash, StoreError> {
                self.inner.clone().load(partition, key, key_type).await
            }

            async fn store(
                self: Arc<Self>,
                partition: Partition,
                key: Hash,
                value: Hash,
                key_type: KeyType,
            ) -> Result<(), StoreError> {
                self.inner
                    .clone()
                    .store(partition, key, value, key_type)
                    .await
            }

            async fn compare_and_swap(
                self: Arc<Self>,
                partition: Partition,
                key: Hash,
                expected: Hash,
                value: Hash,
                key_type: KeyType,
            ) -> Result<Hash, StoreError> {
                self.recorder.record(StoreCall::HeadSwap);
                self.inner
                    .clone()
                    .compare_and_swap(partition, key, expected, value, key_type)
                    .await
            }

            async fn list(
                self: Arc<Self>,
                partition: Partition,
                key_type: KeyType,
            ) -> Result<KeyValueStream, StoreError> {
                self.inner.clone().list(partition, key_type).await
            }

            async fn flush(self: Arc<Self>, sync_data: bool) -> Result<(), StoreError> {
                if sync_data {
                    self.recorder.record(StoreCall::MutableFlush);
                    if self.recorder.fail_mutable_flush.load(Ordering::SeqCst) {
                        return Err(StoreError::internal("mutable store cannot reach the disk"));
                    }
                }
                self.inner.clone().flush(sync_data).await
            }
        }

        /// A repository with a `main` branch and one revision that is stored and not pushed,
        /// on stores that record what the push asks of them.
        struct Fixture {
            repository: Arc<RepositoryContext>,
            recorder: Arc<Recorder>,
            main: BranchId,
            revision: Hash,
        }

        impl Fixture {
            async fn new() -> Self {
                let (immutable_store, mutable_store, _execution) =
                    test_store_create().await.expect("Failed to create stores");
                let recorder = Arc::new(Recorder::default());
                let repository = Arc::new(RepositoryContext::new_server_context(
                    Arc::new(RecordingImmutableStore {
                        inner: immutable_store,
                        recorder: recorder.clone(),
                    }),
                    Arc::new(RecordingMutableStore {
                        inner: mutable_store,
                        recorder: recorder.clone(),
                    }),
                    random::<RepositoryId>(),
                ));

                let write_token = get_write_token();
                let main = branch::create(
                    repository.clone(),
                    &write_token,
                    BranchId::from(uuid::Uuid::now_v7()),
                    "main",
                    branch::default_category(),
                    "test-creator",
                    1,
                    vec![],
                    false,
                    false,
                )
                .await
                .expect("Could not create root branch");

                let state = State::new();
                state.set_parent_self(Hash::default());
                state.set_revision_number(1);
                let revision = state
                    .serialize(repository.clone(), &write_token)
                    .await
                    .expect("Failed to serialize state");

                // Whatever the setup asked of the stores is not the push's doing
                recorder.take();

                Self {
                    repository,
                    recorder,
                    main,
                    revision,
                }
            }

            async fn push(&self) -> Result<PushResult, Status> {
                push(
                    self.repository.clone(),
                    self.main,
                    self.revision,
                    false,
                    false,
                    false,
                    DEFAULT_HISTORY_STEP_SIZE,
                    crate::grpc::server::RevisionListAcceleration::default(),
                )
                .await
            }

            async fn head(&self) -> Hash {
                load_latest(self.repository.clone(), self.main)
                    .await
                    .unwrap_or_default()
            }
        }

        async fn in_execution<F: Future>(test: F) -> F::Output {
            let execution = setup_execution("test", String::default(), String::default());
            Box::pin(LORE_CONTEXT.scope(execution, test)).await
        }

        #[tokio::test]
        async fn pushed_state_is_durable_before_the_head_and_the_head_before_the_ack() {
            in_execution(async {
                let fixture = Fixture::new().await;

                let pushed = fixture.push().await.expect("push should succeed");

                assert!(pushed.success);
                assert_eq!(fixture.head().await, fixture.revision);
                assert_eq!(
                    fixture.recorder.take(),
                    [
                        StoreCall::ImmutableFlush,
                        StoreCall::HeadSwap,
                        StoreCall::MutableFlush
                    ]
                );
            })
            .await;
        }

        #[tokio::test]
        async fn head_does_not_move_when_the_pushed_state_cannot_be_made_durable() {
            in_execution(async {
                let fixture = Fixture::new().await;
                fixture
                    .recorder
                    .fail_immutable_flush
                    .store(true, Ordering::SeqCst);

                let status = fixture.push().await.err().expect("push should fail");

                assert_eq!(status.code(), tonic::Code::Internal);
                assert_eq!(fixture.recorder.take(), [StoreCall::ImmutableFlush]);
                assert!(fixture.head().await.is_zero());
            })
            .await;
        }

        #[tokio::test]
        async fn push_is_not_acknowledged_until_the_head_is_durable() {
            in_execution(async {
                let fixture = Fixture::new().await;
                fixture
                    .recorder
                    .fail_mutable_flush
                    .store(true, Ordering::SeqCst);

                let status = fixture.push().await.err().expect("push should fail");

                assert_eq!(status.code(), tonic::Code::Internal);
                assert_eq!(
                    fixture.recorder.take(),
                    [
                        StoreCall::ImmutableFlush,
                        StoreCall::HeadSwap,
                        StoreCall::MutableFlush
                    ]
                );

                // The head moved in memory. The retry finds it in place and must not take
                // that for a completed push: it fails for as long as the head cannot be made
                // durable, and succeeds through the same two flushes once it can
                assert_eq!(fixture.head().await, fixture.revision);
                assert!(fixture.push().await.is_err());
                assert_eq!(
                    fixture.recorder.take(),
                    [StoreCall::ImmutableFlush, StoreCall::MutableFlush]
                );

                fixture
                    .recorder
                    .fail_mutable_flush
                    .store(false, Ordering::SeqCst);
                let pushed = fixture.push().await.expect("retried push should succeed");

                assert!(pushed.success);
                assert_eq!(
                    fixture.recorder.take(),
                    [StoreCall::ImmutableFlush, StoreCall::MutableFlush]
                );
            })
            .await;
        }

        #[tokio::test]
        async fn retried_push_does_not_flush_heads_over_a_failed_state_flush() {
            in_execution(async {
                let fixture = Fixture::new().await;
                fixture.push().await.expect("push should succeed");
                fixture.recorder.take();
                fixture
                    .recorder
                    .fail_immutable_flush
                    .store(true, Ordering::SeqCst);

                assert!(fixture.push().await.is_err());

                assert_eq!(fixture.recorder.take(), [StoreCall::ImmutableFlush]);
            })
            .await;
        }
    }
}
