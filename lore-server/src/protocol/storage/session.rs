// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use dashmap::DashMap;
use lore_revision::lore::RepositoryId;

pub(crate) const MAX_CONCURRENT_SESSIONS: u32 = 10_000;

pub struct SessionEntry {
    pub repository: RepositoryId,
    pub correlation_id: String,
    pub user_id: String,
    /// Whether the session's token grants write on the repository (mutating commands are
    /// refused otherwise). Auth-off servers always grant it.
    pub write_allowed: bool,
    /// The session token's `exp` (Unix seconds); commands are refused once it passes, so a
    /// long-lived connection cannot outlive the token that authorized it. 0 means no expiry
    /// (auth-off sessions).
    pub expires_at: u64,
}

impl SessionEntry {
    /// Whether the token that started this session is still valid at `now` (Unix seconds).
    pub fn is_live(&self, now: u64) -> bool {
        is_live(self.expires_at, now)
    }
}

fn is_live(expires_at: u64, now: u64) -> bool {
    expires_at == 0 || now < expires_at
}

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or(0)
}

/// A started session's authorization to read its repository, which a Copy on another session
/// may use as its source. It lasts only while that session does.
struct SourceGrant {
    session_id: u32,
    user_id: String,
    expires_at: u64,
}

/// Per-connection session state for the `lore-storage/0.4` protocol.
///
/// Tracks active sessions mapping session IDs to repository, correlation ID, and user ID tuples,
/// and each live session's source grant for Copy checks. Each `start()` always allocates a new
/// session ID — deduplication is handled client-side by `StorageConnector`.
pub struct SessionMap {
    entries: DashMap<u32, SessionEntry>,
    authorized_repos: DashMap<RepositoryId, Vec<SourceGrant>>,
    counter: AtomicU32,
}

#[derive(Debug, PartialEq)]
pub enum SessionError {
    LimitReached,
    CounterExhausted,
    NotFound,
}

impl Default for SessionMap {
    fn default() -> Self {
        Self {
            entries: DashMap::new(),
            authorized_repos: DashMap::new(),
            counter: AtomicU32::new(1),
        }
    }
}

impl SessionMap {
    /// Start a new session. Always allocates a fresh session ID — deduplication
    /// is the client's responsibility (`StorageConnector`).
    pub fn start(
        &self,
        repository: RepositoryId,
        correlation_id: String,
        user_id: String,
        write_allowed: bool,
        expires_at: u64,
    ) -> Result<(u32, String), SessionError> {
        if self.entries.len() >= MAX_CONCURRENT_SESSIONS as usize {
            return Err(SessionError::LimitReached);
        }

        let session_id = self.counter.fetch_add(1, Ordering::Relaxed);
        if session_id == 0 {
            return Err(SessionError::CounterExhausted);
        }

        let correlation_id = if correlation_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            correlation_id
        };

        // The entry holds its shard lock only for this push: nothing is awaited under it and no
        // other lock is taken.
        #[allow(clippy::disallowed_methods)]
        let mut grants = self.authorized_repos.entry(repository).or_default();
        grants.push(SourceGrant {
            session_id,
            user_id: user_id.clone(),
            expires_at,
        });
        drop(grants);

        self.entries.insert(
            session_id,
            SessionEntry {
                repository,
                correlation_id: correlation_id.clone(),
                user_id,
                write_allowed,
                expires_at,
            },
        );

        Ok((session_id, correlation_id))
    }

    /// Stop an active session, ending its source grant.
    pub fn stop(&self, session_id: u32) -> Result<(), SessionError> {
        let Some((_, entry)) = self.entries.remove(&session_id) else {
            return Err(SessionError::NotFound);
        };
        if let Some(mut grants) = self.authorized_repos.get_mut(&entry.repository) {
            grants.retain(|grant| grant.session_id != session_id);
        }
        self.authorized_repos
            .remove_if(&entry.repository, |_, grants| grants.is_empty());
        Ok(())
    }

    pub fn get(&self, session_id: u32) -> Option<dashmap::mapref::one::Ref<'_, u32, SessionEntry>> {
        self.entries.get(&session_id)
    }

    /// Whether `user_id` may read `repository` as a Copy source at `now`: a session for it that
    /// this caller started on this connection is still running and its token has not expired.
    pub fn is_repository_authorized(
        &self,
        repository: RepositoryId,
        user_id: &str,
        now: u64,
    ) -> bool {
        self.source_authorization(repository, user_id, now) == SourceAuthorization::Live
    }

    /// How `user_id`'s sessions on this connection authorize `repository` as a Copy source at
    /// `now`.
    pub fn source_authorization(
        &self,
        repository: RepositoryId,
        user_id: &str,
        now: u64,
    ) -> SourceAuthorization {
        let Some(grants) = self.authorized_repos.get(&repository) else {
            return SourceAuthorization::Absent;
        };
        let mut own = grants
            .iter()
            .filter(|grant| grant.user_id == user_id)
            .peekable();
        if own.peek().is_none() {
            return SourceAuthorization::Absent;
        }
        if own.any(|grant| is_live(grant.expires_at, now)) {
            SourceAuthorization::Live
        } else {
            SourceAuthorization::Expired
        }
    }
}

/// A caller's authorization to read a repository as a Copy source on one connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SourceAuthorization {
    /// A running session with an unexpired token grants it.
    Live,
    /// Only sessions whose tokens have expired grant it; renewing them restores it.
    Expired,
    /// No session of the caller's grants it.
    Absent,
}

#[cfg(test)]
mod tests {
    use rand::random;

    use super::*;

    #[test]
    fn start_assigns_session_id_from_one() {
        let map = SessionMap::default();
        let (id, _) = map.start(random(), "corr-1".into(), String::new(), true, 0).unwrap();
        assert_eq!(id, 1);
    }

    #[test]
    fn start_increments_session_id() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let (id1, _) = map.start(repo, "corr-1".into(), String::new(), true, 0).unwrap();
        let (id2, _) = map.start(repo, "corr-2".into(), String::new(), true, 0).unwrap();
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
    }

    #[test]
    fn start_always_allocates_new_id() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let (id1, _) = map.start(repo, "corr-1".into(), String::new(), true, 0).unwrap();
        let (id2, _) = map.start(repo, "corr-1".into(), String::new(), true, 0).unwrap();
        assert_ne!(id1, id2);
    }

    #[test]
    fn start_empty_correlation_generates_uuid() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let (id1, corr1) = map.start(repo, String::new(), String::new(), true, 0).unwrap();
        let (id2, corr2) = map.start(repo, String::new(), String::new(), true, 0).unwrap();
        assert_ne!(id1, id2);
        assert!(!corr1.is_empty());
        assert!(!corr2.is_empty());
        assert_ne!(corr1, corr2);
    }

    #[test]
    fn stop_removes_session() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let (id, _) = map.start(repo, "corr-1".into(), String::new(), true, 0).unwrap();
        assert!(map.get(id).is_some());
        map.stop(id).unwrap();
        assert!(map.get(id).is_none());
    }

    #[test]
    fn stop_unknown_returns_not_found() {
        let map = SessionMap::default();
        assert_eq!(map.stop(999), Err(SessionError::NotFound));
    }

    #[test]
    fn stop_already_stopped_returns_not_found() {
        let map = SessionMap::default();
        let (id, _) = map.start(random(), "corr-1".into(), String::new(), true, 0).unwrap();
        map.stop(id).unwrap();
        assert_eq!(map.stop(id), Err(SessionError::NotFound));
    }

    #[test]
    fn start_after_stop_allocates_new_id() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let (id1, _) = map.start(repo, "corr-1".into(), String::new(), true, 0).unwrap();
        map.stop(id1).unwrap();
        let (id2, _) = map.start(repo, "corr-1".into(), String::new(), true, 0).unwrap();
        assert_ne!(id1, id2);
    }

    #[test]
    fn get_returns_entry_with_user_id() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let (id, _) = map.start(repo, "corr-1".into(), "user-42".into(), true, 0).unwrap();
        let entry = map.get(id).unwrap();
        assert_eq!(entry.repository, repo);
        assert_eq!(entry.correlation_id, "corr-1");
        assert_eq!(entry.user_id, "user-42");
    }

    #[test]
    fn get_returns_none_for_unknown() {
        let map = SessionMap::default();
        assert!(map.get(42).is_none());
    }

    #[test]
    fn is_repository_authorized() {
        let map = SessionMap::default();
        let repo_a = random::<RepositoryId>();
        let repo_b = random::<RepositoryId>();
        map.start(repo_a, "corr-1".into(), String::new(), true, 0).unwrap();

        assert!(map.is_repository_authorized(repo_a, "", NOW));
        assert!(!map.is_repository_authorized(repo_b, "", NOW));
    }

    const NOW: u64 = 1_000_000;

    /// Start a session for `user` whose token expires a minute after `NOW`.
    fn start_for(map: &SessionMap, repo: RepositoryId, user: &str) -> u32 {
        let (id, _) = map
            .start(repo, String::new(), user.to_string(), true, NOW + 60)
            .unwrap();
        id
    }

    #[test]
    fn stop_removes_the_source_grant() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let id = start_for(&map, repo, "alice");
        assert!(map.is_repository_authorized(repo, "alice", NOW));
        map.stop(id).unwrap();
        assert!(!map.is_repository_authorized(repo, "alice", NOW));
    }

    #[test]
    fn stopping_one_session_keeps_another_for_the_same_repository() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let first = start_for(&map, repo, "alice");
        start_for(&map, repo, "alice");
        map.stop(first).unwrap();
        assert!(map.is_repository_authorized(repo, "alice", NOW));
    }

    #[test]
    fn a_source_grant_ends_at_its_token_expiry() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        start_for(&map, repo, "alice");
        assert!(map.is_repository_authorized(repo, "alice", NOW + 59));
        assert!(!map.is_repository_authorized(repo, "alice", NOW + 60));
    }

    #[test]
    fn a_source_grant_belongs_to_the_caller_that_started_it() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        start_for(&map, repo, "alice");
        assert!(!map.is_repository_authorized(repo, "mallory", NOW));
    }

    #[test]
    fn auth_off_sessions_do_not_expire() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let (id, _) = map
            .start(repo, String::new(), String::new(), true, 0)
            .unwrap();
        assert!(map.get(id).unwrap().is_live(u64::MAX));
        assert!(map.is_repository_authorized(repo, "", u64::MAX));
    }

    #[test]
    fn concurrent_session_limit() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        for i in 0..MAX_CONCURRENT_SESSIONS {
            map.start(repo, format!("corr-{i}"), String::new(), true, 0).unwrap();
        }
        assert_eq!(
            map.start(repo, "one-more".into(), String::new(), true, 0),
            Err(SessionError::LimitReached)
        );
    }

    #[test]
    fn limit_freed_by_stop() {
        let map = SessionMap::default();
        let repo = random::<RepositoryId>();
        let mut ids = Vec::new();
        for i in 0..MAX_CONCURRENT_SESSIONS {
            let (id, _) = map.start(repo, format!("corr-{i}"), String::new(), true, 0).unwrap();
            ids.push(id);
        }
        assert_eq!(
            map.start(repo, "blocked".into(), String::new(), true, 0),
            Err(SessionError::LimitReached)
        );
        map.stop(ids[0]).unwrap();
        map.start(repo, "freed".into(), String::new(), true, 0).unwrap();
    }
}
