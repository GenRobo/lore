// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Managed repositories: repositories whose content only a listed publisher may change.
//!
//! A JSON policy file (`[server.auth] managed_repositories_file`) lists them:
//!
//! ```json
//! {"version": 1,
//!  "managed": [{"resource_id": "urc-<32 lowercase hex>", "name": "pack",
//!               "publishers": ["<sub>"], "authors": ["<sub>"]}]}
//! ```
//!
//! On a managed repository, write is granted only by an explicit `urc-<id>` grant carrying
//! `publish`, held by a subject listed in `publishers`; `write`, `owner` and `admin`, wildcard
//! or explicit, read only. Elevated permissions (`owner`, `admin`, `obliterate`, `migrate`)
//! count only from such a publisher grant. `authors` is validated here and used elsewhere.
//!
//! No file configured: nothing is managed. A configured file that is missing or invalid
//! fails closed: at startup the server refuses to start, after startup every repository is
//! treated as managed with no publishers until the file is valid again. The file is re-read
//! when its modification time or size changes, checked at most every [`RECHECK`].

use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use std::time::Instant;

use serde::Deserialize;
use thiserror::Error;
use tracing::error;
use tracing::info;

use super::jwt::ResourcePermission;

/// The only policy format this server reads.
const VERSION: u32 = 1;

/// The only permission that grants write on a managed repository.
pub const PUBLISH: &str = "publish";

/// How long a loaded policy is trusted before the file is checked for a change.
pub const RECHECK: Duration = Duration::from_secs(5);

#[derive(Debug, Error)]
pub enum PolicyError {
    #[error("cannot read managed repositories policy {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid managed repositories policy: {0}")]
    Invalid(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyFile {
    version: u32,
    managed: Vec<PolicyEntry>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyEntry {
    resource_id: String,
    #[serde(default)]
    #[allow(dead_code)]
    name: Option<String>,
    publishers: Vec<String>,
    authors: Vec<String>,
}

/// A parsed, validated policy: managed resource id to its publishers.
#[derive(Debug, Default, PartialEq)]
pub struct ManagedPolicy {
    publishers: HashMap<String, HashSet<String>>,
}

impl ManagedPolicy {
    pub fn parse(bytes: &[u8]) -> Result<Self, PolicyError> {
        let file: PolicyFile =
            serde_json::from_slice(bytes).map_err(|err| PolicyError::Invalid(err.to_string()))?;
        if file.version != VERSION {
            return Err(PolicyError::Invalid(format!(
                "version {} is not {VERSION}",
                file.version
            )));
        }
        let mut publishers = HashMap::new();
        for entry in file.managed {
            if !is_canonical_resource_id(&entry.resource_id) {
                return Err(PolicyError::Invalid(format!(
                    "resource_id {:?} is not urc- followed by 32 lowercase hex digits",
                    entry.resource_id
                )));
            }
            let listed = subjects(&entry.resource_id, "publishers", entry.publishers)?;
            subjects(&entry.resource_id, "authors", entry.authors)?;
            if publishers
                .insert(entry.resource_id.clone(), listed)
                .is_some()
            {
                return Err(PolicyError::Invalid(format!(
                    "resource_id {} is listed twice",
                    entry.resource_id
                )));
            }
        }
        Ok(Self { publishers })
    }

    pub fn len(&self) -> usize {
        self.publishers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.publishers.is_empty()
    }
}

/// Subjects are non-empty visible-ASCII strings (0x21-0x7e), compared exactly (case-sensitive);
/// anything else, or a duplicate, makes the policy invalid. GRID's broker and GDAM apply the
/// same rule, so all three agree on which policies are valid.
fn subjects(
    resource_id: &str,
    field: &str,
    listed: Vec<String>,
) -> Result<HashSet<String>, PolicyError> {
    let mut set = HashSet::with_capacity(listed.len());
    for subject in listed {
        if subject.is_empty() || !subject.bytes().all(|byte| (0x21..=0x7e).contains(&byte)) {
            return Err(PolicyError::Invalid(format!(
                "{resource_id}: {field} holds an empty or malformed subject"
            )));
        }
        if !set.insert(subject) {
            return Err(PolicyError::Invalid(format!(
                "{resource_id}: {field} lists a subject twice"
            )));
        }
    }
    Ok(set)
}

fn is_canonical_resource_id(resource_id: &str) -> bool {
    resource_id.strip_prefix("urc-").is_some_and(|hex| {
        hex.len() == 32
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            && hex.bytes().any(|byte| byte != b'0')
    })
}

/// The policy as an authorization check sees it.
#[derive(Clone, Debug, Default)]
pub enum Snapshot {
    /// No policy configured: nothing is managed.
    #[default]
    Unconfigured,
    Valid(Arc<ManagedPolicy>),
    /// A configured policy that cannot be read or parsed: everything is managed and nobody
    /// may publish.
    Invalid,
}

/// How `repository` (a `urc-<id>` resource id) is governed.
pub enum Governance<'a> {
    Unmanaged,
    Managed {
        publishers: Option<&'a HashSet<String>>,
    },
}

impl Snapshot {
    pub fn governance(&self, repository: &str) -> Governance<'_> {
        match self {
            Snapshot::Unconfigured => Governance::Unmanaged,
            Snapshot::Invalid => Governance::Managed { publishers: None },
            Snapshot::Valid(policy) => match policy.publishers.get(repository) {
                None => Governance::Unmanaged,
                Some(publishers) => Governance::Managed {
                    publishers: Some(publishers),
                },
            },
        }
    }

    /// The grants of `subject` that count on `repository`. On an unmanaged repository, every
    /// grant naming it or `urc-*`. On a managed one, only an explicit grant carrying `publish`
    /// held by a listed publisher; every other grant is reduced to read.
    pub fn effective_grants<'g>(
        &self,
        subject: &str,
        repository: &str,
        grants: &'g [ResourcePermission],
    ) -> EffectiveGrants<'g> {
        let matching = grants
            .iter()
            .filter(|grant| grant.matches_repository(repository));
        match self.governance(repository) {
            Governance::Unmanaged => EffectiveGrants {
                full: matching.collect(),
                read_only: Vec::new(),
                managed: false,
            },
            Governance::Managed { publishers } => {
                let publisher = publishers.is_some_and(|publishers| publishers.contains(subject));
                let (full, read_only) = matching.partition(|grant| {
                    publisher
                        && !grant.is_wildcard_resource()
                        && grant
                            .permission
                            .iter()
                            .any(|permission| permission == PUBLISH)
                });
                EffectiveGrants {
                    full,
                    read_only,
                    managed: true,
                }
            }
        }
    }
}

/// The grants that apply to one repository, split by how far they count.
pub struct EffectiveGrants<'g> {
    /// Grants whose every permission counts.
    pub full: Vec<&'g ResourcePermission>,
    /// Grants that count only for read on a managed repository.
    pub read_only: Vec<&'g ResourcePermission>,
    /// On a managed repository every grant in `full` is a publisher's `publish` grant, which is
    /// what writes there; elsewhere `write`, `owner` or `admin` writes.
    managed: bool,
}

impl EffectiveGrants<'_> {
    pub fn allows_read(&self) -> bool {
        self.full
            .iter()
            .chain(self.read_only.iter())
            .any(|grant| grant.allows_read())
    }

    pub fn allows_write(&self) -> bool {
        if self.managed {
            !self.full.is_empty()
        } else {
            self.full.iter().any(|grant| grant.allows_write())
        }
    }

    pub fn has_permission(&self, wanted: &str) -> bool {
        self.full.iter().any(|grant| {
            grant
                .permission
                .iter()
                .any(|permission| permission == wanted)
        })
    }
}

struct Holder {
    path: Option<PathBuf>,
    snapshot: Snapshot,
    /// The bytes the valid snapshot was parsed from. The file's contents decide whether it
    /// changed: a replacement that keeps its size and modification time must still take effect.
    bytes: Option<Vec<u8>>,
    checked: Instant,
}

static POLICY: RwLock<Option<Holder>> = RwLock::new(None);

fn load(path: &Path) -> Result<(ManagedPolicy, Vec<u8>), PolicyError> {
    let bytes = std::fs::read(path).map_err(|source| PolicyError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    Ok((ManagedPolicy::parse(&bytes)?, bytes))
}

/// Load the configured policy at startup. An error must stop the server.
pub fn configure(path: Option<&Path>) -> Result<(), PolicyError> {
    let holder = match path {
        None => Holder {
            path: None,
            snapshot: Snapshot::Unconfigured,
            bytes: None,
            checked: Instant::now(),
        },
        Some(path) => {
            let (policy, bytes) = load(path)?;
            info!(
                path = %path.display(),
                managed = policy.len(),
                "Loaded managed repositories policy"
            );
            Holder {
                path: Some(path.to_path_buf()),
                snapshot: Snapshot::Valid(Arc::new(policy)),
                bytes: Some(bytes),
                checked: Instant::now(),
            }
        }
    };
    *POLICY
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(holder);
    Ok(())
}

/// Whether a policy file is configured. Fixed at startup: an unconfigured server never starts
/// managing a repository, so a decision taken without a policy stays valid.
pub fn is_configured() -> bool {
    POLICY
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
        .is_some_and(|holder| holder.path.is_some())
}

/// Make the next [`current`] look at the file now, as it would after [`RECHECK`].
#[cfg(test)]
pub(crate) fn recheck_now() {
    if let Some(holder) = POLICY
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_mut()
    {
        holder.checked = Instant::now() - RECHECK;
    }
}

/// The policy in force, re-read first when the file may have changed.
pub fn current() -> Snapshot {
    {
        let guard = POLICY
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match guard.as_ref() {
            None => return Snapshot::Unconfigured,
            Some(holder) if holder.path.is_none() || holder.checked.elapsed() < RECHECK => {
                return holder.snapshot.clone();
            }
            Some(_) => {}
        }
    }
    let mut guard = POLICY
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(holder) = guard.as_mut() else {
        return Snapshot::Unconfigured;
    };
    let Some(path) = holder.path.clone() else {
        return holder.snapshot.clone();
    };
    if holder.checked.elapsed() >= RECHECK {
        holder.checked = Instant::now();
        // Read the file every time: it is small, and its metadata does not prove it unchanged.
        let read = std::fs::read(&path).map_err(|source| PolicyError::Read {
            path: path.clone(),
            source,
        });
        let unchanged = matches!(holder.snapshot, Snapshot::Valid(_))
            && read.as_ref().ok() == holder.bytes.as_ref();
        if !unchanged {
            match read.and_then(|bytes| Ok((ManagedPolicy::parse(&bytes)?, bytes))) {
                Ok((policy, bytes)) => {
                    info!(
                        path = %path.display(),
                        managed = policy.len(),
                        "Reloaded managed repositories policy"
                    );
                    holder.snapshot = Snapshot::Valid(Arc::new(policy));
                    holder.bytes = Some(bytes);
                }
                Err(err) => {
                    if !matches!(holder.snapshot, Snapshot::Invalid) {
                        error!(
                            error = %err,
                            "Managed repositories policy is unusable: every repository is \
                             treated as managed with no publishers until it is fixed"
                        );
                    }
                    holder.snapshot = Snapshot::Invalid;
                    holder.bytes = None;
                }
            }
        }
    }
    holder.snapshot.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    const PACK: &str = "urc-0123456789abcdef0123456789abcdef";
    const OTHER: &str = "urc-fedcba9876543210fedcba9876543210";

    fn grant(resource_id: &str, permission: &[&str]) -> ResourcePermission {
        ResourcePermission {
            resource_id: resource_id.to_string(),
            permission: permission.iter().map(|p| (*p).to_string()).collect(),
        }
    }

    fn policy(json: &str) -> Result<ManagedPolicy, PolicyError> {
        ManagedPolicy::parse(json.as_bytes())
    }

    fn pack_policy() -> Snapshot {
        Snapshot::Valid(Arc::new(
            policy(&format!(
                r#"{{"version":1,"managed":[{{"resource_id":"{PACK}","name":"pack",
                    "publishers":["gdam-publisher"],"authors":["alice"]}}]}}"#
            ))
            .expect("valid policy"),
        ))
    }

    #[test]
    fn parse_accepts_empty_and_rejects_malformed_policies() {
        assert!(
            policy(r#"{"version":1,"managed":[]}"#)
                .expect("empty")
                .is_empty()
        );
        let entry = |id: &str, publishers: &str| {
            format!(
                r#"{{"version":1,"managed":[{{"resource_id":"{id}","publishers":{publishers},"authors":[]}}]}}"#
            )
        };
        assert!(policy(&entry(PACK, r#"["svc"]"#)).is_ok());
        for bad in [
            String::new(),
            "{}".to_string(),
            r#"{"version":2,"managed":[]}"#.to_string(),
            r#"{"version":1,"managed":[],"extra":1}"#.to_string(),
            entry(&PACK.to_uppercase().replace("URC-", "urc-"), r#"["svc"]"#),
            entry("urc-*", r#"["svc"]"#),
            entry("urc-0123", r#"["svc"]"#),
            entry("urc-00000000000000000000000000000000", r#"["svc"]"#),
            entry(PACK, r#"[""]"#),
            entry(PACK, r#"[" svc"]"#),
            entry(PACK, r#"["svc","svc"]"#),
            format!(
                r#"{{"version":1,"managed":[{{"resource_id":"{PACK}","publishers":[],"authors":[]}},
                    {{"resource_id":"{PACK}","publishers":[],"authors":[]}}]}}"#
            ),
            format!(
                r#"{{"version":1,"managed":[{{"resource_id":"{PACK}","publishers":[],"authors":[""]}}]}}"#
            ),
        ] {
            assert!(policy(&bad).is_err(), "{bad} must be rejected");
        }
    }

    #[test]
    fn only_a_listed_publishers_explicit_publish_grant_counts_on_a_managed_repository() {
        let snapshot = pack_policy();
        let check = |subject: &str, grants: &[ResourcePermission]| {
            let effective = snapshot.effective_grants(subject, PACK, grants);
            (
                effective.allows_read(),
                effective.allows_write(),
                effective.has_permission("admin"),
            )
        };
        assert_eq!(
            check("gdam-publisher", &[grant(PACK, &["publish", "admin"])]),
            (true, true, true)
        );
        // `publish` alone is what writes a managed repository.
        assert_eq!(
            check("gdam-publisher", &[grant(PACK, &["read", "publish"])]),
            (true, true, false)
        );
        // Case-sensitive subjects, no wildcard publish, no unlisted publisher.
        assert_eq!(
            check("GDAM-publisher", &[grant(PACK, &["publish"])]),
            (true, false, false)
        );
        assert_eq!(
            check("gdam-publisher", &[grant("urc-*", &["publish"])]),
            (true, false, false)
        );
        assert_eq!(
            check("alice", &[grant(PACK, &["publish"])]),
            (true, false, false)
        );
        // Ordinary and elevated grants read only, wildcard or explicit, even for a publisher.
        for permissions in [
            &["read", "write"][..],
            &["owner", "admin", "obliterate", "migrate"],
        ] {
            for resource in [PACK, "urc-*"] {
                for subject in ["gdam-publisher", "alice"] {
                    let (read, write, admin) = check(subject, &[grant(resource, permissions)]);
                    assert!(!write && !admin, "{subject} {resource} {permissions:?}");
                    assert!(read, "{subject} {resource} {permissions:?}");
                }
            }
        }
        // Unmanaged repositories keep today's behaviour.
        let wildcard_writer = [grant("urc-*", &["write"])];
        let effective = snapshot.effective_grants("alice", OTHER, &wildcard_writer);
        assert!(effective.allows_write());
    }

    #[test]
    fn an_invalid_policy_manages_everything_and_lets_nobody_publish() {
        let grants = [grant(OTHER, &["publish", "write", "admin"])];
        let effective = Snapshot::Invalid.effective_grants("gdam-publisher", OTHER, &grants);
        assert!(effective.allows_read());
        assert!(!effective.allows_write());
        assert!(!effective.has_permission("admin"));
    }
}
