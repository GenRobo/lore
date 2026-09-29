// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Validation of the ucs-auth broker's CheckUserPermission and LookupUserPermissions answers.
//! Only `read` and `write` count: an entry with no permission, or only unknown ones, grants
//! nothing, and a resource id that does not name a repository is ignored.

use std::collections::HashSet;
use std::str::FromStr;

use lore_proto::auth::ResourcePermission;
use lore_revision::lore::RepositoryId;
use tracing::warn;

use crate::auth::Access;

const WILDCARD: &str = "urc-*";

pub fn resource_id(repository: RepositoryId) -> String {
    format!("urc-{repository}")
}

fn permits(permission: &[String], access: Access) -> bool {
    permission.iter().any(|permission| match access {
        Access::Read => permission == "read" || permission == "write",
        Access::Write => permission == "write",
    })
}

/// Whether a CheckUserPermission answer grants `access` on `repository`. Only an entry for
/// exactly that repository's resource id counts.
pub fn check_grants(
    allowed: &[ResourcePermission],
    repository: RepositoryId,
    access: Access,
) -> bool {
    let wanted = resource_id(repository);
    allowed
        .iter()
        .any(|entry| entry.resource_id == wanted && permits(&entry.permission, access))
}

/// The repositories a LookupUserPermissions answer lets the caller list.
#[derive(Debug, PartialEq, Eq)]
pub enum ListScope {
    /// A readable `urc-*` grant: every repository on this origin.
    All,
    Only(Vec<RepositoryId>),
}

pub fn list_scope(grants: &[ResourcePermission]) -> ListScope {
    let mut seen = HashSet::new();
    let mut repositories = Vec::new();
    for grant in grants
        .iter()
        .filter(|grant| permits(&grant.permission, Access::Read))
    {
        if grant.resource_id == WILDCARD {
            return ListScope::All;
        }
        match parse_resource_id(&grant.resource_id) {
            Some(repository) => {
                if seen.insert(repository) {
                    repositories.push(repository);
                }
            }
            None => warn!(
                resource_id = %grant.resource_id,
                "Ignoring a broker grant that names no repository"
            ),
        }
    }
    ListScope::Only(repositories)
}

/// `urc-` followed by exactly 32 hex digits naming a non-zero repository.
fn parse_resource_id(resource_id: &str) -> Option<RepositoryId> {
    let repository = RepositoryId::from_str(resource_id.strip_prefix("urc-")?).ok()?;
    (!repository.is_zero()).then_some(repository)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant(resource_id: &str, permission: &[&str]) -> ResourcePermission {
        ResourcePermission {
            resource_id: resource_id.to_string(),
            permission: permission.iter().map(|p| (*p).to_string()).collect(),
        }
    }

    #[test]
    fn check_needs_read_or_write_on_exactly_the_repository() {
        let repository: RepositoryId = rand::random();
        let other: RepositoryId = rand::random();
        let id = resource_id(repository);

        let grants = |permission: &[&str], access| {
            check_grants(&[grant(&id, permission)], repository, access)
        };
        assert!(grants(&["read"], Access::Read));
        assert!(grants(&["write"], Access::Read));
        assert!(grants(&["read", "write"], Access::Write));
        assert!(!grants(&["read"], Access::Write));

        for ignored in [
            grant(&id, &[]),
            grant(&id, &["obliterate", "migrate", "owner", "admin"]),
            grant(&resource_id(other), &["read", "write"]),
            grant(WILDCARD, &["read", "write"]),
            grant(&id.to_uppercase(), &["read", "write"]),
        ] {
            assert!(
                !check_grants(&[ignored.clone()], repository, Access::Read),
                "{ignored:?} must not grant visibility"
            );
        }
    }

    #[test]
    fn check_matches_any_entry_not_just_the_first() {
        let repository: RepositoryId = rand::random();
        let answer = [
            grant(&resource_id(rand::random()), &["read"]),
            grant(&resource_id(repository), &["read"]),
        ];
        assert!(check_grants(&answer, repository, Access::Read));
    }

    #[test]
    fn a_readable_wildcard_lists_everything() {
        let answer = [
            grant(&resource_id(rand::random()), &["read"]),
            grant(WILDCARD, &["read"]),
        ];
        assert_eq!(list_scope(&answer), ListScope::All);
    }

    #[test]
    fn listing_keeps_only_readable_well_formed_repositories() {
        let readable: RepositoryId = rand::random();
        let writable: RepositoryId = rand::random();
        let answer = [
            grant(WILDCARD, &[]),
            grant(WILDCARD, &["obliterate"]),
            grant(&resource_id(rand::random()), &[]),
            grant(&resource_id(rand::random()), &["migrate"]),
            grant("urc-not-a-repository", &["read"]),
            grant("urc-00000000000000000000000000000000", &["read"]),
            grant(&format!("urc-{readable}00"), &["read"]),
            grant(&resource_id(readable), &["read"]),
            grant(&resource_id(readable), &["read", "write"]),
            grant(&resource_id(writable), &["write"]),
        ];
        assert_eq!(
            list_scope(&answer),
            ListScope::Only(vec![readable, writable])
        );
    }

    #[test]
    fn an_empty_answer_lists_nothing() {
        assert_eq!(list_scope(&[]), ListScope::Only(vec![]));
    }
}
