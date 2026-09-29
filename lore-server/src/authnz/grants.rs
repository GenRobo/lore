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
use crate::auth::jwt::AuthorizationToken;

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
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ListScope {
    /// The answer holds a readable `urc-*` grant: every repository on this origin.
    pub every_repository: bool,
    /// The concrete readable repositories the answer names.
    pub repositories: Vec<RepositoryId>,
}

pub fn list_scope(grants: &[ResourcePermission]) -> ListScope {
    let mut scope = ListScope::default();
    let mut seen = HashSet::new();
    for grant in grants
        .iter()
        .filter(|grant| permits(&grant.permission, Access::Read))
    {
        if grant.resource_id == WILDCARD {
            scope.every_repository = true;
            continue;
        }
        match parse_resource_id(&grant.resource_id) {
            Some(repository) => {
                if seen.insert(repository) {
                    scope.repositories.push(repository);
                }
            }
            None => warn!(
                resource_id = %grant.resource_id,
                "Ignoring a broker grant that names no repository"
            ),
        }
    }
    scope
}

/// Whether the caller's own verified token backs a broker's `urc-*` answer. A token that lists
/// its grants must hold a readable `urc-*` itself, so a broker answering `urc-*` for everyone
/// cannot widen a scoped caller's listing. A token without a `resources` claim leaves the
/// decision to the broker.
pub fn token_backs_wildcard(token: Option<&AuthorizationToken>) -> bool {
    let Some(token) = token else {
        return false;
    };
    match token.resources.as_ref() {
        Some(resources) => resources
            .iter()
            .any(|grant| grant.is_wildcard_resource() && grant.allows_read()),
        None => true,
    }
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
        let concrete: RepositoryId = rand::random();
        let answer = [
            grant(&resource_id(concrete), &["read"]),
            grant(WILDCARD, &["read"]),
        ];
        assert_eq!(
            list_scope(&answer),
            ListScope {
                every_repository: true,
                repositories: vec![concrete],
            }
        );
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
            ListScope {
                every_repository: false,
                repositories: vec![readable, writable],
            }
        );
    }

    #[test]
    fn an_empty_answer_lists_nothing() {
        assert_eq!(list_scope(&[]), ListScope::default());
    }

    fn token(resources: Option<Vec<(&str, &[&str])>>) -> AuthorizationToken {
        AuthorizationToken {
            resources: resources.map(|resources| {
                resources
                    .into_iter()
                    .map(
                        |(resource_id, permission)| crate::auth::jwt::ResourcePermission {
                            resource_id: resource_id.to_string(),
                            permission: permission.iter().map(|p| (*p).to_string()).collect(),
                        },
                    )
                    .collect()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn only_a_token_holding_a_readable_wildcard_backs_one() {
        let scoped = resource_id(rand::random());
        assert!(token_backs_wildcard(Some(&token(Some(vec![(
            WILDCARD,
            &["read"]
        )])))));
        assert!(
            token_backs_wildcard(Some(&token(None))),
            "no resources claim: broker decides"
        );
        assert!(!token_backs_wildcard(None), "no token at all");
        for resources in [
            vec![(scoped.as_str(), &["read", "write"][..])],
            vec![(WILDCARD, &[][..])],
            vec![],
        ] {
            assert!(
                !token_backs_wildcard(Some(&token(Some(resources.clone())))),
                "{resources:?}"
            );
        }
    }
}
