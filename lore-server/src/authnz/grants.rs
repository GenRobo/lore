// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Validation of the ucs-auth broker's CheckUserPermission and LookupUserPermissions answers,
//! capped by the caller's own verified token. Only `read` and `write` count in a broker answer:
//! an entry with no permission, or only unknown ones, grants nothing, and a resource id that
//! does not name a repository is ignored. A token without a `resources` claim grants nothing.

use std::collections::HashSet;
use std::str::FromStr;

use lore_proto::auth::ResourcePermission;
use lore_revision::lore::RepositoryId;
use tracing::warn;

use crate::auth::Access;
use crate::auth::jwt::AuthorizationToken;
use crate::auth::jwt::verify_authorization;
use crate::auth::jwt::verify_write_authorization;

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

/// The repositories a set of grants lets the caller list.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct ListScope {
    /// The grants hold a readable `urc-*`: every repository on this origin.
    pub every_repository: bool,
    /// The concrete readable repositories the grants name.
    pub repositories: Vec<RepositoryId>,
}

impl ListScope {
    pub fn covers(&self, repository: RepositoryId) -> bool {
        self.every_repository || self.repositories.contains(&repository)
    }

    pub fn is_empty(&self) -> bool {
        !self.every_repository && self.repositories.is_empty()
    }
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

/// Whether the caller's own verified token grants `access` on `repository`, exactly or through
/// `urc-*`. The broker is asked only after this holds, so it can narrow the token's scope but
/// never widen it.
pub fn token_grants(
    token: Option<&AuthorizationToken>,
    repository: RepositoryId,
    access: Access,
) -> bool {
    let Some(token) = token else {
        return false;
    };
    match access {
        Access::Read => verify_authorization(token, repository).is_ok(),
        Access::Write => verify_write_authorization(token, repository).is_ok(),
    }
}

/// The readable repositories of the caller's own verified token: the most a listing may show.
pub fn token_scope(token: Option<&AuthorizationToken>) -> ListScope {
    let mut scope = ListScope::default();
    let Some(resources) = token.and_then(|token| token.resources.as_ref()) else {
        return scope;
    };
    let mut seen = HashSet::new();
    for grant in resources.iter().filter(|grant| grant.allows_read()) {
        if grant.is_wildcard_resource() {
            scope.every_repository = true;
        } else if let Some(repository) = parse_resource_id(&grant.resource_id)
            && seen.insert(repository)
        {
            scope.repositories.push(repository);
        }
    }
    scope
}

/// Whether the caller may create repositories. Names are global, so a create reveals whether a
/// name is taken anywhere on the origin: only a token holding a writable `urc-*`, whose holder
/// may see every repository anyway, may ask.
pub fn may_create(token: Option<&AuthorizationToken>) -> bool {
    token
        .and_then(|token| token.resources.as_ref())
        .is_some_and(|resources| {
            resources
                .iter()
                .any(|grant| grant.is_wildcard_resource() && grant.allows_write())
        })
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
                !check_grants(std::slice::from_ref(&ignored), repository, Access::Read),
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
    fn a_token_without_a_resources_claim_grants_nothing() {
        let repository: RepositoryId = rand::random();
        for absent in [None, Some(token(None)), Some(token(Some(vec![])))] {
            let absent = absent.as_ref();
            assert!(
                !token_grants(absent, repository, Access::Read),
                "{absent:?}"
            );
            assert!(
                !token_grants(absent, repository, Access::Write),
                "{absent:?}"
            );
            assert!(token_scope(absent).is_empty(), "{absent:?}");
            assert!(!may_create(absent), "{absent:?}");
        }
    }

    #[test]
    fn a_token_grants_only_what_it_names() {
        let scoped: RepositoryId = rand::random();
        let other: RepositoryId = rand::random();
        let reader = token(Some(vec![(resource_id(scoped).as_str(), &["read"][..])]));
        assert!(token_grants(Some(&reader), scoped, Access::Read));
        assert!(!token_grants(Some(&reader), scoped, Access::Write));
        assert!(!token_grants(Some(&reader), other, Access::Read));

        let staff = token(Some(vec![(WILDCARD, &["read", "write"][..])]));
        assert!(token_grants(Some(&staff), other, Access::Write));
    }

    #[test]
    fn a_token_scope_lists_its_own_readable_grants() {
        let scoped: RepositoryId = rand::random();
        let unreadable: RepositoryId = rand::random();
        let reader = token(Some(vec![
            (resource_id(scoped).as_str(), &["read"][..]),
            (resource_id(scoped).as_str(), &["write"][..]),
            (resource_id(unreadable).as_str(), &[][..]),
            ("urc-not-a-repository", &["read"][..]),
            (WILDCARD, &[][..]),
        ]));
        let scope = token_scope(Some(&reader));
        assert_eq!(
            scope,
            ListScope {
                every_repository: false,
                repositories: vec![scoped],
            }
        );
        assert!(scope.covers(scoped));
        assert!(!scope.covers(unreadable));

        let staff = token_scope(Some(&token(Some(vec![(WILDCARD, &["read"][..])]))));
        assert!(staff.every_repository);
        assert!(staff.covers(rand::random()));
    }

    #[test]
    fn only_a_writable_wildcard_may_create() {
        let scoped = resource_id(rand::random());
        assert!(may_create(Some(&token(Some(vec![(
            WILDCARD,
            &["read", "write"][..]
        )])))));
        for resources in [
            vec![(WILDCARD, &["read"][..])],
            vec![(scoped.as_str(), &["read", "write"][..])],
            vec![(WILDCARD, &[][..]), (scoped.as_str(), &["write"][..])],
        ] {
            assert!(
                !may_create(Some(&token(Some(resources.clone())))),
                "{resources:?}"
            );
        }
    }
}
