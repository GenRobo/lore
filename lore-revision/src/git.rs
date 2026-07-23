// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

//! Cross-VCS guard: detection of a git working tree overlapping the Lore workspace.
//!
//! A Lore workspace can share its root with a git working tree (source in git, assets in
//! Lore). That layout is only safe while the two tracked sets stay disjoint: a path staged
//! into Lore that git also tracks will be overwritten or deleted from the git tree by a
//! backward sync. This module detects the overlapping git working tree and snapshots its
//! tracked set so staging can enforce disjointness as an invariant instead of relying on the
//! ignore allowlist being configured correctly.

use std::collections::HashSet;
use std::path::Path;

use crate::lore_debug;
use crate::lore_error;

/// Presence and tracked set of a git working tree covering the workspace root.
pub enum GitPresence {
    /// No git working tree at or above the workspace root.
    Absent,
    /// A git working tree covers the workspace; the set holds its tracked paths relative to
    /// the workspace root, lowercased and forward-slash separated.
    Tracked(HashSet<String>),
    /// A git working tree exists but its tracked set could not be read (the `git` executable
    /// is missing or failed), so the disjointness guard cannot be enforced.
    Unavailable,
}

impl GitPresence {
    /// Whether a workspace-relative path is tracked by the overlapping git working tree.
    pub fn tracks(&self, relative: &str) -> bool {
        match self {
            GitPresence::Tracked(set) => set.contains(&relative.to_lowercase()),
            GitPresence::Absent | GitPresence::Unavailable => false,
        }
    }
}

/// Whether a `.git` entry exists directly at `root` (a directory, or a file for git
/// worktrees and submodules) — the shared-root layout the root-mount fail-safe guards.
pub fn git_at_root(root: &Path) -> bool {
    root.join(".git").exists()
}

/// Detect a git working tree at or above `root` and snapshot its tracked set. Detection is a
/// filesystem probe only; `git ls-files` runs (once) only when a `.git` is actually present,
/// so workspaces without git never pay for a subprocess.
pub fn detect(root: &Path) -> GitPresence {
    if !root.ancestors().any(git_at_root) {
        return GitPresence::Absent;
    }
    match snapshot_tracked(root) {
        Ok(tracked) => {
            lore_debug!(
                "Cross-VCS guard: git working tree covers {} ({} tracked paths)",
                root.display(),
                tracked.len()
            );
            GitPresence::Tracked(tracked)
        }
        Err(err) => {
            lore_error!(
                "A git working tree covers {} but its tracked files could not be listed ({err}); \
                 the cross-VCS staging guard is NOT enforced",
                root.display()
            );
            GitPresence::Unavailable
        }
    }
}

/// Paths tracked by git under `root`, relative to `root` (git emits them relative to the
/// directory it runs in), lowercased for case-insensitive comparison against Lore paths.
fn snapshot_tracked(root: &Path) -> std::io::Result<HashSet<String>> {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["ls-files", "-z"])
        .output()?;
    if !output.status.success() {
        return Err(std::io::Error::other(format!(
            "git ls-files failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(output
        .stdout
        .split(|&byte| byte == 0)
        .filter(|entry| !entry.is_empty())
        .map(|entry| String::from_utf8_lossy(entry).to_lowercase())
        .collect())
}
