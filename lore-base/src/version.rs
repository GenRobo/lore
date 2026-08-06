// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::sync::LazyLock;

pub static LORE_LIBRARY_VERSION: LazyLock<String> =
    LazyLock::new(|| env!("VERGEN_LORE_LIBRARY_VERSION_NAME").to_owned());

pub static LORE_LIBRARY_VERSION_CSTR: &str =
    concat!(env!("VERGEN_LORE_LIBRARY_VERSION_NAME"), "\0");

/// Source commit this binary was built from, or "unknown" when the build had
/// neither LORE_BUILD_SHA nor a git tree. This — not the version name — is what
/// identifies a build: the version name is CARGO_PKG_VERSION plus a Lore revision
/// number that degrades to "0", so it is identical across a whole lineage.
pub static LORE_BUILD_SHA: LazyLock<String> =
    LazyLock::new(|| env!("VERGEN_LORE_BUILD_SHA").to_owned());

/// Abbreviated build commit, for log lines and the client/server handshake.
pub fn lore_build_sha_short() -> &'static str {
    static SHORT: LazyLock<String> = LazyLock::new(|| {
        let sha = LORE_BUILD_SHA.as_str();
        sha.get(..12).unwrap_or(sha).to_owned()
    });
    SHORT.as_str()
}

/// True when this build could not determine its own commit. Such a build cannot
/// prove a match, so the handshake must treat it as unmatchable rather than as
/// agreeing with whatever it is talking to.
pub fn lore_build_sha_is_unknown() -> bool {
    LORE_BUILD_SHA.as_str() == "unknown"
}

/// Operator-facing version line: the version name plus the build commit. This is
/// what `--version` must print — the version name alone is identical across a
/// whole lineage, so without the commit an operator cannot tell what is deployed.
pub static LORE_VERSION_WITH_BUILD: LazyLock<String> = LazyLock::new(|| {
    format!(
        "{} (build {})",
        LORE_LIBRARY_VERSION.as_str(),
        lore_build_sha_short()
    )
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_line_names_the_build_commit() {
        assert!(LORE_VERSION_WITH_BUILD.contains(lore_build_sha_short()));
        assert!(LORE_VERSION_WITH_BUILD.contains(LORE_LIBRARY_VERSION.as_str()));
    }
}
