// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::path::Path;

use bitflags::bitflags;
use lore_error_set::prelude::*;
use serde::Deserialize;
use serde::Serialize;

use crate::bitflagsops;
use crate::event::LoreEvent;
use crate::interface::LoreString;
use crate::lore_warn;
use crate::repository::BASE_SUFFIX;
use crate::repository::DOT_LORE;
use crate::repository::DOT_URC;
use crate::repository::MINE_SUFFIX;
use crate::repository::TEMP_FILE_EXTENSION;
use crate::repository::THEIRS_SUFFIX;
use crate::util::path::RelativePath;
use crate::util::path::RelativePathBuf;

#[derive(Clone, Default, Debug)]
pub struct Filter {
    pub ignore: FilterInstance,
    pub view: FilterInstance,
}

#[derive(Clone, Default, Debug)]
pub struct FilterInstance {
    pub lines: Vec<FilterLine>,
}

#[derive(Default, Clone, Debug)]
pub struct FilterLine {
    glob: String,
    negated: bool,
    directory: bool,
    generated: bool,
    filename: bool,
}

#[error_set]
pub enum FilterError {}

pub fn load(
    ignore_path: impl AsRef<Path>,
    view_path: impl AsRef<Path>,
) -> Result<Filter, FilterError> {
    let mut ignore = load_filter(ignore_path)?;
    ignore.add_exclusion(DOT_URC)?;
    ignore.add_exclusion(DOT_LORE)?;
    // Never track a co-located git repository's own database: at a shared root an
    // unconfigured `lore stage .` must not pull `.git/**` into Lore (a backward sync would
    // then rewrite it).
    ignore.add_exclusion(".git")?;
    ignore.add_exclusion(&format!("*{MINE_SUFFIX}"))?;
    ignore.add_exclusion(&format!("*{THEIRS_SUFFIX}"))?;
    ignore.add_exclusion(&format!("*{BASE_SUFFIX}"))?;
    ignore.add_exclusion(&format!("*{TEMP_FILE_EXTENSION}"))?;

    let view = load_filter(view_path)?;

    Ok(Filter { ignore, view })
}

pub fn load_view(view_path: impl AsRef<Path>) -> Result<Filter, FilterError> {
    Ok(Filter {
        ignore: FilterInstance::default(),
        view: load_filter(view_path)?,
    })
}

impl FilterInstance {
    /// Number of user-authored (non-generated) rules. Zero means the filter file was absent
    /// or parsed to nothing effective — e.g. an allowlist written on a single line — which
    /// callers that *require* a scoped allowlist (the destructive-sync fail-safe) treat the
    /// same as no file at all.
    pub fn user_rule_count(&self) -> usize {
        self.lines.iter().filter(|line| !line.generated).count()
    }
}

pub fn load_filter(path: impl AsRef<Path>) -> Result<FilterInstance, FilterError> {
    let mut filter = FilterInstance::default();
    if let Ok(file) = File::open(path) {
        let mut has_include = false;
        let mut has_exclude = false;
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            let mut glob = line.trim();
            if glob.is_empty() || glob.starts_with('#') {
                continue;
            }

            let mut negated = false;
            while glob.starts_with('!') {
                negated = !negated;
                glob = &glob[1..];
            }

            // Allow exclamation marks in path/file names through escape backslash
            if glob.starts_with("\\!") {
                glob = &glob[1..];
            }

            if negated {
                filter.add_inclusion(glob)?;
                has_include = true;
            } else {
                filter.add_exclusion(glob)?;
                has_exclude = true;
            }
        }

        if has_include && !has_exclude {
            lore_warn!(
                "Filter only has inclusions but no exclusions, this will not have any effect - did you forget to exclude all?"
            );
        }
    }
    Ok(filter)
}

pub fn save(filter: &FilterInstance, path: impl AsRef<Path>) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    for line in &filter.lines {
        if line.generated {
            continue;
        }

        let mut out = String::new();
        if line.negated {
            out.push('!');
        }
        if !line.filename && !line.glob.contains('/') {
            out.push('/');
        }
        out.push_str(&line.glob);
        if line.directory {
            out.push('/');
        }
        out.push('\n');

        file.write_all(out.as_bytes())?;
    }
    Ok(())
}

impl FilterInstance {
    pub fn add_exclusion(&mut self, glob: &str) -> Result<(), FilterError> {
        let leading_separator = glob.starts_with('/');
        let ending_separator = glob.ends_with('/');

        let glob = glob.trim_matches('/').to_lowercase();

        let filename = !leading_separator && !glob.contains('/') && glob != "**";
        {
            self.lines.push(FilterLine {
                glob: glob.clone(),
                negated: false,
                directory: ending_separator,
                generated: false,
                filename,
            });
        }
        if !filename || ending_separator {
            // If this item turns out to be a directory, and a subpath of this item
            // gets re-included by a later rule, we must ensure that everything else
            // in this subtree is properly excluded
            if !glob.ends_with('*') && !glob.ends_with("*/") {
                let mut glob = glob;
                if !glob.ends_with('/') {
                    glob.push('/');
                }
                glob.push_str("**");
                self.lines.push(FilterLine {
                    glob,
                    negated: false,
                    directory: false,
                    generated: true,
                    filename: false,
                });
            }
        }
        Ok(())
    }

    pub fn add_inclusion(&mut self, glob: &str) -> Result<(), FilterError> {
        if glob.starts_with("**") {
            return Err(FilterError::internal(
                "filter inclusions cannot start with ** as that will force traversal of the entire revision tree",
            ));
        }

        let leading_separator = glob.starts_with('/');
        let ending_separator = glob.ends_with('/');

        let glob = glob.trim_matches('/').to_lowercase();

        let filename = !leading_separator && !glob.contains('/');
        if !filename {
            // In order to properly force traversal of excluded directories to reach reincluded subpaths, like
            // Engine
            // !Engine/Sub/Path
            // we must add a directory match reinclusion of Engine and Engine/Sub in order to reach the reincluded
            // subpath Engine/Sub/Path - but if Engine/Sub is a file it should NOT be reincluded. Use a directory
            // match flag to achieve this
            let mut subpath = RelativePathBuf::new();
            let mut path_parts: Vec<&str> = glob.split('/').collect();
            path_parts.pop();
            for part in path_parts.iter() {
                subpath.push(part);
                self.lines.push(FilterLine {
                    glob: subpath.as_str().to_lowercase(),
                    negated: true,
                    directory: true,
                    generated: true,
                    filename: false,
                });
            }
        }

        self.lines.push(FilterLine {
            glob: glob.clone(),
            negated: true,
            directory: ending_separator,
            generated: false,
            filename,
        });

        if !filename {
            // Now, in order to make sure we also include anything below this path
            // add a glob pattern to re-include all the subtree items
            if !glob.ends_with('*') && !glob.ends_with("*/") {
                let mut glob = glob;
                if !glob.ends_with('/') {
                    glob.push('/');
                }
                glob.push_str("**");
                self.lines.push(FilterLine {
                    glob,
                    negated: true,
                    directory: false,
                    generated: true,
                    filename: false,
                });
            }
        }

        Ok(())
    }

    /// Returns whether `path` is excluded by applying every filter line in
    /// order, where a later matching line overrides earlier ones.
    ///
    /// An inclusion (negated) line can only clear `excluded` and an exclusion
    /// line can only set it, so a line whose effect equals the current state
    /// could at most match to no effect. Such lines are skipped before the glob
    /// match, which avoids evaluating inclusion lines while not yet excluded and
    /// exclusion lines while already excluded.
    pub fn excludes(&self, path: &RelativePath, is_directory: bool) -> bool {
        if path.is_empty() || path.as_str() == "." {
            return false;
        }
        let mut excluded = false;
        let match_path = path.as_lowercase_str();
        let match_filename = path.name_lowercase();
        for line in &self.lines {
            if line.negated != excluded {
                continue;
            }
            if line.directory && !is_directory {
                continue;
            }
            let to_match = if line.filename {
                match_filename
            } else {
                match_path
            };
            if glob_match::glob_match(line.glob.as_str(), to_match) {
                excluded = !line.negated;
            }
        }
        excluded
    }
}

/// Data for the event emitted when a path is excluded by a filter.
#[repr(C)]
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoreFilterExcludeEventData {
    /// Reason the path was excluded.
    pub reason: u8,
    /// Path that was excluded.
    pub path: LoreString,
}

pub enum FilterReason {
    Ignore = 0,
    View,
}

bitflags! {
    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct FilterMode: u16 {
        const Ignore = 0b1;
        const View = 0b10;
        const Full = 0b11;
    }
}
bitflagsops!(FilterMode, u16);

impl Filter {
    pub fn excludes(&self, path: &RelativePath, is_directory: bool, mode: FilterMode) -> bool {
        if path.is_empty() {
            return false;
        }
        if mode.contains(FilterMode::Ignore) && self.ignore.excludes(path, is_directory) {
            return true;
        }
        if mode.contains(FilterMode::View) && self.view.excludes(path, is_directory) {
            return true;
        }
        false
    }

    pub fn emit_excludes(&self, path: &RelativePath, is_directory: bool, mode: FilterMode) -> bool {
        if path.is_empty() {
            return false;
        }
        if mode.contains(FilterMode::Ignore) && self.ignore.excludes(path, is_directory) {
            LoreEvent::FilterExclude(LoreFilterExcludeEventData {
                reason: FilterReason::Ignore as u8,
                path: path.into(),
            })
            .send();
            return true;
        }
        if mode.contains(FilterMode::View) && self.view.excludes(path, is_directory) {
            LoreEvent::FilterExclude(LoreFilterExcludeEventData {
                reason: FilterReason::View as u8,
                path: path.into(),
            })
            .send();
            return true;
        }
        false
    }
}
