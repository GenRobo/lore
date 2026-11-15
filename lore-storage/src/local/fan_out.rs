// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Fan-out helpers for the lazy progressive bucket layout used by
//! `LocalImmutableStore` and `LocalMutableStore`.

use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use zerocopy::FromBytes;
use zerocopy::FromZeros;
use zerocopy::Immutable;
use zerocopy::IntoBytes;

use crate::Hash;

/// The set of valid bucket counts a group can be at. Fan-out only ever moves a
/// group to the next-higher value in this list.
pub const LEVEL_LADDER: [usize; 5] = [1, 32, 64, 128, 256];

/// Maximum bucket count per group. Equals the last entry in `LEVEL_LADDER` and matches the
/// existing `BUCKET_COUNT` in the local store implementations. Server-mode stores start here.
pub const FAN_OUT_LEVEL_MAX: usize = 256;

/// Default per-bucket entry threshold that triggers fan-out at the next serialize. Used by
/// `MutableStoreSettings::default()` and `ImmutableStoreSettings::default()`.
pub const FAN_OUT_THRESHOLD_DEFAULT: usize = 1000;

/// Filename of the per-group level marker, relative to the group directory.
pub const MARKER_FILENAME: &str = "level";

/// Filename of the per-group two-phase commit sentinel, relative to the group directory.
/// Present iff a fan-out commit started but did not yet reach the marker write step. Recovery
/// during store open detects this and rolls forward.
pub const LEVEL_PENDING_FILENAME: &str = "level.pending";

/// Filename suffix appended to bucket files during a fan-out commit. Bucket file `index_<bb>`
/// gets written to `index_<bb>.new` first; the rename to the final name only happens after
/// every `.new` is on disk and the `level.pending` sentinel has been written.
pub const BUCKET_NEW_SUFFIX: &str = ".new";

/// Magic value at the start of the marker file. Bytes spell `LVNO` on disk.
const MARKER_MAGIC: u32 = u32::from_le_bytes(*b"LVNO");

/// Marker file format version. Independent of `MutableStoreVersion` /
/// `ImmutableStoreVersion`; bumped only if the marker file's binary layout changes.
const MARKER_VERSION: u32 = 1;

#[repr(C)]
#[derive(Default, IntoBytes, FromBytes, Immutable)]
struct LevelMarkerHeader {
    magic: u32,
    version: u32,
    bucket_count: u32,
    _reserved: u32,
}

/// Compute the bucket index for `key` at a group whose bucket count is `bucket_count`.
///
/// The same hash byte (`key.data()[1]`) is interpreted at different bit widths depending on
/// the level: level 1 always returns 0; level `N` (a power of two in `{32, 64, 128, 256}`)
/// returns the high `log2(N)` bits of `key.data()[1]`. Going from level `N` to level `M` (with
/// `M > N`, both in the ladder) splits each old bucket into `M / N` new buckets — the new
/// index is the same hash byte right-shifted by a smaller amount, so an entry's new bucket
/// is always within the contiguous range `[old_idx * M/N, old_idx * M/N + M/N - 1]`.
///
/// # Arguments
/// * `key` — the entry's hash. Only `data()[1]` is consulted; `data()[0]` selected the group
///   and is irrelevant here.
/// * `bucket_count` — the group's current `bucket_count`. Must be `1` or a power of two in
///   `[2, 256]`.
///
/// # Returns
/// The bucket index in `[0, bucket_count)`.
///
/// # Invariants
/// * `bucket_count == 1 || (bucket_count.is_power_of_two() && bucket_count <= 256)`. Violation
///   is a `debug_assert!` (release builds skip the check).
/// * Result is always in `[0, bucket_count)`.
pub fn bucket_index_for(key: &Hash, bucket_count: usize) -> usize {
    debug_assert!(
        bucket_count == 1 || (bucket_count.is_power_of_two() && bucket_count <= 256),
        "bucket_count must be 1 or a power of two ≤ 256, got {bucket_count}"
    );
    if bucket_count <= 1 {
        return 0;
    }
    let shift = (256usize / bucket_count).trailing_zeros();
    (key.data()[1] as usize) >> shift
}

/// Compute the target level for a group whose largest bucket has overshot the threshold.
///
/// Returns the smallest value in `LEVEL_LADDER` such that splitting the group's worst bucket
/// `M / current_level` ways would bring its expected post-split entry count to or below
/// `threshold`. Concretely: returns the smallest `M ∈ LEVEL_LADDER` with
/// `M ≥ ceil(current_level * b_max / threshold)`, capped at 256.
///
/// # Arguments
/// * `current_level` — the group's current `bucket_count`. Typically a value from
///   `LEVEL_LADDER`.
/// * `b_max` — the maximum entry count observed across the group's buckets.
/// * `threshold` — the per-bucket fan-out threshold from the store's settings.
///
/// # Returns
/// A value from `LEVEL_LADDER` (always ≤ 256). When `b_max ≤ threshold` the function returns
/// the smallest ladder value ≥ `current_level`, which for typical inputs equals
/// `current_level` itself (no transition needed).
///
/// # Invariants
/// * `threshold > 0`. Violation is a `debug_assert!`.
/// * Multiplication `current_level * b_max` saturates at `usize::MAX` rather than overflowing.
/// * Result is always a member of `LEVEL_LADDER`.
pub fn level_for(current_level: usize, b_max: usize, threshold: usize) -> usize {
    debug_assert!(threshold > 0, "threshold must be positive");
    let product = current_level.saturating_mul(b_max);
    let required = product.div_ceil(threshold);
    LEVEL_LADDER
        .iter()
        .copied()
        .find(|&m| m >= required)
        .unwrap_or(256)
}

/// Read the level marker file in `group_path`, returning the recorded bucket count.
///
/// # Arguments
/// * `group_path` — the per-group directory (e.g., `<store>/index/<gg>/`).
///
/// # Returns
/// * `Ok(None)` when no marker file exists; the caller should default to 256 (today's
///   layout for legacy stores untouched by fan-out-aware code).
/// * `Ok(Some(level))` when the marker is present, has a valid magic and version, and
///   parses to a level value.
/// * `Err(io::Error)` for I/O failures, truncated files, mismatched magic, or unsupported
///   version. The error kind is `InvalidData` for corruption.
///
/// # Invariants
/// * The marker file is always exactly `size_of::<LevelMarkerHeader>()` bytes.
/// * A successfully-parsed marker has `magic == MARKER_MAGIC` and `version == MARKER_VERSION`.
pub fn read_level_marker(group_path: &Path) -> std::io::Result<Option<usize>> {
    read_level_header_file(&group_path.join(MARKER_FILENAME))
}

/// Write the level marker file in `group_path`, recording the current bucket count.
///
/// # Arguments
/// * `group_path` — the per-group directory. Must already exist.
/// * `level` — the bucket count to record. Should be a value from `LEVEL_LADDER`.
/// * `sync_data` — when true, the marker has reached the storage media before returning (the
///   file is fsynced before it is published and the directory after).
///
/// # Returns
/// * `Ok(())` on success.
/// * `Err(io::Error)` for any I/O failure during file creation, write, rename, or fsync.
///
/// # Invariants
/// * Existing marker file (if any) is replaced atomically: a reader, or a process restarted
///   after a kill at any point, finds the previous marker or the new one, never a short file.
/// * `level` is cast to u32; must fit (always true for ladder values ≤ 256).
pub fn write_level_marker(group_path: &Path, level: usize, sync_data: bool) -> std::io::Result<()> {
    write_level_header_file(&group_path.join(MARKER_FILENAME), level, sync_data)
}

/// Format the path for a bucket index file inside a group directory: `<group_path>/index_<bb>`
/// where `<bb>` is the lowercase 2-digit hex of `bucket_index as u8`.
///
/// # Invariants
/// * `bucket_index` is cast to `u8`; values ≥ 256 wrap, but `bucket_index` is always in
///   `[0, 256)` per the level ladder.
pub fn bucket_path(group_path: &Path, bucket_index: usize) -> PathBuf {
    group_path.join(format!("index_{:02x}", bucket_index as u8))
}

/// Format the path for the in-progress (`.new`) twin of a bucket index file used during
/// fan-out commits.
pub fn bucket_new_path(group_path: &Path, bucket_index: usize) -> PathBuf {
    group_path.join(format!(
        "index_{:02x}{}",
        bucket_index as u8, BUCKET_NEW_SUFFIX
    ))
}

/// Read the `level.pending` sentinel in `group_path`, returning the recorded target bucket count.
///
/// The pending file shares the binary layout of the level marker (same 16-byte header) so the
/// existing read path can be reused.
///
/// # Returns
/// * `Ok(None)` when no pending file exists.
/// * `Ok(Some(level))` when present and well-formed.
/// * `Err(io::Error)` for I/O failures, truncation, mismatched magic, or unsupported version.
pub fn read_level_pending(group_path: &Path) -> std::io::Result<Option<usize>> {
    read_level_header_file(&group_path.join(LEVEL_PENDING_FILENAME))
}

/// Write the `level.pending` sentinel in `group_path` with the recorded target bucket count.
/// Same binary layout as the level marker.
pub fn write_level_pending(
    group_path: &Path,
    level: usize,
    sync_data: bool,
) -> std::io::Result<()> {
    write_level_header_file(&group_path.join(LEVEL_PENDING_FILENAME), level, sync_data)
}

/// Delete the `level.pending` sentinel. Returns `Ok(())` whether or not it existed.
pub fn delete_level_pending(group_path: &Path) -> std::io::Result<()> {
    match std::fs::remove_file(group_path.join(LEVEL_PENDING_FILENAME)) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

/// Read a level-header file (marker or pending) from an explicit path. Shared format helper.
fn read_level_header_file(path: &Path) -> std::io::Result<Option<usize>> {
    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut header = LevelMarkerHeader::new_zeroed();
    file.read_exact(header.as_mut_bytes())?;
    if header.magic != MARKER_MAGIC {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "level header file has invalid magic 0x{:08x}, expected 0x{:08x}",
                header.magic, MARKER_MAGIC
            ),
        ));
    }
    if header.version != MARKER_VERSION {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "level header file has unsupported version {}, expected {}",
                header.version, MARKER_VERSION
            ),
        ));
    }
    Ok(Some(header.bucket_count as usize))
}

/// Write a level-header file (marker or pending) to an explicit path. Shared format helper.
/// The file is published by rename, see [`crate::fs_util::write_file_atomic`].
fn write_level_header_file(path: &Path, level: usize, sync_data: bool) -> std::io::Result<()> {
    let header = LevelMarkerHeader {
        magic: MARKER_MAGIC,
        version: MARKER_VERSION,
        bucket_count: level as u32,
        _reserved: 0,
    };
    crate::fs_util::write_file_atomic(path, header.as_bytes(), sync_data)
}

/// Whether `err` is what reading a level-header file reports when the file is shorter than its
/// header. Releases that wrote these files in place left them like that when the process was
/// killed between creating the file and writing it; files published by rename never are.
pub fn is_torn_level_header(err: &std::io::Error) -> bool {
    err.kind() == std::io::ErrorKind::UnexpectedEof
}

/// Parse `index_<bb>` (exactly, no suffix) into the bucket index.
fn parse_bucket_file_name(name: &std::ffi::OsStr) -> Option<usize> {
    let digits = name.to_str()?.strip_prefix("index_")?;
    if digits.len() != 2 {
        return None;
    }
    usize::from_str_radix(digits, 16).ok()
}

/// Work out the bucket count of a group from the bucket files it holds, for a group whose
/// level marker cannot be read.
///
/// `first_key` returns the key of any one entry stored in the given bucket file, or `None` when
/// the file holds no entry.
///
/// The result is exact, not a guess:
/// * A key stored in a bucket other than the first belongs there at exactly one level: the
///   index at level `N` is the key byte shifted right by `8 - log2(N)`, and two different
///   shifts of the same byte only agree when both give zero.
/// * A group whose only bucket file is `index_00` is a valid level 1 group whichever level
///   wrote it, because level 1 keeps every key of the group in that one bucket.
///
/// # Returns
/// * `Ok(level)` — a value from `LEVEL_LADDER`.
/// * `Err(io::Error)` when a bucket file cannot be read, or holds a key that belongs to it at
///   no level.
pub fn infer_level<F>(group_path: &Path, first_key: F) -> std::io::Result<usize>
where
    F: Fn(&Path) -> std::io::Result<Option<Hash>>,
{
    for entry in std::fs::read_dir(group_path)? {
        let entry = entry?;
        let Some(bucket_index) = parse_bucket_file_name(&entry.file_name()) else {
            continue;
        };
        if bucket_index == 0 {
            continue;
        }
        let path = entry.path();
        let Some(key) = first_key(&path)? else {
            continue;
        };
        return LEVEL_LADDER
            .iter()
            .copied()
            .find(|&level| bucket_index_for(&key, level) == bucket_index)
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "bucket file {} holds a key that belongs to it at no fan-out level",
                        path.display()
                    ),
                )
            });
    }
    Ok(1)
}

/// Replace a level marker that is shorter than its header with the level the group's bucket
/// files are laid out for (see [`infer_level`]) and return that level.
///
/// Call after [`recover_level_transition`]: a short marker next to a complete `level.pending`
/// is rewritten by the roll-forward and never gets here.
pub fn repair_level_marker<F>(group_path: &Path, first_key: F) -> std::io::Result<usize>
where
    F: Fn(&Path) -> std::io::Result<Option<Hash>>,
{
    let level = infer_level(group_path, first_key)?;
    lore_base::lore_warn!(
        "Level marker in {} was cut short by an interrupted write, restoring it as {level} from the bucket files",
        group_path.display()
    );
    write_level_marker(group_path, level, false)?;
    Ok(level)
}

/// Remove the `index_<bb>.new` files in `group_path` that a fan-out commit about to publish
/// its `level.pending` did not write itself, i.e. every bucket not listed in `written`
/// (ascending). They are what an earlier, interrupted commit left behind, and recovery after a
/// kill renames every `.new` it finds: a stale one must not ride along with this commit.
fn remove_stale_new_files(group_path: &Path, written: &[usize]) -> std::io::Result<()> {
    for entry in std::fs::read_dir(group_path)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(bucket_name) = name
            .to_str()
            .and_then(|name| name.strip_suffix(BUCKET_NEW_SUFFIX))
        else {
            continue;
        };
        let Some(bucket_index) = parse_bucket_file_name(std::ffi::OsStr::new(bucket_name)) else {
            continue;
        };
        if written.binary_search(&bucket_index).is_ok() {
            continue;
        }
        match std::fs::remove_file(entry.path()) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

/// Name the step of a level publication an I/O error comes from.
fn publish_step<T>(step: &str, result: std::io::Result<T>) -> std::io::Result<T> {
    result.map_err(|err| std::io::Error::new(err.kind(), format!("{step}: {err}")))
}

/// Publish the bucket count `level` for a group, after the buckets listed in `written`
/// (ascending) have been serialized to their `index_<bb>.new` files.
///
/// This is the two-phase commit of a group whose layout on disk changes, or that never had a
/// marker: `level.pending` is the commit point, then the `.new` files are renamed into place,
/// the level marker is written and `level.pending` is deleted. A process killed before the
/// commit point leaves the old layout untouched; killed after it, the next open rolls forward
/// (see [`recover_level_transition`]).
///
/// With no `.new` file there is nothing to roll forward and the marker is written directly.
///
/// With `sync_data` every step has reached the storage media before the next one starts,
/// except the removal of `level.pending`, which the caller's fsync of the directory covers
/// (replaying it after a power cut is harmless).
///
/// Blocking; an error leaves whatever was published in place and the next flush of the group
/// commits again.
pub fn publish_level(
    group_path: &Path,
    written: &[usize],
    level: usize,
    sync_data: bool,
) -> std::io::Result<()> {
    if written.is_empty() {
        return publish_step(
            "writing level marker for empty group",
            write_level_marker(group_path, level, sync_data),
        );
    }

    publish_step(
        "removing stale .new bucket files",
        remove_stale_new_files(group_path, written),
    )?;
    publish_step(
        "writing level.pending",
        write_level_pending(group_path, level, sync_data),
    )?;
    for &bucket_index in written {
        publish_step(
            "renaming .new bucket file during fan-out commit",
            crate::fs_util::rename_file(
                &bucket_new_path(group_path, bucket_index),
                &bucket_path(group_path, bucket_index),
            ),
        )?;
    }
    publish_step(
        "writing level marker",
        write_level_marker(group_path, level, sync_data),
    )?;
    publish_step("deleting level.pending", delete_level_pending(group_path))
}

/// Recover from a possibly-interrupted fan-out commit in `group_path`.
///
/// If `level.pending` exists, this function rolls forward by:
/// 1. Renaming `index_<bb>.new` → `index_<bb>` for each `bb in 0..target` whose `.new` file
///    is still present (already-renamed buckets are silently skipped).
/// 2. Writing the level marker for `target`.
/// 3. Deleting `level.pending`.
///
/// All steps are idempotent, so a recovery interrupted mid-way and re-run on the next open
/// converges to the same state.
///
/// A `level.pending` shorter than its header is dropped instead. The sentinel only becomes the
/// commit point once it is complete, and no `.new` file is renamed before that, so a short one
/// (left by a release that wrote it in place and was killed inside that write) means the
/// commit never started: the group keeps its previous layout. The `.new` files it leaves
/// behind are swept by the next commit, see [`publish_level`].
///
/// # Returns
/// * `Ok(None)` if no recovery was needed (no complete `level.pending` present).
/// * `Ok(Some(level))` if recovery rolled forward to `level`.
/// * `Err(io::Error)` if recovery encountered an I/O error. A `.new` file that cannot be
///   renamed aborts the routine with `level.pending` left in place, so the group is never
///   marked as being at `target` while one of its buckets is still at the old layout; the
///   next open retries.
pub fn recover_level_transition(
    group_path: &Path,
    sync_data: bool,
) -> std::io::Result<Option<usize>> {
    let target = match read_level_pending(group_path) {
        Ok(Some(level)) => level,
        Ok(None) => return Ok(None),
        Err(err) if is_torn_level_header(&err) => {
            lore_base::lore_warn!(
                "Dropping incomplete fan-out sentinel in {}, the commit it announced never started",
                group_path.display()
            );
            delete_level_pending(group_path)?;
            return Ok(None);
        }
        Err(err) => return Err(err),
    };

    for bb in 0..target {
        let new_path = bucket_new_path(group_path, bb);
        let final_path = bucket_path(group_path, bb);
        match crate::fs_util::rename_file(&new_path, &final_path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                // Either already renamed in a previous (interrupted) recovery attempt, or
                // never existed (an empty bucket at an index ≥ N skipped writing `.new`).
            }
            Err(err) => return Err(err),
        }
    }

    write_level_marker(group_path, target, sync_data)?;
    delete_level_pending(group_path)?;
    Ok(Some(target))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_with_byte_one(byte: u8) -> Hash {
        let mut hash = Hash::default();
        hash.data_mut()[1] = byte;
        hash
    }

    #[test]
    fn level_ladder_is_powers_of_two_starting_at_one() {
        assert_eq!(LEVEL_LADDER, [1, 32, 64, 128, 256]);
        for &m in &LEVEL_LADDER[1..] {
            assert!(m.is_power_of_two(), "level {m} must be power of two");
            assert!(
                m <= FAN_OUT_LEVEL_MAX,
                "level {m} must be <= FAN_OUT_LEVEL_MAX"
            );
        }
    }

    #[test]
    fn fan_out_level_max_matches_top_of_ladder() {
        assert_eq!(FAN_OUT_LEVEL_MAX, LEVEL_LADDER[LEVEL_LADDER.len() - 1]);
    }

    #[test]
    fn fan_out_threshold_default_is_one_thousand() {
        assert_eq!(FAN_OUT_THRESHOLD_DEFAULT, 1000);
    }

    #[test]
    fn bucket_index_for_level_one_always_zero() {
        for byte in [0u8, 0x05, 0x80, 0xFF] {
            let key = key_with_byte_one(byte);
            assert_eq!(bucket_index_for(&key, 1), 0);
        }
    }

    #[test]
    fn bucket_index_for_level_256_returns_full_byte() {
        for byte in [0u8, 0x05, 0x42, 0x80, 0xFF] {
            let key = key_with_byte_one(byte);
            assert_eq!(bucket_index_for(&key, 256), byte as usize);
        }
    }

    #[test]
    fn bucket_index_for_level_32_takes_top_5_bits() {
        // byte = 0b101_01010 (0xAA): top 5 bits = 0b10101 = 21
        let key = key_with_byte_one(0xAA);
        assert_eq!(bucket_index_for(&key, 32), 21);
        // byte = 0xFF: top 5 bits = 0b11111 = 31
        let key = key_with_byte_one(0xFF);
        assert_eq!(bucket_index_for(&key, 32), 31);
        // byte = 0x00: 0
        let key = key_with_byte_one(0x00);
        assert_eq!(bucket_index_for(&key, 32), 0);
    }

    #[test]
    fn bucket_index_for_level_64_takes_top_6_bits() {
        // byte = 0xAA = 0b10101010: top 6 bits = 0b101010 = 42
        let key = key_with_byte_one(0xAA);
        assert_eq!(bucket_index_for(&key, 64), 42);
    }

    #[test]
    fn bucket_index_for_level_128_takes_top_7_bits() {
        // byte = 0xAA = 0b10101010: top 7 bits = 0b1010101 = 85
        let key = key_with_byte_one(0xAA);
        assert_eq!(bucket_index_for(&key, 128), 85);
    }

    #[test]
    fn split_n_to_2n_preserves_bucket_membership_via_high_bit() {
        // For any byte, the bucket at level 2N is either 2*idx_N or 2*idx_N + 1.
        for byte in 0..=255u8 {
            let key = key_with_byte_one(byte);
            let idx_32 = bucket_index_for(&key, 32);
            let idx_64 = bucket_index_for(&key, 64);
            assert!(
                idx_64 == 2 * idx_32 || idx_64 == 2 * idx_32 + 1,
                "byte 0x{byte:02x}: idx_32={idx_32}, idx_64={idx_64}"
            );
        }
    }

    #[test]
    fn level_for_below_threshold_is_current_level() {
        // b_max ≤ threshold ⇒ no fan-out required ⇒ current_level returned (still in ladder).
        assert_eq!(level_for(1, 800, 1000), 1);
        assert_eq!(level_for(1, 1000, 1000), 1);
        assert_eq!(level_for(32, 999, 1000), 32);
    }

    #[test]
    fn level_for_5k_at_level_1_is_32() {
        // 1 * 5000 / 1000 = 5, smallest ladder M >= 5 is 32
        assert_eq!(level_for(1, 5000, 1000), 32);
    }

    #[test]
    fn level_for_1500_at_level_32_is_64() {
        // 32 * 1500 / 1000 = 48, smallest ladder M >= 48 is 64
        assert_eq!(level_for(32, 1500, 1000), 64);
    }

    #[test]
    fn level_for_1500_at_level_128_is_256() {
        // 128 * 1500 / 1000 = 192, smallest ladder M >= 192 is 256
        assert_eq!(level_for(128, 1500, 1000), 256);
    }

    #[test]
    fn level_for_caps_at_256() {
        // Even an extreme b_max can't push us past 256.
        assert_eq!(level_for(128, 10_000, 1000), 256);
        assert_eq!(level_for(256, 10_000, 1000), 256);
    }

    #[test]
    fn level_for_uses_ceiling_division() {
        // 1*1001/1000 ceiling is 2, smallest ladder M ≥ 2 is 32; floor would wrongly return current_level=1 even though trigger has fired.
        assert_eq!(level_for(1, 1001, 1000), 32);
    }

    fn temp_group_dir() -> crate::test_util::TempDir {
        crate::test_util::TempDir::new("fan_out_marker_")
    }

    #[test]
    fn read_level_marker_missing_returns_none() {
        let dir = temp_group_dir();
        assert_eq!(read_level_marker(dir.path()).unwrap(), None);
    }

    #[test]
    fn write_then_read_round_trip_every_ladder_value() {
        for &level in &LEVEL_LADDER {
            let dir = temp_group_dir();
            write_level_marker(dir.path(), level, true).unwrap();
            assert_eq!(read_level_marker(dir.path()).unwrap(), Some(level));
        }
    }

    #[test]
    fn read_level_marker_with_corrupt_magic_errors() {
        let dir = temp_group_dir();
        let marker = dir.path().join(MARKER_FILENAME);
        std::fs::write(
            &marker,
            [0xDE, 0xAD, 0xBE, 0xEF, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        )
        .unwrap();
        let err = read_level_marker(dir.path()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn read_level_marker_truncated_errors() {
        let dir = temp_group_dir();
        let marker = dir.path().join(MARKER_FILENAME);
        std::fs::write(&marker, [b'L', b'V', b'N', b'O', 1, 0, 0, 0]).unwrap();
        let err = read_level_marker(dir.path()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn read_level_marker_unsupported_version_errors() {
        let dir = temp_group_dir();
        let marker = dir.path().join(MARKER_FILENAME);
        let mut bytes = vec![];
        bytes.extend_from_slice(&MARKER_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&999u32.to_le_bytes());
        bytes.extend_from_slice(&32u32.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        std::fs::write(&marker, bytes).unwrap();
        let err = read_level_marker(dir.path()).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn write_level_marker_overwrites_existing() {
        let dir = temp_group_dir();
        write_level_marker(dir.path(), 1, false).unwrap();
        write_level_marker(dir.path(), 64, false).unwrap();
        assert_eq!(read_level_marker(dir.path()).unwrap(), Some(64));
    }

    #[test]
    fn marker_file_is_exactly_16_bytes() {
        let dir = temp_group_dir();
        write_level_marker(dir.path(), 32, false).unwrap();
        let metadata = std::fs::metadata(dir.path().join(MARKER_FILENAME)).unwrap();
        assert_eq!(metadata.len(), 16);
    }

    #[test]
    fn read_level_pending_missing_returns_none() {
        let dir = temp_group_dir();
        assert_eq!(read_level_pending(dir.path()).unwrap(), None);
    }

    #[test]
    fn level_pending_round_trip_every_ladder_value() {
        for &level in &LEVEL_LADDER {
            let dir = temp_group_dir();
            write_level_pending(dir.path(), level, false).unwrap();
            assert_eq!(read_level_pending(dir.path()).unwrap(), Some(level));
        }
    }

    #[test]
    fn delete_level_pending_is_idempotent() {
        let dir = temp_group_dir();
        // Delete on missing file succeeds.
        delete_level_pending(dir.path()).unwrap();
        // Delete after write removes it.
        write_level_pending(dir.path(), 32, false).unwrap();
        delete_level_pending(dir.path()).unwrap();
        assert_eq!(read_level_pending(dir.path()).unwrap(), None);
        // Second delete is also fine.
        delete_level_pending(dir.path()).unwrap();
    }

    #[test]
    fn bucket_path_lowercase_two_digit_hex() {
        let dir = temp_group_dir();
        assert_eq!(
            bucket_path(dir.path(), 0).file_name().unwrap(),
            std::ffi::OsStr::new("index_00")
        );
        assert_eq!(
            bucket_path(dir.path(), 0xab).file_name().unwrap(),
            std::ffi::OsStr::new("index_ab")
        );
        assert_eq!(
            bucket_path(dir.path(), 255).file_name().unwrap(),
            std::ffi::OsStr::new("index_ff")
        );
    }

    #[test]
    fn bucket_new_path_appends_dot_new() {
        let dir = temp_group_dir();
        assert_eq!(
            bucket_new_path(dir.path(), 0xab).file_name().unwrap(),
            std::ffi::OsStr::new("index_ab.new")
        );
    }

    #[test]
    fn recover_level_transition_no_pending_is_noop() {
        let dir = temp_group_dir();
        assert_eq!(recover_level_transition(dir.path(), false).unwrap(), None);
        // No marker should be created when there's nothing to recover.
        assert_eq!(read_level_marker(dir.path()).unwrap(), None);
    }

    #[test]
    fn recover_level_transition_renames_all_new_files() {
        let dir = temp_group_dir();
        // Set up "after pending, before any rename" state for target=4: four .new files exist with synthetic content; pending says target=4; no marker yet.
        for bb in 0..4 {
            std::fs::write(bucket_new_path(dir.path(), bb), [bb as u8; 8]).unwrap();
        }
        write_level_pending(dir.path(), 4, false).unwrap();

        let recovered = recover_level_transition(dir.path(), false).unwrap();
        assert_eq!(recovered, Some(4));

        // All .new files renamed to final.
        for bb in 0..4 {
            assert!(!bucket_new_path(dir.path(), bb).exists());
            let bytes = std::fs::read(bucket_path(dir.path(), bb)).unwrap();
            assert_eq!(bytes, vec![bb as u8; 8]);
        }
        // Marker reflects target.
        assert_eq!(read_level_marker(dir.path()).unwrap(), Some(4));
        // Pending deleted.
        assert!(!dir.path().join(LEVEL_PENDING_FILENAME).exists());
    }

    #[test]
    fn recover_level_transition_skips_already_renamed_buckets() {
        let dir = temp_group_dir();
        // Mid-rename state: bb=0 already renamed (final present, no .new); bb=1 still .new.
        std::fs::write(bucket_path(dir.path(), 0), b"final-0").unwrap();
        std::fs::write(bucket_new_path(dir.path(), 1), b"new-1").unwrap();
        write_level_pending(dir.path(), 2, false).unwrap();

        let recovered = recover_level_transition(dir.path(), false).unwrap();
        assert_eq!(recovered, Some(2));

        assert_eq!(
            std::fs::read(bucket_path(dir.path(), 0)).unwrap(),
            b"final-0"
        );
        assert_eq!(std::fs::read(bucket_path(dir.path(), 1)).unwrap(), b"new-1");
        assert!(!bucket_new_path(dir.path(), 1).exists());
        assert_eq!(read_level_marker(dir.path()).unwrap(), Some(2));
        assert!(!dir.path().join(LEVEL_PENDING_FILENAME).exists());
    }

    #[test]
    fn recover_level_transition_is_idempotent() {
        let dir = temp_group_dir();
        for bb in 0..2 {
            std::fs::write(bucket_new_path(dir.path(), bb), [bb as u8; 4]).unwrap();
        }
        write_level_pending(dir.path(), 2, false).unwrap();

        let first = recover_level_transition(dir.path(), false).unwrap();
        let second = recover_level_transition(dir.path(), false).unwrap();
        // First run rolls forward; second run is a no-op since pending is gone.
        assert_eq!(first, Some(2));
        assert_eq!(second, None);
        assert_eq!(read_level_marker(dir.path()).unwrap(), Some(2));
    }

    /// The level header bytes a writer killed part-way through an in-place write left behind.
    const TORN_HEADER: &[u8] = &[];

    #[test]
    fn level_marker_survives_a_writer_killed_before_the_rename() {
        let dir = temp_group_dir();
        write_level_marker(dir.path(), 32, false).unwrap();

        // A replacement that got as far as part of its temporary file and no further
        let temporary = crate::fs_util::temporary_path(&dir.path().join(MARKER_FILENAME));
        std::fs::write(&temporary, [b'L', b'V', b'N']).unwrap();
        assert_eq!(read_level_marker(dir.path()).unwrap(), Some(32));

        // The next writer is not in the way of the leftover and leaves none itself
        write_level_marker(dir.path(), 64, true).unwrap();
        assert_eq!(read_level_marker(dir.path()).unwrap(), Some(64));
        assert!(!temporary.exists());
    }

    #[test]
    fn recover_level_transition_drops_torn_pending() {
        let dir = temp_group_dir();
        write_level_marker(dir.path(), 256, false).unwrap();
        std::fs::write(bucket_path(dir.path(), 0x9d), b"committed").unwrap();
        // A commit that was killed while creating its sentinel: nothing was renamed yet
        std::fs::write(bucket_new_path(dir.path(), 0x9d), b"uncommitted").unwrap();
        std::fs::write(dir.path().join(LEVEL_PENDING_FILENAME), TORN_HEADER).unwrap();
        assert!(read_level_pending(dir.path()).is_err());

        assert_eq!(recover_level_transition(dir.path(), false).unwrap(), None);

        assert!(!dir.path().join(LEVEL_PENDING_FILENAME).exists());
        assert_eq!(read_level_marker(dir.path()).unwrap(), Some(256));
        assert_eq!(
            std::fs::read(bucket_path(dir.path(), 0x9d)).unwrap(),
            b"committed"
        );
    }

    #[test]
    fn recover_level_transition_rewrites_torn_marker_from_pending() {
        let dir = temp_group_dir();
        // Killed while writing the marker, the last step before the sentinel is deleted
        std::fs::write(bucket_path(dir.path(), 0xd4), b"renamed").unwrap();
        std::fs::write(bucket_new_path(dir.path(), 0xfe), b"not yet renamed").unwrap();
        write_level_pending(dir.path(), 256, false).unwrap();
        std::fs::write(dir.path().join(MARKER_FILENAME), TORN_HEADER).unwrap();

        assert_eq!(
            recover_level_transition(dir.path(), false).unwrap(),
            Some(256)
        );

        assert_eq!(read_level_marker(dir.path()).unwrap(), Some(256));
        assert_eq!(
            std::fs::read(bucket_path(dir.path(), 0xfe)).unwrap(),
            b"not yet renamed"
        );
        assert!(!dir.path().join(LEVEL_PENDING_FILENAME).exists());
    }

    #[test]
    fn recover_level_transition_keeps_pending_when_a_rename_fails() {
        let dir = temp_group_dir();
        std::fs::write(bucket_new_path(dir.path(), 0), b"new-0").unwrap();
        // A directory with something in it cannot be replaced by the rename
        std::fs::create_dir_all(bucket_path(dir.path(), 0).join("in the way")).unwrap();
        write_level_pending(dir.path(), 32, false).unwrap();

        assert!(recover_level_transition(dir.path(), false).is_err());

        // Not marked as being at the new level while a bucket is still at the old one
        assert_eq!(read_level_marker(dir.path()).unwrap(), None);
        assert_eq!(read_level_pending(dir.path()).unwrap(), Some(32));
        assert!(bucket_new_path(dir.path(), 0).exists());
    }

    #[test]
    fn infer_level_of_a_single_bucket_group_is_one() {
        let dir = temp_group_dir();
        assert_eq!(infer_level(dir.path(), |_| Ok(None)).unwrap(), 1);

        // Whichever level wrote it, `index_00` on its own holds every key of the group
        std::fs::write(bucket_path(dir.path(), 0), b"bucket").unwrap();
        let level = infer_level(dir.path(), |_| -> std::io::Result<Option<Hash>> {
            panic!("the first bucket says nothing about the level")
        })
        .unwrap();
        assert_eq!(level, 1);
    }

    #[test]
    fn infer_level_from_a_key_in_a_later_bucket() {
        // Key byte 0xAA is bucket 21 at level 32, 42 at 64, 85 at 128 and 170 at 256
        for (bucket_index, level) in [(21, 32), (42, 64), (85, 128), (170, 256)] {
            let dir = temp_group_dir();
            std::fs::write(bucket_path(dir.path(), 0), b"bucket").unwrap();
            std::fs::write(bucket_path(dir.path(), bucket_index), b"bucket").unwrap();
            // Neither a leftover of a commit nor a temporary is a bucket file
            std::fs::write(bucket_new_path(dir.path(), 200), b"stale").unwrap();
            std::fs::write(
                crate::fs_util::temporary_path(&bucket_path(dir.path(), 201)),
                b"stale",
            )
            .unwrap();

            let inferred = infer_level(dir.path(), |path| {
                assert_eq!(path, bucket_path(dir.path(), bucket_index));
                Ok(Some(key_with_byte_one(0xAA)))
            })
            .unwrap();
            assert_eq!(inferred, level);
        }
    }

    #[test]
    fn infer_level_skips_empty_buckets_and_rejects_misplaced_keys() {
        let dir = temp_group_dir();
        std::fs::write(bucket_path(dir.path(), 7), b"bucket").unwrap();
        assert_eq!(infer_level(dir.path(), |_| Ok(None)).unwrap(), 1);

        // Key byte 0xAA is in bucket 7 at no level
        let err = infer_level(dir.path(), |_| Ok(Some(key_with_byte_one(0xAA)))).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn repair_level_marker_restores_a_torn_marker() {
        let dir = temp_group_dir();
        std::fs::write(bucket_path(dir.path(), 0x12), b"bucket").unwrap();
        std::fs::write(dir.path().join(MARKER_FILENAME), TORN_HEADER).unwrap();
        let err = read_level_marker(dir.path()).unwrap_err();
        assert!(is_torn_level_header(&err));

        let level = repair_level_marker(dir.path(), |_| Ok(Some(key_with_byte_one(0x12)))).unwrap();

        assert_eq!(level, 256);
        assert_eq!(read_level_marker(dir.path()).unwrap(), Some(256));
    }

    #[test]
    fn publish_level_commits_the_new_files_and_sweeps_stale_ones() {
        let dir = temp_group_dir();
        std::fs::write(bucket_path(dir.path(), 3), b"old-3").unwrap();
        std::fs::write(bucket_path(dir.path(), 5), b"old-5").unwrap();
        std::fs::write(bucket_new_path(dir.path(), 3), b"new-3").unwrap();
        // Left by an earlier commit that was killed, possibly cut short itself
        std::fs::write(bucket_new_path(dir.path(), 5), b"").unwrap();

        publish_level(dir.path(), &[3], 256, true).unwrap();

        assert_eq!(std::fs::read(bucket_path(dir.path(), 3)).unwrap(), b"new-3");
        assert_eq!(std::fs::read(bucket_path(dir.path(), 5)).unwrap(), b"old-5");
        assert!(!bucket_new_path(dir.path(), 3).exists());
        assert!(!bucket_new_path(dir.path(), 5).exists());
        assert_eq!(read_level_marker(dir.path()).unwrap(), Some(256));
        assert!(!dir.path().join(LEVEL_PENDING_FILENAME).exists());
    }

    #[test]
    fn publish_level_without_new_files_only_writes_the_marker() {
        let dir = temp_group_dir();
        std::fs::write(bucket_new_path(dir.path(), 5), b"stale").unwrap();

        publish_level(dir.path(), &[], 1, false).unwrap();

        assert_eq!(read_level_marker(dir.path()).unwrap(), Some(1));
        assert!(!dir.path().join(LEVEL_PENDING_FILENAME).exists());
        // Nothing renames a `.new` file without a sentinel, so it is left for the next commit
        assert!(bucket_new_path(dir.path(), 5).exists());
        assert!(!bucket_path(dir.path(), 5).exists());
    }
}
