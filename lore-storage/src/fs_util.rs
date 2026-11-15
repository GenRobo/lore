// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

#[cfg(not(target_family = "windows"))]
pub fn rename_file<P: AsRef<Path>>(from: P, to: P) -> std::io::Result<()> {
    std::fs::rename(from.as_ref(), to.as_ref())
}

#[cfg(target_family = "windows")]
pub fn rename_file<P: AsRef<Path>>(from: P, to: P) -> std::io::Result<()> {
    use windows_sys::Win32::Storage::FileSystem::*;

    // `to_extended_wide` applies the \\?\ verbatim prefix only when the
    // path would otherwise exceed MAX_PATH, so short paths skip the prefix
    // overhead. MoveFileExW parses each parameter independently, so a
    // short non-prefixed source and a long prefixed destination (or any
    // mix) resolve correctly.
    let from = lore_base::fs::win_path::to_extended_wide(from.as_ref());
    let to = lore_base::fs::win_path::to_extended_wide(to.as_ref());

    // Safety: Call Win32 APIs, buffers are valid and null-terminated
    let ok = unsafe { MoveFileExW(from.as_ptr(), to.as_ptr(), MOVEFILE_REPLACE_EXISTING) };

    if ok == 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(target_family = "windows"))]
pub fn sync_dir<P: AsRef<Path>>(path: P) -> std::io::Result<()> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::OpenOptionsExt;

    let dir = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY)
        .open(path.as_ref())?;
    let fd = dir.as_raw_fd();
    // SAFETY: Safe to call libc function to flush directory changes
    let result = unsafe { libc::fsync(fd) };
    if result == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(target_family = "windows")]
pub fn sync_dir<P: AsRef<Path>>(_path: P) -> std::io::Result<()> {
    // No-op on Windows, there is no API to flush a directory
    Ok(())
}

/// Temporary sibling a file is written to before it is renamed over `path`.
///
/// The suffix is appended instead of replacing the extension, so the temporary of
/// `index_<bb>.new` is `index_<bb>.new.tmp` and cannot collide with the one of `index_<bb>`.
pub fn temporary_path(path: &Path) -> PathBuf {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    PathBuf::from(temporary)
}

/// Replace the file at `path` with `contents` so that a reader, or a process restarted after a
/// kill, finds either the previous file or the complete new one and never a truncated mix: the
/// bytes go to a temporary sibling which is then renamed over `path`.
///
/// With `sync_data` the temporary is fsynced before the rename and the directory after it, so
/// the replacement has reached the storage media when this returns.
pub fn write_file_atomic(path: &Path, contents: &[u8], sync_data: bool) -> std::io::Result<()> {
    let temporary = temporary_path(path);
    if let Err(err) = write_and_rename(&temporary, path, contents, sync_data) {
        // Nothing reads the temporary; a leftover would only be overwritten by the next write
        let _ = std::fs::remove_file(&temporary);
        return Err(err);
    }
    if sync_data && let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn write_and_rename(
    temporary: &Path,
    path: &Path,
    contents: &[u8],
    sync_data: bool,
) -> std::io::Result<()> {
    let mut file = std::fs::File::create(temporary)?;
    file.write_all(contents)?;
    if sync_data {
        file.sync_all()?;
    }
    drop(file);
    rename_file(temporary, path)
}

pub async fn unlink_recursive<P: AsRef<Path>>(absolute_path: P) -> tokio::io::Result<()> {
    let absolute_path = absolute_path.as_ref();
    lore_base::lore_trace!("Deleting {}", absolute_path.display());
    let metadata = tokio::fs::metadata(absolute_path).await;

    if let Err(_err) = metadata {
        return Ok(());
    }

    let metadata = metadata.unwrap();
    if metadata.is_dir() {
        if let Err(err) = tokio::fs::remove_dir_all(absolute_path).await {
            if err.kind() == tokio::io::ErrorKind::NotFound {
                return Ok(());
            }
            lore_base::lore_debug!(
                "Error deleting directory {}: {} - retry after setting write permission",
                absolute_path.display(),
                err
            );

            let mut permissions = metadata.permissions();
            #[allow(clippy::permissions_set_readonly_false)]
            permissions.set_readonly(false);
            let _ = tokio::fs::set_permissions(absolute_path, permissions).await;
            if let Err(err) = tokio::fs::remove_dir_all(absolute_path).await {
                if err.kind() == tokio::io::ErrorKind::NotFound {
                    return Ok(());
                }
                return Err(err);
            }
        }
    } else if let Err(err) = tokio::fs::remove_file(absolute_path).await {
        if err.kind() == tokio::io::ErrorKind::NotFound {
            return Ok(());
        }

        let mut permissions = metadata.permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        permissions.set_readonly(false);
        let _ = tokio::fs::set_permissions(absolute_path, permissions).await;
        if let Err(err) = tokio::fs::remove_file(absolute_path).await {
            if err.kind() == tokio::io::ErrorKind::NotFound {
                return Ok(());
            }
            return Err(err);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporary_path_appends_to_the_whole_name() {
        assert_eq!(
            temporary_path(Path::new("group/index_ab.new")),
            Path::new("group/index_ab.new.tmp")
        );
        assert_eq!(
            temporary_path(Path::new("group/level")),
            Path::new("group/level.tmp")
        );
    }

    #[test]
    fn write_file_atomic_replaces_the_file_and_leaves_no_temporary() {
        let dir = crate::test_util::TempDir::new("fs_util_atomic_");
        let path = dir.path().join("version");

        for (contents, sync_data) in [(&b"first"[..], false), (&b"second, longer"[..], true)] {
            write_file_atomic(&path, contents, sync_data).unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), contents);
            assert!(!temporary_path(&path).exists());
        }
    }

    #[test]
    fn write_file_atomic_keeps_the_previous_file_when_the_write_fails() {
        let dir = crate::test_util::TempDir::new("fs_util_atomic_failed_");
        let path = dir.path().join("version");
        write_file_atomic(&path, b"committed", false).unwrap();

        // The temporary cannot be created: a directory holds its name
        std::fs::create_dir(temporary_path(&path)).unwrap();

        assert!(write_file_atomic(&path, b"replacement", true).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"committed");
    }

    #[test]
    fn a_truncated_temporary_is_what_a_killed_writer_leaves() {
        let dir = crate::test_util::TempDir::new("fs_util_atomic_killed_");
        let path = dir.path().join("version");
        write_file_atomic(&path, b"committed", false).unwrap();

        // Killed after writing part of the temporary: the file itself was never touched
        std::fs::write(temporary_path(&path), b"repl").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"committed");

        write_file_atomic(&path, b"replacement", false).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
        assert!(!temporary_path(&path).exists());
    }
}
