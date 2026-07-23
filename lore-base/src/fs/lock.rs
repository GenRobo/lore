// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::fs::OpenOptions;
#[cfg(target_family = "unix")]
use std::os::fd::AsRawFd;
#[cfg(target_family = "windows")]
use std::os::windows::io::AsRawHandle;
use std::path::Path;
use std::time::Duration;

#[cfg(target_family = "windows")]
use windows_sys::Win32::Storage::FileSystem;

pub struct FSLock {
    file: std::fs::File,
}

impl FSLock {
    pub fn acquire_file_lock(path: impl AsRef<Path>) -> std::io::Result<FSLock> {
        let mut path = path.as_ref().to_path_buf();
        let mut file_name = path
            .file_name()
            .ok_or(std::io::Error::other(
                "Acquiring file lock on path with no file",
            ))?
            .to_owned();
        path.pop();
        let mut path = path.canonicalize()?;
        file_name.push(".lock");
        path.push(file_name);
        Self::acquire_exact_path(&path).map_err(|_err| {
            std::io::Error::other(format!("Failed to acquire lock file \"{path:?}\""))
        })
    }

    pub fn acquire_directory_lock(path: impl AsRef<Path>) -> std::io::Result<FSLock> {
        let path = path.as_ref().canonicalize()?.join("lock");
        Self::acquire_exact_path(&path)
    }

    fn acquire_exact_path(path: impl AsRef<Path> + Copy) -> std::io::Result<FSLock> {
        let mut retry = 2;
        let file = loop {
            let file = OpenOptions::new()
                .create(false)
                .truncate(false)
                .write(false)
                .read(true)
                .open(path);
            if let Ok(file) = file {
                break file;
            }

            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .read(true)
                .open(path);
            if let Ok(file) = file {
                break file;
            }

            retry -= 1;
            if retry == 0 {
                return Err(file.unwrap_err());
            }
            std::thread::sleep(Duration::from_millis(10));
        };

        // Poll a non-blocking lock attempt against the deadline rather than parking in the
        // OS's unbounded blocking acquire: a lock held for an open-ended time (a mounted
        // workspace holds its repository lock while serving) must produce a timely error,
        // never an indefinite hang.
        let deadline = std::time::Instant::now() + Self::acquire_timeout();
        loop {
            match Self::try_lock(&file) {
                Ok(()) => return Ok(Self { file }),
                Err(err) if Self::is_contended(&err) => {
                    if std::time::Instant::now() >= deadline {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!(
                                "lock {} is held by another process",
                                path.as_ref().display()
                            ),
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(200));
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// How long a contended lock acquisition waits before giving up: long enough to queue
    /// behind another short command, short enough that a lock held open-endedly fails fast.
    /// Override with `LORE_LOCK_TIMEOUT_SECS`.
    fn acquire_timeout() -> Duration {
        std::env::var("LORE_LOCK_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(10))
    }

    /// Whether a lock error means "currently held by someone else" (retryable until the
    /// deadline) rather than a real I/O failure.
    fn is_contended(err: &std::io::Error) -> bool {
        #[cfg(target_family = "windows")]
        {
            // ERROR_LOCK_VIOLATION = 33
            err.raw_os_error() == Some(33)
        }
        #[cfg(not(target_family = "windows"))]
        {
            err.kind() == std::io::ErrorKind::WouldBlock
        }
    }

    fn try_lock(file: &std::fs::File) -> std::io::Result<()> {
        #[cfg(target_family = "windows")]
        {
            // Safety: Calling OS functions
            let ret = unsafe {
                let mut overlapped = std::mem::zeroed();
                FileSystem::LockFileEx(
                    file.as_raw_handle(),
                    FileSystem::LOCKFILE_EXCLUSIVE_LOCK | FileSystem::LOCKFILE_FAIL_IMMEDIATELY,
                    0,
                    !0,
                    !0,
                    &mut overlapped,
                )
            };
            if ret == 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        }

        #[cfg(not(target_family = "windows"))]
        {
            // Safety: Calling OS functions
            let ret = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if ret < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // GRID VF-1: a second acquisition of a held repository lock must fail with a bounded,
    // recognizable timeout — never block forever (a mounted workspace holds its lock for the
    // mount's lifetime).
    #[test]
    fn directory_lock_times_out_instead_of_blocking() {
        // Both flock (per open-file-description) and LockFileEx (per handle) contend across
        // two separate opens within one process, so the test needs no second process.
        // Env mutation is process-global; this is the only test using the variable.
        unsafe { std::env::set_var("LORE_LOCK_TIMEOUT_SECS", "1") };

        let dir = std::env::temp_dir().join(format!("lore-lock-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create lock test dir");

        let held = FSLock::acquire_directory_lock(&dir).expect("first acquisition");
        let start = std::time::Instant::now();
        let second = FSLock::acquire_directory_lock(&dir);
        let elapsed = start.elapsed();

        let err = match second {
            Ok(_) => panic!("second acquisition must not succeed while held"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut, "unexpected error: {err}");
        assert!(
            elapsed >= Duration::from_secs(1) && elapsed < Duration::from_secs(8),
            "acquisition should time out promptly, took {elapsed:?}"
        );

        drop(held);
        let reacquired = FSLock::acquire_directory_lock(&dir);
        assert!(reacquired.is_ok(), "lock must be acquirable after release");

        unsafe { std::env::remove_var("LORE_LOCK_TIMEOUT_SECS") };
        drop(reacquired);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

impl Drop for FSLock {
    fn drop(&mut self) {
        #[cfg(target_family = "windows")]
        {
            // Safety: Calling OS functions
            unsafe { FileSystem::UnlockFile(self.file.as_raw_handle(), 0, 0, !0, !0) };
        }

        #[cfg(not(target_family = "windows"))]
        {
            // Safety: Calling OS functions
            unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
        }
    }
}
