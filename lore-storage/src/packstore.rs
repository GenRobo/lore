// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
#[cfg(target_family = "unix")]
use std::os::unix::fs::FileExt;
#[cfg(target_family = "windows")]
use std::os::windows::fs::FileExt;
#[cfg(target_family = "windows")]
use std::os::windows::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use bytes::BytesMut;
use lore_error_set::prelude::*;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::sync::RwLockWriteGuard;

#[error_set]
pub enum PackfileError {}

#[derive(Debug)]
pub struct PackStoreRef {
    pub id: u32,
    pub offset: u32,
}

struct PackFile {
    id: u32,
    size: usize,
    file: Option<File>,
    buffer: Vec<u8>,
    /// Written to since it was last picked up for an fsync. Set after the write returned.
    dirty: AtomicBool,
}

/// Fsync state of a packstore, shared with the blocking task that runs the fsyncs so the state
/// stays correct when the flush that started them stops waiting.
#[derive(Default)]
struct PackSync {
    /// Held for the whole of an fsync pass. A flush arriving while one runs waits for it and
    /// then only has to cover what was written in the meantime.
    pass: Arc<Mutex<()>>,
    /// Number of writes that reached a packfile on disk.
    written: AtomicU64,
    /// Value of `written` that the last successful pass over all packfiles covered.
    synced: AtomicU64,
    /// Number of directories, counted upwards from the one holding the packfiles, in which a
    /// packfile or a directory was created without the directory having been fsynced since.
    /// Synced along with the next payload: an empty packfile has nothing to lose.
    created: AtomicUsize,
    /// An fsync failed. The kernel reports a write-back error once and may drop the pages it
    /// could not write, so a later fsync succeeding proves nothing about the earlier data:
    /// the store stays unable to promise durability until it is reopened.
    failed: AtomicBool,
}

#[cfg(target_family = "windows")]
struct Retry {
    current: u64,
    maximum: u64,
    counter: usize,
    limit: usize,
}

#[cfg(target_family = "windows")]
impl Retry {
    fn new(start: u64, maximum: u64, limit: usize) -> Self {
        Retry {
            current: start,
            maximum,
            counter: 0,
            limit,
        }
    }

    async fn wait(&mut self) -> bool {
        tokio::time::sleep(std::time::Duration::from_millis(self.current)).await;
        self.current = std::cmp::min(self.current * 2, self.maximum);
        self.counter += 1;
        self.counter < self.limit
    }
}

#[cfg(target_family = "windows")]
async fn packfile_read(file: &File, offset: usize, size: usize) -> Result<Bytes, PackfileError> {
    let mut buffer = BytesMut::with_capacity(size);
    unsafe { buffer.set_len(size) };
    let mut retry = Retry::new(10, 10_000, 100);
    let mut read = 0;
    let mut offset = offset;
    loop {
        match file
            .seek_read(&mut buffer.as_mut()[read..], offset as u64)
            .internal("Failed reading from packstore file")
        {
            Ok(this_read) => {
                if this_read == 0 {
                    break;
                }
                read += this_read;
                offset += this_read;
                if read == size {
                    break;
                }
            }
            Err(err) => {
                if !retry.wait().await {
                    return Err(err.into());
                }
            }
        }
    }
    if read != size {
        return Err(PackfileError::internal(format!(
            "Failed reading from packstore file, read {read} of {size} bytes"
        )));
    }
    Ok(buffer.freeze())
}

#[cfg(target_family = "windows")]
async fn packfile_write(
    file: &mut File,
    buffer: Bytes,
    offset: usize,
) -> Result<(), PackfileError> {
    // On Windows the seek_read changes the file pointer so we must
    // use seek_write to write to the end
    let mut retry = Retry::new(10, 10_000, 100);
    let mut wrote = 0;
    let mut offset = offset;
    let size = buffer.len();
    loop {
        match file
            .seek_write(&buffer.as_ref()[wrote..], offset as u64)
            .internal("Failed writing to packstore file")
        {
            Ok(this_write) => {
                if this_write == 0 {
                    break;
                }
                wrote += this_write;
                offset += this_write;
                if wrote == size {
                    break;
                }
            }
            Err(err) => {
                if !retry.wait().await {
                    return Err(err.into());
                }
            }
        }
    }
    if wrote != size {
        return Err(PackfileError::internal(format!(
            "Failed writing to packstore file, wrote {wrote} of {size} bytes"
        )));
    }
    Ok(())
}

#[allow(clippy::unused_async)]
#[cfg(target_family = "unix")]
async fn packfile_read(file: &File, offset: usize, size: usize) -> Result<Bytes, PackfileError> {
    let mut buffer = BytesMut::with_capacity(size);
    // Safety: Ok to leave uninitialized, read_exact_at will either initialize all data with the read operation, or fail
    unsafe { buffer.set_len(size) };
    file.read_exact_at(buffer.as_mut(), offset as u64)
        .internal("Failed reading from packstore file")?;
    Ok(buffer.freeze())
}

#[allow(clippy::unused_async)]
#[cfg(target_family = "unix")]
async fn packfile_write(
    file: &mut File,
    buffer: Bytes,
    offset: usize,
) -> Result<(), PackfileError> {
    // On Unix based system the read_exact_at does not change file position so the file position
    // should always be at end of file - but offset is required to replace data (e.g. for obliteration)
    Ok(file
        .write_all_at(buffer.as_ref(), offset as u64)
        .internal("Failed writing to packstore file")?)
}

/// Maximum size of a single packfile
const PACKSTORE_SIZE_LIMIT: u64 = 3 * 1024 * 1024 * 1024;

pub struct PackStore {
    path: Option<PathBuf>,
    min_count: usize,
    packfile: RwLock<Vec<RwLock<PackFile>>>,
    writeable: RwLock<Vec<u32>>,
    /// Shared per-store GC counters; `resume()` feeds loaded packfile sizes into them
    /// so an over-cap store fires compaction without a startup scan. `None` for stores
    /// that don't participate in automatic GC (e.g. migration/scratch packstores).
    gc_counters: Option<Arc<crate::maintenance::GcCounters>>,
    sync: Arc<PackSync>,
}

impl PackStore {
    pub fn new(
        path: Option<PathBuf>,
        min_count: usize,
        gc_counters: Option<Arc<crate::maintenance::GcCounters>>,
    ) -> Self {
        PackStore {
            path: path.map(|path| {
                let mut path = path;
                path.push("pack");
                path
            }),
            min_count,
            packfile: RwLock::default(),
            writeable: RwLock::default(),
            gc_counters,
            sync: Arc::default(),
        }
    }

    pub async fn resume(&self) -> Result<(), PackfileError> {
        let mut packfile = self.packfile.write().await;
        let mut writeable = self.writeable.write().await;

        if !writeable.is_empty() {
            return Ok(());
        }

        let mut loaded_size: u64 = 0;

        if let Some(path) = self.path.as_ref() {
            let path = path.clone();
            if !path.exists() {
                // The missing directories plus the one the topmost of them is created in
                let created = path
                    .ancestors()
                    .take_while(|directory| {
                        !directory.as_os_str().is_empty() && !directory.exists()
                    })
                    .count()
                    + 1;
                std::fs::DirBuilder::new()
                    .recursive(true)
                    .create(&path)
                    .internal(&format!(
                        "Failed to create packstore directory {}",
                        path.display()
                    ))?;
                self.sync.created.fetch_max(created, Ordering::AcqRel);
            }

            let paths = std::fs::read_dir(&path).internal(&format!(
                "Failed to read packstore directory {}",
                path.display()
            ))?;
            let mut packfile_count = 0;
            for entry in paths {
                let Ok(entry) = entry else {
                    continue;
                };
                let Ok(file_meta) = std::fs::metadata(entry.path()) else {
                    continue;
                };
                if !file_meta.is_file() {
                    continue;
                }

                let file_name = entry.file_name();
                let Some(file_id) = file_name.to_str() else {
                    continue;
                };
                let Ok(file_id) = file_id.parse::<u32>() else {
                    continue;
                };
                if file_id == 0 {
                    continue;
                }

                if file_id > packfile_count {
                    packfile_count = file_id;
                }
            }

            for index in 0..packfile_count {
                let file_id = index + 1;
                let file_path = path.join(file_id.to_string());
                let mut file_options = OpenOptions::new();
                file_options.read(true).write(true);
                #[cfg(target_family = "windows")]
                {
                    // Prevent any other process from writing the file
                    file_options
                        .share_mode(windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ);
                }
                if let Ok(_meta) = std::fs::metadata(file_path.as_path()) {
                    lore_base::lore_trace!("Resuming packfile {file_id}");
                    file_options.create(false);
                } else {
                    lore_base::lore_trace!("Create packfile {file_id}");
                    file_options.create(true).truncate(true);
                    self.sync.created.fetch_max(1, Ordering::AcqRel);
                }
                let mut file = file_options.open(file_path.as_path()).internal(&format!(
                    "Failed opening packstore file {}",
                    file_path.display()
                ))?;
                let file_size = file.seek(SeekFrom::End(0)).internal(&format!(
                    "Failed seeking to packstore file end {}",
                    file_path.display()
                ))?;

                let file = PackFile {
                    id: file_id,
                    size: file_size as usize,
                    file: Some(file),
                    buffer: vec![],
                    dirty: AtomicBool::new(false),
                };

                packfile.push(RwLock::new(file));
                loaded_size += file_size;

                if file_size < PACKSTORE_SIZE_LIMIT {
                    lore_base::lore_trace!("Packfile {file_id} is writeable");
                    writeable.push(file_id);
                }
            }
        }

        lore_base::lore_trace!("{} writable packfiles", writeable.len());

        let _ = self.fill_writeable(packfile, writeable);

        if let Some(gc) = &self.gc_counters {
            gc.add_loaded_size(loaded_size);
        }

        Ok(())
    }

    async fn mark_full(&self, id: u32) {
        let packfile = self.packfile.write().await;
        let mut writeable = self.writeable.write().await;
        for (index, writeable_id) in writeable.iter().enumerate() {
            if *writeable_id == id {
                writeable.swap_remove(index);
                break;
            }
        }

        let _ = self.fill_writeable(packfile, writeable);
    }

    fn fill_writeable<'a>(
        &'a self,
        mut packfile: RwLockWriteGuard<'a, Vec<RwLock<PackFile>>>,
        mut writeable: RwLockWriteGuard<'a, Vec<u32>>,
    ) -> Result<(), PackfileError> {
        while writeable.len() < self.min_count {
            let index = packfile.len();
            let id = (index + 1) as u32;
            lore_base::lore_trace!("Create additional packfile {id}");

            let file = if let Some(path) = self.path.as_ref() {
                let file_path = path.join(id.to_string());
                let mut file_options = OpenOptions::new();
                file_options
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true);
                #[cfg(target_family = "windows")]
                {
                    // Prevent any other process from writing the file
                    file_options
                        .share_mode(windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ);
                }
                let file = file_options.open(file_path.as_path()).internal(&format!(
                    "Failed opening packstore file {}",
                    file_path.display()
                ))?;
                self.sync.created.fetch_max(1, Ordering::AcqRel);

                PackFile {
                    id,
                    size: 0,
                    file: Some(file),
                    buffer: vec![],
                    dirty: AtomicBool::new(false),
                }
            } else {
                PackFile {
                    id,
                    size: 0,
                    file: None,
                    buffer: vec![],
                    dirty: AtomicBool::new(false),
                }
            };

            packfile.push(RwLock::new(file));
            writeable.push(id);
        }

        Ok(())
    }

    pub async fn stop_write(&self, id: u32) -> Result<usize, PackfileError> {
        if self.path.is_none() {
            return Err(PackfileError::internal("No more packfiles"));
        }

        if id == 0 {
            return Err(PackfileError::internal("Invalid packfile"));
        }

        let index = (id as usize) - 1;
        let current_size = {
            let mut packfiles = self.packfile.read().await;
            if packfiles.is_empty() {
                drop(packfiles);
                self.resume().await?;
                packfiles = self.packfile.read().await;
            }
            if index >= packfiles.len() {
                return Err(PackfileError::internal("No more packfiles"));
            }
            packfiles[index].read().await.size
        };

        self.mark_full(id).await;

        Ok(current_size)
    }

    pub async fn truncate(&self, id: u32) -> Result<(), PackfileError> {
        if self.path.is_none() {
            return Err(PackfileError::internal("No more packfiles"));
        }

        if id == 0 {
            return Err(PackfileError::internal("Invalid packfile"));
        }

        // Ensure packfile is not marked as writeable
        {
            let writeable = self.writeable.read().await;
            if writeable.contains(&id) {
                lore_base::lore_warn!("Tried truncating a writeable packfile {id}");
                return Err(PackfileError::internal(
                    "Cannot truncate a writeable packfile",
                ));
            }
        }

        {
            let mut packfile = self.packfile.write().await;

            let index = (id - 1) as usize;
            let packfile_count = packfile.len();
            if index >= packfile_count {
                return Err(PackfileError::internal("Invalid packfile"));
            }

            // Retire this packfile, caller guarantees nothing refers to it anymore so truncate it to zero
            if id as usize == packfile_count && packfile_count > self.min_count {
                // We can discard this packfile, it is the last packfile and we have enough remaining
                // packfiles without it to satisfy the min count requested
                drop(packfile.pop());

                if let Some(path) = self.path.as_ref() {
                    let file_path = path.join(id.to_string());
                    lore_base::lore_debug!(
                        "Discard compacted and truncated packfile {}",
                        file_path.display()
                    );
                    let _ = tokio::fs::remove_file(file_path.as_path()).await;
                }

                return Ok(());
            }

            let mut packfile = packfile[index].write().await;

            packfile.dirty.store(false, Ordering::Release);
            if let Some(file) = packfile.file.as_ref() {
                let _ = file.set_len(0);
                let _ = file.sync_all();
            }
            packfile.buffer.clear();
            packfile.size = 0;
        }

        lore_base::lore_debug!("Truncated compacted packfile {id} to zero");

        // Mark the truncated packfile as writeable again
        {
            let mut writeable = self.writeable.write().await;
            if !writeable.contains(&id) {
                writeable.push(id);
            }
        }

        Ok(())
    }

    pub async fn load(&self, id: u32, offset: u32, size: u32) -> Result<Bytes, PackfileError> {
        if size == 0 {
            return Ok(Bytes::default());
        }
        if id == 0 {
            return Err(PackfileError::internal("Invalid packfile"));
        }

        let index = (id - 1) as usize;
        let size = size as usize;
        let offset = offset as usize;

        let mut packfiles = self.packfile.read().await;
        if packfiles.is_empty() {
            drop(packfiles);
            self.resume().await?;
            packfiles = self.packfile.read().await;
        }

        if index >= packfiles.len() {
            return Err(PackfileError::internal("Invalid packfile"));
        }

        let packfile = packfiles[index].read().await;
        if packfile.id != id {
            return Err(PackfileError::internal("Packfile ID mismatch index"));
        }

        // We got lock on the right packfile, load data
        if let Some(file) = packfile.file.as_ref() {
            return packfile_read(file, offset, size).await;
        }

        if (offset + size) > packfile.buffer.len() {
            return Err(PackfileError::internal(
                "Failed reading from packstore buffer, boundary violation",
            ));
        }

        Ok(Bytes::copy_from_slice(
            &packfile.buffer[offset..(offset + size)],
        ))
    }

    pub async fn store(&self, buffer: Bytes) -> Result<PackStoreRef, PackfileError> {
        let size = buffer.len();
        if size == 0 {
            return Err(PackfileError::internal(
                "Failed to store invalid zero sized buffer",
            ));
        }

        // Note that it is fine if multiple store operations end up on the same writeable packfiles
        // and both trigger the full-and-remove from writeable in parallel
        let mut packfiles = self.packfile.read().await;
        if packfiles.is_empty() {
            drop(packfiles);
            self.resume().await?;
            packfiles = self.packfile.read().await;
        }

        let mut packfile = {
            let writeable = self.writeable.read().await;

            if writeable.is_empty() {
                return Err(PackfileError::internal("No more packfiles"));
            }

            let mut writeable_index = (size + buffer.as_ptr() as usize) % writeable.len();
            let mut retry = 0;

            let mut packfile_id = writeable[writeable_index];
            // Writeable IDs are guaranteed to be valid since packfile list never shrinks
            let mut packfile_index = (packfile_id - 1) as usize;
            let mut try_lock = packfiles[packfile_index].try_write();
            while try_lock.is_err() {
                retry += 1;
                writeable_index = (writeable_index + 1) % writeable.len();
                if retry >= writeable.len() {
                    break;
                }
                packfile_id = writeable[writeable_index];
                packfile_index = (packfile_id - 1) as usize;
                try_lock = packfiles[packfile_index].try_write();
            }

            drop(writeable);

            match try_lock {
                Ok(guard) => guard,
                Err(_) => packfiles[packfile_index].write().await,
            }
        };

        let offset = packfile.size;
        let id = packfile.id;
        let mut full = false;

        if let Some(file) = packfile.file.as_mut() {
            packfile_write(file, buffer.clone(), offset).await?;
            packfile.size += size;
            // Marked once the bytes are in the file: an fsync that picks the mark up covers them
            packfile.dirty.store(true, Ordering::Release);
            self.sync.written.fetch_add(1, Ordering::AcqRel);
            if packfile.size >= PACKSTORE_SIZE_LIMIT as usize {
                full = true;
            }
        } else {
            if (packfile.buffer.len() + size) > packfile.buffer.capacity() {
                packfile.buffer.reserve(4 * 1024 * 1024);
            }

            packfile.buffer.extend_from_slice(&buffer[..size]);
            packfile.size += size;
        }

        drop(packfile);
        drop(packfiles);

        if full {
            self.mark_full(id).await;
        }

        Ok(PackStoreRef {
            id,
            offset: offset as u32,
        })
    }

    pub async fn obliterate(&self, id: u32, offset: u32, size: u32) -> Result<(), PackfileError> {
        let offset = offset as usize;
        let size = size as usize;
        let zeros = BytesMut::zeroed(size).freeze();

        let mut packfiles = self.packfile.read().await;
        if packfiles.is_empty() {
            drop(packfiles);
            self.resume().await?;
            packfiles = self.packfile.read().await;
        }

        if id == 0 || id as usize > packfiles.len() {
            return Err(PackfileError::internal("Invalid packfile"));
        }

        let index = (id - 1) as usize;
        let mut packfile = packfiles[index].write().await;
        if packfile.id != id {
            return Err(PackfileError::internal("Packfile ID mismatch index"));
        }

        if let Some(file) = packfile.file.as_mut() {
            packfile_write(file, zeros, offset).await?;
            packfile.dirty.store(true, Ordering::Release);
            self.sync.written.fetch_add(1, Ordering::AcqRel);
        }
        drop(packfile);
        drop(packfiles);

        // An obliteration is only done once the zeros are on the storage media
        self.flush(id, true).await
    }

    /// Whether payload was written that no successful `flush_all(true)` has covered yet.
    /// Lock-free.
    pub fn needs_sync(&self) -> bool {
        self.sync.written.load(Ordering::Acquire) != self.sync.synced.load(Ordering::Acquire)
            || self.sync.failed.load(Ordering::Acquire)
    }

    fn sync_failed() -> PackfileError {
        PackfileError::internal(
            "A packstore file failed to sync earlier, what reached the storage media is unknown until the store is reopened",
        )
    }

    /// Fsync the given packfiles, then the directories if a packfile was created since they
    /// were last synced. Runs on a blocking thread that owns `pass`, so the pass is only over
    /// when the last fsync returned even if the caller stopped waiting, and a failure is
    /// recorded even if nobody is left to receive it.
    async fn sync_files(
        &self,
        pass: tokio::sync::OwnedMutexGuard<()>,
        files: Vec<File>,
        covers: Option<u64>,
    ) -> Result<(), PackfileError> {
        let sync = self.sync.clone();
        let path = self.path.clone();
        lore_base::lore_spawn_blocking!(move || {
            let _pass = pass;
            let mut result = files.iter().try_for_each(File::sync_data);
            if result.is_ok()
                && let Some(path) = path.as_deref()
            {
                let created = sync.created.swap(0, Ordering::AcqRel);
                result = path
                    .ancestors()
                    .take(created)
                    .filter(|directory| !directory.as_os_str().is_empty())
                    .try_for_each(crate::fs_util::sync_dir);
            }
            match result {
                Ok(()) => {
                    if let Some(covers) = covers {
                        sync.synced.fetch_max(covers, Ordering::AcqRel);
                    }
                }
                Err(_) => sync.failed.store(true, Ordering::Release),
            }
            result
        })
        .await
        .internal("Packstore sync task failed")?
        .internal("Failed syncing packstore file")?;
        Ok(())
    }

    /// Duplicate the handles of the packfiles written to since they were last picked up for an
    /// fsync (only packfile `only` when given), clearing their marks. Call with `pass` held.
    async fn take_dirty(&self, only: Option<u32>) -> Result<Vec<File>, PackfileError> {
        let packfiles = self.packfile.read().await;
        let mut files = Vec::new();
        for packfile in packfiles.iter() {
            let packfile = packfile.read().await;
            if only.is_some_and(|id| id != packfile.id) {
                continue;
            }
            let Some(file) = &packfile.file else {
                continue;
            };
            if !packfile.dirty.swap(false, Ordering::AcqRel) {
                continue;
            }
            match file.try_clone() {
                Ok(file) => files.push(file),
                // Sync in this thread if the handle cannot be duplicated
                Err(_) => {
                    if let Err(err) = file.sync_data() {
                        self.sync.failed.store(true, Ordering::Release);
                        return Err(PackfileError::internal_with_context(
                            err,
                            "Failed syncing packstore file",
                        ));
                    }
                }
            }
        }
        Ok(files)
    }

    /// Flush packfile `id`. With `sync_data`, everything written to it so far has reached the
    /// storage media when this returns `Ok`. Without, there is nothing to do: writes go
    /// straight to the file.
    pub async fn flush(&self, id: u32, sync_data: bool) -> Result<(), PackfileError> {
        {
            let packfile = self.packfile.read().await;

            if id == 0 || id as usize > packfile.len() {
                return Err(PackfileError::internal("Invalid packfile"));
            }

            let index = (id - 1) as usize;
            let packfile = packfile[index].read().await;
            if packfile.id != id {
                return Err(PackfileError::internal("Packfile ID mismatch index"));
            }
        }

        if !sync_data {
            return Ok(());
        }

        let pass = self.sync.pass.clone().lock_owned().await;
        if self.sync.failed.load(Ordering::Acquire) {
            return Err(Self::sync_failed());
        }
        let files = self.take_dirty(Some(id)).await?;
        if files.is_empty() {
            return Ok(());
        }
        self.sync_files(pass, files, None).await
    }

    /// Flush all packfiles. With `sync_data`, every write that returned before this was called
    /// has reached the storage media when this returns `Ok`; a flush that finds another one
    /// running waits for it rather than trusting it. Without, there is nothing to do: writes
    /// go straight to the files.
    pub async fn flush_all(&self, sync_data: bool) -> Result<(), PackfileError> {
        if !sync_data {
            return Ok(());
        }

        let pass = self.sync.pass.clone().lock_owned().await;
        if self.sync.failed.load(Ordering::Acquire) {
            return Err(Self::sync_failed());
        }
        let covers = self.sync.written.load(Ordering::Acquire);
        if self.sync.synced.load(Ordering::Acquire) >= covers {
            return Ok(());
        }
        let files = self.take_dirty(None).await?;
        self.sync_files(pass, files, Some(covers)).await
    }

    pub async fn total_size(&self) -> Result<usize, PackfileError> {
        let mut size = 0;
        let mut packfiles = self.packfile.read().await;
        if packfiles.is_empty() {
            drop(packfiles);
            self.resume().await?;
            packfiles = self.packfile.read().await;
        }
        for packfile in packfiles.iter() {
            size += packfile.read().await.size;
        }
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn only_a_syncing_flush_settles_what_was_written() {
        let dir = crate::test_util::TempDir::new("packstore_sync_");
        let store = PackStore::new(Some(dir.to_path_buf()), 1, None);

        let packref = store.store(Bytes::from_static(b"payload")).await.unwrap();
        assert!(store.needs_sync());

        // A flush that does not sync leaves the write owed to the storage media, both for
        // one packfile and for all of them
        store.flush(packref.id, false).await.unwrap();
        store.flush_all(false).await.unwrap();
        assert!(store.needs_sync());

        store.flush_all(true).await.unwrap();
        assert!(!store.needs_sync());

        // And a write after it is owed again
        store.store(Bytes::from_static(b"more")).await.unwrap();
        assert!(store.needs_sync());
        store.flush_all(true).await.unwrap();
        assert!(!store.needs_sync());
    }

    #[tokio::test]
    async fn concurrent_syncing_flushes_all_return_settled() {
        let dir = crate::test_util::TempDir::new("packstore_concurrent_");
        let store = Arc::new(PackStore::new(Some(dir.to_path_buf()), 1, None));
        store.store(Bytes::from_static(b"payload")).await.unwrap();

        let mut flushes = tokio::task::JoinSet::new();
        for _ in 0..8 {
            let store = store.clone();
            flushes.spawn(async move {
                store.flush_all(true).await.unwrap();
                // Whichever flush did the fsync, none returns before it is done
                assert!(!store.needs_sync());
            });
        }
        while let Some(flush) = flushes.join_next().await {
            flush.unwrap();
        }
    }

    #[tokio::test]
    async fn a_failed_sync_is_not_forgotten() {
        let dir = crate::test_util::TempDir::new("packstore_failed_");
        let store = PackStore::new(Some(dir.to_path_buf()), 1, None);
        store.store(Bytes::from_static(b"payload")).await.unwrap();

        // What an fsync error leaves behind: the kernel may have dropped the pages
        store.sync.failed.store(true, Ordering::Release);

        assert!(store.needs_sync());
        assert!(store.flush_all(true).await.is_err());
        assert!(store.flush_all(true).await.is_err());
        assert!(store.flush(1, true).await.is_err());
    }

    #[tokio::test]
    async fn memory_packstore_has_nothing_to_sync() {
        let store = PackStore::new(None, 1, None);
        store.store(Bytes::from_static(b"payload")).await.unwrap();
        assert!(!store.needs_sync());
        store.flush_all(true).await.unwrap();
    }
}
