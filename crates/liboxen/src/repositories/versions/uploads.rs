//! Uploads of a large version file in parts, each stored once as it arrives and hashed in order, so
//! the file is published under its hash only once its bytes match it.
//!
//! An upload lives in this process's memory from [`Upload::begin`] until the part that completes
//! it, and holds the repository's write gate until its last handle drops, so no exclusive
//! maintenance operation (a storage move, a prune) runs while it is open. A part stays in memory
//! from the moment its body starts arriving until it is hashed, and an upload keeps at most
//! `MAX_UPLOAD_MEMORY` of parts at once, so a part may arrive only as far ahead of the next one the
//! hash needs as that allows. Until the upload completes, a part sent again once stored is accepted
//! and ignored, so a client can resend any part whose response it lost. Once it completes, any part
//! gets [`OxenError::VersionUploadNotFound`], as for an upload that was aborted, so a client that
//! lost the last part's response checks whether the version file exists.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::future::join_all;
use parking_lot::Mutex;
use tempfile::NamedTempFile;
use uuid::Uuid;
use xxhash_rust::xxh3::Xxh3;

use crate::constants::stream_segment_size;
use crate::core::repo_locks::{self, WriteInFlight};
use crate::core::v_latest::workspaces::files;
use crate::error::OxenError;
use crate::model::{LocalRepository, MerkleHash};
use crate::repositories::workspaces;
use crate::storage::{StorageKind, VersionLocation, VersionUpload, s3};
use crate::util;
use crate::view::versions::WorkspaceDestination;

/// The largest file an upload takes, 1 TiB. This is provisional, to be raised when users need
/// larger files.
const MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024 * 1024;

/// The most memory one upload's parts take at once, 1 GiB. It always admits one part, the largest
/// of which is `MAX_FILE_SIZE` split into `s3::MAX_PARTS`.
const MAX_UPLOAD_MEMORY: u64 = 1024 * 1024 * 1024;

/// How long an upload may go with no part arriving or being stored before it is aborted.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// How often [`abort_idle_periodically`] looks for idle uploads.
const IDLE_CHECK_PERIOD: Duration = Duration::from_secs(10);

static UPLOADS: LazyLock<Mutex<HashMap<String, Arc<Upload>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// What storing one part did.
#[derive(Debug, PartialEq, Eq)]
pub enum PartStored {
    /// The upload still waits for other parts.
    Pending,
    /// The part completed the upload: the file is stored under its hash, and staged in the
    /// upload's workspace if it has one.
    Completed,
}

/// A version file being uploaded in parts.
pub struct Upload {
    id: String,
    repo: LocalRepository,
    expected_hash: MerkleHash,
    size: u64,
    part_size: u64,
    num_parts: u64,
    /// How many parts fit in `MAX_UPLOAD_MEMORY`, and so how far ahead of the next part the hash
    /// needs a part may arrive.
    part_window: u64,
    destination: Option<WorkspaceDestination>,
    /// A copy of the file on the data volume to stage from, kept when the upload stages into a
    /// workspace and its storage keeps the file somewhere other than the local disk.
    staging_copy: Option<Arc<NamedTempFile>>,
    storage: Box<dyn VersionUpload>,
    progress: Mutex<Progress>,
    _write: WriteInFlight,
}

struct Progress {
    /// `None` while a run of parts is being hashed.
    hasher: Option<Xxh3>,
    /// The first part not yet in the hash.
    hashed_to: u64,
    /// The first part not yet taken for hashing. Every part before it was received.
    taken_to: u64,
    /// Parts admitted and not yet done storing: their bodies are arriving, or being stored.
    in_flight: HashSet<u64>,
    early: BTreeMap<u64, Bytes>,
    last_active_at: Instant,
    ended: bool,
}

/// A part of an upload admitted for storing. While it lives, the upload is not idle and no other
/// copy of the part is admitted. Dropping it before the part is stored lets the part be sent again.
pub struct ReceivingPart {
    upload: Arc<Upload>,
    part_number: u64,
}

impl Upload {
    /// Begin uploading the version file `hash` of `size` bytes to `repo`, to be staged at
    /// `destination` once it completes.
    pub async fn begin(
        repo: &LocalRepository,
        hash: &str,
        size: u64,
        destination: Option<WorkspaceDestination>,
    ) -> Result<Arc<Upload>, OxenError> {
        Self::begin_with_memory(repo, hash, size, destination, MAX_UPLOAD_MEMORY).await
    }

    /// [`Upload::begin`], keeping at most `memory` bytes of parts in memory at once.
    async fn begin_with_memory(
        repo: &LocalRepository,
        hash: &str,
        size: u64,
        destination: Option<WorkspaceDestination>,
        memory: u64,
    ) -> Result<Arc<Upload>, OxenError> {
        let expected_hash: MerkleHash = hash.parse().map_err(|_| {
            OxenError::VersionUploadRefused(format!("'{hash}' is not a version file hash").into())
        })?;
        if size == 0 {
            return Err(OxenError::VersionUploadRefused(
                "a file of zero bytes has no parts".into(),
            ));
        }
        if size > MAX_FILE_SIZE {
            return Err(OxenError::VersionUploadRefused(
                format!(
                    "a file of {size} bytes is larger than the {MAX_FILE_SIZE} bytes an upload takes"
                )
                .into(),
            ));
        }
        let workspace = match &destination {
            Some(destination) => Some(
                workspaces::get_async(repo, &destination.workspace_id)
                    .await?
                    .ok_or_else(|| {
                        OxenError::WorkspaceNotFound(destination.workspace_id.clone().into())
                    })?,
            ),
            None => None,
        };

        let id = Uuid::new_v4().to_string();
        let write = repo_locks::begin_write(repo)?;
        let store = repo.version_store();
        let floor = match store.storage_kind() {
            StorageKind::Local => stream_segment_size(),
            StorageKind::S3 => stream_segment_size().max(s3::MIN_PART_SIZE as u64),
        };
        let part_size = floor.max(size.div_ceil(s3::MAX_PARTS as u64));
        let stored_locally = matches!(
            store.version_location(&expected_hash.to_string()).await?,
            VersionLocation::Local(_)
        );
        let staging_copy = match workspace {
            Some(workspace) if !stored_locally => {
                let dir = workspace.dir();
                let copy = tokio::task::spawn_blocking(move || {
                    util::fs::create_dir_all(&dir)?;
                    let copy = NamedTempFile::new_in(&dir)?;
                    copy.as_file().set_len(size)?;
                    Ok::<_, OxenError>(copy)
                })
                .await??;
                Some(Arc::new(copy))
            }
            _ => None,
        };
        let storage = store
            .begin_version_upload(expected_hash, size, part_size)
            .await?;
        let upload = Arc::new(Upload {
            id: id.clone(),
            repo: repo.clone(),
            expected_hash,
            size,
            part_size,
            num_parts: size.div_ceil(part_size),
            part_window: (memory / part_size).max(1),
            destination,
            staging_copy,
            storage,
            progress: Mutex::new(Progress {
                hasher: Some(Xxh3::new()),
                hashed_to: 1,
                taken_to: 1,
                in_flight: HashSet::new(),
                early: BTreeMap::new(),
                last_active_at: Instant::now(),
                ended: false,
            }),
            _write: write,
        });
        UPLOADS.lock().insert(id, Arc::clone(&upload));
        Ok(upload)
    }

    /// The open upload `upload_id` to `repo`.
    pub fn find(repo: &LocalRepository, upload_id: &str) -> Result<Arc<Upload>, OxenError> {
        UPLOADS
            .lock()
            .get(upload_id)
            .filter(|upload| upload.repo.path == repo.path)
            .cloned()
            .ok_or_else(|| OxenError::VersionUploadNotFound(upload_id.into()))
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// The size of every part but the last.
    pub fn part_size(&self) -> u64 {
        self.part_size
    }

    pub fn num_parts(&self) -> u64 {
        self.num_parts
    }

    /// How many consecutive parts the upload accepts at once, starting at the first part not yet
    /// hashed. A part past them is deferred until the hash catches up.
    pub fn part_window(&self) -> u64 {
        self.part_window
    }

    /// Admit part `part_number`, counting from 1, for storing, or return `None` when it was
    /// received already. Defer it while it is too far ahead to fit in the upload's memory, while an
    /// earlier copy of it is still being stored, or while the upload completes.
    pub fn receive_part(
        self: &Arc<Self>,
        part_number: u64,
    ) -> Result<Option<ReceivingPart>, OxenError> {
        if part_number == 0 || part_number > self.num_parts {
            return Err(self.refused(format!(
                "part {part_number} is not between 1 and {}",
                self.num_parts
            )));
        }
        let mut progress = self.progress.lock();
        if progress.ended {
            return Err(self.not_found());
        }
        if part_number < progress.taken_to || progress.early.contains_key(&part_number) {
            if progress.taken_to > self.num_parts {
                return Err(self.deferred(part_number, "the upload is completing".into()));
            }
            return Ok(None);
        }
        if progress.in_flight.contains(&part_number) {
            return Err(self.deferred(
                part_number,
                "an earlier copy of it is still being stored".into(),
            ));
        }
        if part_number - progress.hashed_to >= self.part_window {
            return Err(self.deferred(
                part_number,
                format!(
                    "the upload accepts {} parts at once, starting at part {}",
                    self.part_window, progress.hashed_to
                ),
            ));
        }
        progress.in_flight.insert(part_number);
        Ok(Some(ReceivingPart {
            upload: Arc::clone(self),
            part_number,
        }))
    }

    /// Hash `taken`, a run of parts that continues the hash, and every run that continues it in
    /// turn. Returns the digest of the whole file once every part is hashed.
    fn hash_runs(&self, taken: (Xxh3, Vec<Bytes>)) -> Result<Option<MerkleHash>, OxenError> {
        let (mut hasher, mut run) = taken;
        loop {
            for part in &run {
                hasher.update(part);
            }
            let mut progress = self.progress.lock();
            if progress.ended {
                return Err(self.not_found());
            }
            progress.hashed_to = progress.taken_to;
            if progress.hashed_to > self.num_parts {
                progress.ended = true;
                return Ok(Some(MerkleHash::new(hasher.digest128())));
            }
            progress.hasher = Some(hasher);
            match progress.take_run() {
                Some(next) => (hasher, run) = next,
                None => return Ok(None),
            }
        }
    }

    /// Publish the file once its digest is known, and stage it at the upload's destination.
    async fn complete(&self, actual_hash: MerkleHash) -> Result<(), OxenError> {
        UPLOADS.lock().remove(&self.id);
        if actual_hash != self.expected_hash {
            self.abort().await;
            return Err(self.refused(format!(
                "the file's {} bytes hash to {actual_hash}, not {}",
                self.size, self.expected_hash
            )));
        }
        self.storage.complete(actual_hash).await?;
        let Some(destination) = &self.destination else {
            return Ok(());
        };
        let workspace = workspaces::get_async(&self.repo, &destination.workspace_id)
            .await?
            .ok_or_else(|| OxenError::WorkspaceNotFound(destination.workspace_id.clone().into()))?;
        match &self.staging_copy {
            Some(copy) => {
                files::add_version_file_from(
                    &workspace,
                    copy.path(),
                    &destination.path,
                    self.expected_hash,
                )
                .await?
            }
            None => {
                let hash = self.expected_hash.to_string();
                files::add_version_file(&workspace, &destination.path, &hash).await?
            }
        };
        Ok(())
    }

    /// End the upload, dropping its early parts and discarding the parts in storage.
    async fn abort(&self) {
        {
            let mut progress = self.progress.lock();
            progress.ended = true;
            progress.early.clear();
        }
        if let Err(err) = self.storage.abort().await {
            tracing::warn!(upload_id = %self.id, error = %err, "Failed to abort a version upload");
        }
    }

    fn refused(&self, reason: String) -> OxenError {
        OxenError::VersionUploadRefused(format!("{reason} (upload {})", self.id).into())
    }

    fn deferred(&self, part_number: u64, reason: String) -> OxenError {
        OxenError::VersionUploadPartDeferred {
            upload_id: self.id.clone(),
            part_number,
            reason,
        }
    }

    fn not_found(&self) -> OxenError {
        OxenError::VersionUploadNotFound(self.id.as_str().into())
    }
}

impl ReceivingPart {
    /// The part's required length in bytes.
    pub fn size(&self) -> u64 {
        let upload = &self.upload;
        if self.part_number == upload.num_parts {
            upload.size - (self.part_number - 1) * upload.part_size
        } else {
            upload.part_size
        }
    }

    /// Store the part's bytes. The part that completes the upload publishes the file and stages it
    /// in the upload's workspace, or discards the whole upload when its bytes do not hash to the
    /// file's hash. Run it to completion, as in its own task: dropped while it completes the
    /// upload, it leaves parts in storage that nothing discards.
    pub async fn store(self, data: Bytes) -> Result<PartStored, OxenError> {
        let upload = &self.upload;
        let part_number = self.part_number;
        let size = self.size();
        if data.len() as u64 != size {
            return Err(upload.refused(format!(
                "part {part_number} is {} bytes, not {size}",
                data.len()
            )));
        }

        let offset = (part_number - 1) * upload.part_size;
        let copied = async {
            let Some(copy) = upload.staging_copy.clone() else {
                return Ok(());
            };
            let data = data.clone();
            tokio::task::spawn_blocking(move || util::fs::write_at(copy.path(), offset, &data))
                .await?
        };
        tokio::try_join!(
            upload.storage.upload_part(part_number, data.clone()),
            copied
        )?;

        // Hash the part, and any parts that arrived early that it unblocks, off the async worker.
        let taken = {
            let mut progress = upload.progress.lock();
            if progress.ended {
                return Err(upload.not_found());
            }
            progress.early.insert(part_number, data);
            progress.take_run()
        };
        let Some(taken) = taken else {
            return Ok(PartStored::Pending);
        };
        let hashing = Arc::clone(upload);
        match tokio::task::spawn_blocking(move || hashing.hash_runs(taken)).await?? {
            None => Ok(PartStored::Pending),
            Some(actual_hash) => {
                upload.complete(actual_hash).await?;
                Ok(PartStored::Completed)
            }
        }
    }
}

impl Progress {
    /// Take the hasher and the run of early parts that continues the hash, when the hasher is
    /// free and the run is not empty.
    fn take_run(&mut self) -> Option<(Xxh3, Vec<Bytes>)> {
        let hasher = self.hasher.take()?;
        let mut run = Vec::new();
        while let Some(part) = self.early.remove(&self.taken_to) {
            run.push(part);
            self.taken_to += 1;
        }
        if run.is_empty() {
            self.hasher = Some(hasher);
            return None;
        }
        Some((hasher, run))
    }
}

impl Drop for ReceivingPart {
    fn drop(&mut self) {
        let mut progress = self.upload.progress.lock();
        progress.in_flight.remove(&self.part_number);
        progress.last_active_at = Instant::now();
    }
}

/// Abort every upload that has gone `IDLE_TIMEOUT` with no part arriving or being stored, as of
/// `now`.
async fn abort_idle(now: Instant) {
    let idle: Vec<_> = UPLOADS
        .lock()
        .extract_if(|_, upload| {
            let progress = upload.progress.lock();
            progress.in_flight.is_empty()
                && now.saturating_duration_since(progress.last_active_at) >= IDLE_TIMEOUT
        })
        .map(|(_, upload)| upload)
        .collect();
    join_all(idle.iter().map(|upload| {
        tracing::info!(upload_id = %upload.id, "Aborting an idle version upload");
        upload.abort()
    }))
    .await;
}

/// Abort idle uploads every `IDLE_CHECK_PERIOD`, for as long as the process runs.
pub async fn abort_idle_periodically() {
    let mut interval = tokio::time::interval(IDLE_CHECK_PERIOD);
    loop {
        interval.tick().await;
        abort_idle(Instant::now()).await;
    }
}

/// Abort every upload in progress. Call when the server shuts down.
pub async fn abort_all() {
    let uploads: Vec<_> = UPLOADS.lock().drain().map(|(_, upload)| upload).collect();
    join_all(uploads.iter().map(|upload| upload.abort())).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test;
    use crate::util::hasher::hash_buffer;

    async fn send(
        upload: &Arc<Upload>,
        part_number: u64,
        data: Bytes,
    ) -> Result<PartStored, OxenError> {
        match upload.receive_part(part_number)? {
            Some(receiving) => receiving.store(data).await,
            None => Ok(PartStored::Pending),
        }
    }

    #[tokio::test]
    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    async fn test_version_upload_stores_parts_out_of_order() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let not_a_hash = Upload::begin(&repo, "not-a-hash", 10, None).await;
            assert!(matches!(
                not_a_hash,
                Err(OxenError::VersionUploadRefused(_))
            ));
            let empty = Upload::begin(&repo, &hash_buffer(b""), 0, None).await;
            assert!(matches!(empty, Err(OxenError::VersionUploadRefused(_))));
            let oversized = Upload::begin(&repo, &hash_buffer(b"a"), MAX_FILE_SIZE + 1, None).await;
            assert!(matches!(oversized, Err(OxenError::VersionUploadRefused(_))));

            // One part more than the upload's window, so the last part starts out too far
            // ahead of part 1.
            let part_size = stream_segment_size() as usize;
            let window = 4;
            let last = window + 1;
            let data: Vec<u8> = (0..part_size * (last - 1) + 1000)
                .map(|i| (i % 251) as u8)
                .collect();
            let hash = hash_buffer(&data);
            let memory = (window * part_size) as u64;
            let leading_zeros = format!("00{hash}");
            let upload =
                Upload::begin_with_memory(&repo, &leading_zeros, data.len() as u64, None, memory)
                    .await?;
            assert_eq!(upload.part_size() as usize, part_size);
            assert_eq!(upload.num_parts() as usize, last);
            assert_eq!(upload.part_window() as usize, window);
            let part = |n: usize| {
                let start = (n - 1) * part_size;
                Bytes::copy_from_slice(&data[start..(start + part_size).min(data.len())])
            };

            let too_far = send(&upload, last as u64, part(last)).await;
            assert!(
                matches!(too_far, Err(OxenError::VersionUploadPartDeferred { .. })),
                "the last part does not fit in memory beside parts 1 to {window}: {too_far:?}"
            );
            for n in 2..last {
                assert_eq!(send(&upload, n as u64, part(n)).await?, PartStored::Pending);
            }
            assert_eq!(
                send(&upload, 2, part(2)).await?,
                PartStored::Pending,
                "a part sent again after it was stored is accepted"
            );
            let first_copy = upload.receive_part(1)?.expect("part 1 is not received yet");
            let second_copy = send(&upload, 1, part(1)).await;
            assert!(
                matches!(
                    second_copy,
                    Err(OxenError::VersionUploadPartDeferred { .. })
                ),
                "a part sent again while its first copy is stored waits for it: {second_copy:?}"
            );
            drop(first_copy);
            let short = send(&upload, 1, part(1).slice(1..)).await;
            assert!(matches!(short, Err(OxenError::VersionUploadRefused(_))));
            let out_of_range = upload.receive_part(last as u64 + 1).err();
            assert!(matches!(
                out_of_range,
                Some(OxenError::VersionUploadRefused(_))
            ));

            assert_eq!(send(&upload, 1, part(1)).await?, PartStored::Pending);
            assert_eq!(
                send(&upload, last as u64, part(last)).await?,
                PartStored::Completed
            );
            assert_eq!(
                repo.version_store().get_version(&hash).await?,
                data,
                "a hash begun with leading zeros publishes under its canonical spelling"
            );
            let late = Upload::find(&repo, upload.id()).err();
            assert!(
                matches!(late, Some(OxenError::VersionUploadNotFound(_))),
                "a completed upload takes no more parts"
            );

            // An upload whose bytes do not match its hash publishes nothing.
            let wrong_hash = hash_buffer(b"some other file");
            let mismatch = Upload::begin(&repo, &wrong_hash, part_size as u64 + 1, None).await?;
            let exclusive = repo_locks::begin_exclusive_with_timeout(&repo, Duration::ZERO)
                .await
                .err();
            assert!(
                matches!(exclusive, Some(OxenError::LockTimeout(_))),
                "an open upload makes maintenance wait: {exclusive:?}"
            );
            send(&mismatch, 2, Bytes::copy_from_slice(&data[..1])).await?;
            let refused = send(&mismatch, 1, part(1)).await;
            assert!(matches!(refused, Err(OxenError::VersionUploadRefused(_))));
            assert!(!repo.version_store().version_exists(&wrong_hash).await?);
            drop(mismatch);

            // An upload with a part arriving is not idle. Once no part arrives, it is aborted and
            // gives up the repository's write gate.
            let idle = Upload::begin(&repo, &hash, data.len() as u64, None).await?;
            send(&idle, 2, part(2)).await?;
            let later = Instant::now() + IDLE_TIMEOUT + Duration::from_secs(1);
            let receiving = idle.receive_part(1)?.expect("part 1 is not received yet");
            assert_eq!(receiving.size(), part_size as u64);
            abort_idle(later).await;
            assert!(
                Upload::find(&repo, idle.id()).is_ok(),
                "an upload receiving a part is not idle"
            );
            drop(receiving);
            abort_idle(later).await;
            let aborted = Upload::find(&repo, idle.id()).err();
            assert!(matches!(aborted, Some(OxenError::VersionUploadNotFound(_))));
            drop((upload, idle));
            let _exclusive =
                repo_locks::begin_exclusive_with_timeout(&repo, Duration::ZERO).await?;
            Ok(())
        })
        .await
    }
}
