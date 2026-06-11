//! Atomic publish via the write-temp-then-rename pattern.
//!
//! [`AtomicFile`] is the public entry point — see its rustdoc for the contract. Module-private
//! [`AtomicTempFile`] is the underlying resource and is intentionally not re-exported: the
//! "you must call `commit` to publish, or the write is silently discarded" invariant only has
//! to be honored inside this module.

use bytes::{Bytes, BytesMut};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tokio::io::AsyncReadExt;
use xxhash_rust::xxh3::{Xxh3, xxh3_128};

use crate::constants;
use crate::error::OxenError;
use crate::model::MerkleHash;
use crate::util::hasher::HashingReader;

/// Infix used in `AtomicTempFile` scratch file names: `<target_basename>.oxentmp.<random>`. Private
/// to keep all knowledge of the pattern in this module — when fsck (or any other sweeper) needs to
/// recognize these temp files, the right move is to add a helper here that does the matching,
/// rather than re-export this constant.
const ATOMIC_TEMP_INFIX: &str = ".oxentmp.";

/// A temp file opened in `target`'s parent directory, ready to be written into and then atomically
/// renamed over `target` via `commit`.
///
/// **Sync** per `docs/async_policy.md`. Async callers run this inside a `tokio::task::spawn_blocking`
/// closure (directly, or via the **Channel hand-off** pattern for streaming sources — see
/// [`AtomicFile::stream_async`]).
///
/// Private on purpose: the type carries a "you must call `commit` to publish, or the write is
/// silently discarded" invariant. External callers use [`AtomicFile`] and its terminal methods
/// ([`write`][AtomicFile::write] / [`stream`][AtomicFile::stream] /
/// [`stream_async`][AtomicFile::stream_async] / [`copy_from`][AtomicFile::copy_from]), which
/// take care of the commit step.
///
/// The scratch filename is `<target_basename>.oxentmp.<random>` so the `.oxentmp.` infix is a
/// deterministic shape fsck can match on (via a future helper here) for orphans left behind by
/// hard-kills, even when atomic writes start landing under the working tree where third-party files
/// live.
///
/// Drop semantics: if the handle is dropped without `commit` (panic, error return),
/// `tempfile::NamedTempFile`'s Drop unlinks the scratch file. After a successful `commit`, the temp
/// path has been renamed away, so the handle is consumed and there is nothing to unlink.
struct AtomicTempFile {
    tmp: tempfile::NamedTempFile,
    target: PathBuf,
}

impl AtomicTempFile {
    /// Open a new temp file as a sibling of `target`. Creates `target`'s parent directory if
    /// needed. The temp lives in that parent so the eventual rename stays on one filesystem (POSIX
    /// `rename` is only atomic when src and dst share a filesystem). Scratch filename:
    /// `<target_basename>.oxentmp.<random>`.
    fn create(target: &Path) -> Result<Self, OxenError> {
        // Validate the target shape before any on-disk side effects, so a malformed input (e.g.
        // a trailing-slash path with no filename component) doesn't leave behind a freshly-created
        // parent directory.
        let target_name = target.file_name().ok_or_else(|| {
            OxenError::file_create_error(
                target,
                std::io::Error::other("target path has no filename component"),
            )
        })?;

        let parent = target.parent().filter(|p| !p.as_os_str().is_empty());
        if let Some(parent) = parent {
            std::fs::create_dir_all(parent)
                .map_err(|err| OxenError::file_create_error(parent, err))?;
        }
        let temp_dir = parent.unwrap_or_else(|| Path::new("."));

        // `tempfile::Builder` produces `<prefix><random><suffix>`, so emitting the `.oxentmp.`
        // infix as part of the prefix yields `<target>.oxentmp.<random>`. The random part is
        // short alphanumeric (~6 chars), unique per directory.
        let temp_prefix = format!("{}{}", target_name.to_string_lossy(), ATOMIC_TEMP_INFIX);
        let tmp = tempfile::Builder::new()
            .prefix(&temp_prefix)
            .suffix("")
            .tempfile_in(temp_dir)
            .map_err(|err| {
                // `tempfile::Builder` generates the random suffix internally, so we don't
                // know the exact path it tried to create. Report the pattern instead, with
                // `<random>` standing in for the unknown alphanumeric tail — directionally
                // honest about what failed (the tempfile, not `temp_dir` itself).
                OxenError::file_create_error(temp_dir.join(format!("{temp_prefix}<random>")), err)
            })?;

        Ok(Self {
            tmp,
            target: target.to_path_buf(),
        })
    }

    /// Mutable reference to the underlying sync writer. Use with `write_all`, or pass to
    /// `std::io::copy(&mut reader, tmp.as_writer())` for streamed payloads. The reference derefs
    /// to `std::fs::File`, so `.sync_all()` and other `File` methods are also available when
    /// needed by the caller.
    fn as_writer(&mut self) -> &mut tempfile::NamedTempFile {
        &mut self.tmp
    }

    /// Record the mtime the published file should carry. Call this before [`commit`] so the
    /// stamp is part of the atomic publish — after `commit`, mtime stamping on the canonical
    /// path is no longer crash-safe. Leaves atime untouched (passing `None` for the atime
    /// argument) so `noatime`-mounted filesystems don't surface a spurious `EPERM`.
    fn set_mtime(&mut self, mtime: SystemTime) -> Result<(), OxenError> {
        let ft = filetime::FileTime::from_system_time(mtime);
        filetime::set_file_handle_times(self.tmp.as_file(), None, Some(ft))?;
        Ok(())
    }

    /// fsync the data, rename the temp file over `target`, then best-effort fsync the parent
    /// directory so the rename itself survives a crash. The parent fsync may fail on platforms
    /// that don't support fsync-on-directory (notably Windows), which is fine.
    fn commit(self) -> Result<(), OxenError> {
        let Self { tmp, target } = self;

        tmp.as_file().sync_all()?;

        // Route around `tempfile::NamedTempFile::persist`, which calls `MoveFileExW` directly on
        // Windows and hits transient `ERROR_ACCESS_DENIED` / `ERROR_SHARING_VIOLATION` failures
        // when another process or thread momentarily holds the target (antivirus, search
        // indexers, concurrent writers racing for the same path). `keep()` consumes the handle
        // and disables `NamedTempFile`'s auto-deletion, handing back the path; we then call
        // `std::fs::rename`, which since Rust 1.85 (rust-lang/rust#131072) uses
        // `SetFileInformationByHandle(FileRenameInfoEx)` with POSIX semantics on Windows 10
        // 1607+ — atomic and immune to that contention. On Linux it's just `rename(2)`.
        let (_file, temp_path) = tmp
            .keep()
            .map_err(|err| OxenError::file_rename_error(err.file.path(), &target, err.error))?;
        if let Err(err) = std::fs::rename(&temp_path, &target) {
            // Auto-cleanup is disabled now, so we unlink the orphaned temp ourselves.
            let _ = std::fs::remove_file(&temp_path);
            return Err(OxenError::file_rename_error(&temp_path, &target, err));
        }

        // Best-effort fsync the parent directory. It's okay if it fails. This operation isn't
        // critical, and we know it isn't supported on Windows at all.
        if let Some(parent) = target.parent().filter(|p| !p.as_os_str().is_empty()) {
            match std::fs::File::open(parent) {
                Ok(dir) => {
                    if let Err(err) = dir.sync_all() {
                        log::debug!(
                            "AtomicTempFile::commit: parent fsync failed for {parent:?}: {err}"
                        );
                    }
                }
                Err(err) => log::debug!(
                    "AtomicTempFile::commit: could not open parent {parent:?} for fsync: {err}"
                ),
            }
        }

        Ok(())
    }
}

/// Atomic publish to a target path via the write-temp-then-rename pattern. A crash at any point
/// leaves either the prior contents at `target` (if any) or the new contents, never a torn file.
///
/// Build with [`new`][Self::new] and chain `with_*` options; finish with a terminal method —
/// [`write`][Self::write] for in-memory bytes, [`stream`][Self::stream] for sync `Read`,
/// [`stream_async`][Self::stream_async] for `AsyncRead`, or [`copy_from`][Self::copy_from] for a
/// source file.
///
/// - `with_hash(h)`: the terminal method hashes the published bytes and refuses to publish if the
///   XXH3-128 digest doesn't match — `target` is unchanged on mismatch and
///   [`OxenError::HashMismatch`] is returned. [`write`][Self::write] hashes the in-memory bytes
///   before any filesystem side effect; the streaming and copy variants hash in-passing and drop
///   the temp file on mismatch.
/// - `with_mtime(t)`: the terminal method stamps the published file with `t` atomically with the
///   bytes, so a crash leaves either the prior contents or the new (correctly-stamped) contents
///   at `target`, never a wrong-mtime file.
///
/// Sync on purpose where possible: per `docs/async_policy.md`, most leaf filesystem utilities use
/// `std::fs` and run inside a `tokio::task::spawn_blocking` provided by the calling operation.
/// [`stream_async`][Self::stream_async] is the only `async` terminal, and uses the Channel
/// hand-off pattern to bridge an async reader to a sync writer.
#[must_use = "AtomicFile does nothing until a terminal method (write/stream/stream_async/copy_from) is called"]
#[derive(Debug, Clone)]
pub struct AtomicFile {
    target: PathBuf,
    expected_hash: Option<MerkleHash>,
    mtime: Option<SystemTime>,
}

impl AtomicFile {
    /// Start building an atomic publish targeting `target`. No filesystem side effects yet — the
    /// temp file is materialized inside the terminal method.
    pub fn new(target: impl AsRef<Path>) -> Self {
        Self {
            target: target.as_ref().to_path_buf(),
            expected_hash: None,
            mtime: None,
        }
    }

    /// Hash the published bytes and refuse to publish on mismatch.
    pub fn with_hash(mut self, expected_hash: MerkleHash) -> Self {
        self.expected_hash = Some(expected_hash);
        self
    }

    /// Stamp the published file with `mtime` atomically with the bytes.
    pub fn with_mtime(mut self, mtime: SystemTime) -> Self {
        self.mtime = Some(mtime);
        self
    }

    /// Publish in-memory `contents` to the target path. Use for small payloads (HEAD, TOML
    /// config, refs files). For streaming sources use [`stream`][Self::stream] /
    /// [`stream_async`][Self::stream_async]; for a file source use [`copy_from`][Self::copy_from].
    pub fn write(self, contents: &[u8]) -> Result<(), OxenError> {
        // Verify-before-publish: hash in-memory bytes before any filesystem side effect.
        if let Some(expected) = self.expected_hash {
            let actual = MerkleHash::new(xxh3_128(contents));
            if actual != expected {
                return Err(OxenError::HashMismatch {
                    path: self.target,
                    expected,
                    actual,
                });
            }
        }
        let mut tmp = AtomicTempFile::create(&self.target)?;
        tmp.as_writer().write_all(contents)?;
        if let Some(mtime) = self.mtime {
            tmp.set_mtime(mtime)?;
        }
        tmp.commit()
    }

    /// Publish everything yielded by sync `reader` to the target path. Driven by `std::io` so
    /// it's callable from sync code paths that can't host a tokio runtime.
    pub fn stream<R>(self, reader: &mut R) -> Result<(), OxenError>
    where
        R: std::io::Read + ?Sized,
    {
        let mut tmp = AtomicTempFile::create(&self.target)?;
        let actual_hash = {
            let mut buf_writer =
                std::io::BufWriter::with_capacity(constants::STREAMING_BUF_SIZE, tmp.as_writer());
            let actual = if self.expected_hash.is_some() {
                let mut hashing = HashingReader::new(reader);
                std::io::copy(&mut hashing, &mut buf_writer)?;
                Some(MerkleHash::new(hashing.digest128()))
            } else {
                std::io::copy(reader, &mut buf_writer)?;
                None
            };
            // `std::io::BufWriter::Drop` attempts to flush but silently swallows errors. Flush
            // explicitly so any IO error propagates before the hash check / mtime stamp / rename.
            buf_writer.flush()?;
            actual
        };
        if let (Some(expected), Some(actual)) = (self.expected_hash, actual_hash)
            && actual != expected
        {
            return Err(OxenError::HashMismatch {
                path: self.target,
                expected,
                actual,
            });
        }
        if let Some(mtime) = self.mtime {
            tmp.set_mtime(mtime)?;
        }
        tmp.commit()
    }

    /// Publish everything yielded by async `reader` to the target path.
    ///
    /// Uses the **Channel hand-off** pattern from `docs/async_policy.md`: the async reader runs on
    /// the Tokio runtime, the sync writer runs on the blocking pool inside a `spawn_blocking`,
    /// and a bounded `tokio::sync::mpsc` channel bridges them. The bound (2 in-flight chunks ×
    /// 10 MB each) keeps memory bounded by exerting backpressure on the reader when the writer
    /// is slow.
    ///
    /// Used for files whose size is unknown ahead of time or known to be large (e.g.
    /// version-store blobs streamed from S3 or a push request body).
    pub async fn stream_async<R>(self, reader: &mut R) -> Result<(), OxenError>
    where
        R: tokio::io::AsyncRead + Unpin + ?Sized,
    {
        /// Internal protocol for the async-reader → sync-writer hand-off. The explicit `Eof`
        /// variant is what distinguishes "reader finished cleanly" from "reader's future was
        /// dropped mid-stream". Without it, a cancelled caller would close the channel and the
        /// writer would interpret that as a clean EOF and commit a truncated file.
        enum StreamMsg {
            Chunk(Bytes),
            Err(std::io::Error),
            Eof,
        }

        let Self {
            target,
            expected_hash,
            mtime,
        } = self;
        let mut reader = tokio::io::BufReader::with_capacity(constants::STREAMING_BUF_SIZE, reader);
        let (tx, mut rx) = tokio::sync::mpsc::channel::<StreamMsg>(2);
        // Sync writer side: pull messages off the channel into an `AtomicTempFile`. Commit *only*
        // after seeing an explicit `StreamMsg::Eof`. Anything else — channel-closed before Eof,
        // or `StreamMsg::Err` — means the reader didn't finish cleanly (cancellation, panic, IO
        // failure), so we return an error and let `tmp` drop without committing so the partial
        // scratch is unlinked.
        //
        // When `expected_hash` is `Some`, every chunk also feeds an `Xxh3` hasher and the rename
        // is skipped if the final digest doesn't match. `tmp` drops in that case so `target` is
        // never overwritten — verify-before-publish, not publish-then-rollback.
        //
        // When `mtime` is `Some`, stamp the target file with the correct mtime after writing and
        // before committing.
        let writer = tokio::task::spawn_blocking(move || -> Result<(), OxenError> {
            let mut tmp = AtomicTempFile::create(&target)?;
            let mut hasher = expected_hash.map(|_| Xxh3::new());
            let mut saw_eof = false;
            while let Some(msg) = rx.blocking_recv() {
                match msg {
                    StreamMsg::Chunk(chunk) => {
                        if let Some(h) = hasher.as_mut() {
                            h.update(&chunk);
                        }
                        tmp.as_writer().write_all(&chunk)?;
                    }
                    StreamMsg::Err(err) => return Err(err.into()),
                    StreamMsg::Eof => {
                        saw_eof = true;
                        break;
                    }
                }
            }
            if !saw_eof {
                return Err(std::io::Error::other(format!(
                    "AtomicFile::stream_async: stream to {target:?} ended before EOF; \
                     partial write discarded"
                ))
                .into());
            }
            if let (Some(expected), Some(hasher)) = (expected_hash, hasher) {
                let actual = MerkleHash::new(hasher.digest128());
                if actual != expected {
                    return Err(OxenError::HashMismatch {
                        path: target,
                        expected,
                        actual,
                    });
                }
            }
            if let Some(mtime) = mtime {
                tmp.set_mtime(mtime)?;
            }
            tmp.commit()
        });
        let mut buf = BytesMut::with_capacity(constants::STREAMING_BUF_SIZE);
        loop {
            if buf.capacity() == 0 {
                buf.reserve(constants::STREAMING_BUF_SIZE);
            }
            match reader.read_buf(&mut buf).await {
                Ok(0) => {
                    let _ = tx.send(StreamMsg::Eof).await;
                    break;
                }
                Ok(_) => {
                    let chunk = buf.split().freeze();
                    if tx.send(StreamMsg::Chunk(chunk)).await.is_err() {
                        // Writer dropped (errored or panicked). The error surfaces via the
                        // `writer.await` below; nothing useful to do here.
                        break;
                    }
                }
                Err(err) => {
                    let _ = tx.send(StreamMsg::Err(err)).await;
                    break;
                }
            }
        }
        drop(tx);
        writer.await?
    }

    /// Publish the contents of file `src` to the target path. `src` and the target must live on
    /// the same filesystem (POSIX `rename` is only atomic on one filesystem).
    ///
    /// Note: the `AtomicFile`'s target is the destination and `src` is the source — opposite of
    /// `std::fs::copy(src, dst)`'s positional ordering.
    pub fn copy_from(self, src: &Path) -> Result<(), OxenError> {
        let mut src_file = File::open(src).map_err(|err| OxenError::file_error(src, err))?;
        self.stream(&mut src_file)
    }
}

#[cfg(test)]
mod tests {
    use super::{ATOMIC_TEMP_INFIX, AtomicFile, AtomicTempFile};
    use crate::error::OxenError;
    use crate::model::MerkleHash;
    use crate::test;
    use std::time::{Duration, SystemTime};

    /// `AtomicFile::new()` (plus the builder chain) must not touch the filesystem — the temp
    /// file is materialized inside the terminal method. This guarantee is what makes the
    /// verify-before-publish semantic on `write` work: a hash mismatch can leave nothing on
    /// disk only because nothing was created yet. Lock the invariant down here so a future
    /// refactor that eagerly opens the temp in `new()` fails the test loudly.
    #[test]
    fn test_atomic_file_new_makes_no_filesystem_changes() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let target = dir.join("never_created");
            let mtime = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
            let bogus_hash = MerkleHash::new(0xdead_beef_dead_beef_dead_beef_dead_beefu128);

            // Build a fully-configured AtomicFile and drop it without calling a terminal.
            let _ = AtomicFile::new(&target)
                .with_hash(bogus_hash)
                .with_mtime(mtime);

            assert!(
                !target.exists(),
                "AtomicFile::new() must not create the target"
            );
            let names: Vec<_> = std::fs::read_dir(dir)?
                .filter_map(|e| e.ok().map(|e| e.file_name()))
                .collect();
            assert!(
                names.is_empty(),
                "AtomicFile::new() / with_* leaked something onto disk: {names:?}",
            );
            Ok(())
        })
    }

    #[tokio::test]
    async fn test_atomic_write_round_trip() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            let target = dir.join("file.txt");
            AtomicFile::new(&target).write(b"hello world")?;

            let contents = tokio::fs::read(&target).await?;
            assert_eq!(contents, b"hello world");
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_atomic_write_overwrites_existing() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            let target = dir.join("file.txt");
            tokio::fs::write(&target, b"old contents").await?;

            AtomicFile::new(&target).write(b"new contents")?;

            let contents = tokio::fs::read(&target).await?;
            assert_eq!(contents, b"new contents");
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_atomic_write_creates_parent_dir() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            let target = dir.join("nested").join("deeper").join("file.txt");
            AtomicFile::new(&target).write(b"x")?;

            let contents = tokio::fs::read(&target).await?;
            assert_eq!(contents, b"x");
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_atomic_write_empty_contents() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            let target = dir.join("file.txt");
            AtomicFile::new(&target).write(b"")?;

            let contents = tokio::fs::read(&target).await?;
            assert!(contents.is_empty());
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_atomic_write_verified_commits_on_match() -> Result<(), OxenError> {
        use xxhash_rust::xxh3::xxh3_128;

        test::run_empty_dir_test_async(|dir| async move {
            let target = dir.join("blob.bin");
            let payload = b"the quick brown fox jumps over the lazy dog";
            let expected = MerkleHash::new(xxh3_128(payload));

            AtomicFile::new(&target)
                .with_hash(expected)
                .write(payload)?;

            let written = tokio::fs::read(&target).await?;
            assert_eq!(written, payload);

            // No stray `.oxentmp.<uuid>` siblings.
            let mut entries = tokio::fs::read_dir(&dir).await?;
            let mut names = Vec::new();
            while let Some(entry) = entries.next_entry().await? {
                names.push(entry.file_name());
            }
            assert_eq!(names.len(), 1, "unexpected leftover files: {names:?}");
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_atomic_write_verified_aborts_on_mismatch() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            let target = dir.join("blob.bin");
            let payload = b"the quick brown fox jumps over the lazy dog";
            let bogus_expected = MerkleHash::new(0xdead_beef_dead_beef_dead_beef_dead_beefu128);

            let result = AtomicFile::new(&target)
                .with_hash(bogus_expected)
                .write(payload);

            match result {
                Err(OxenError::HashMismatch {
                    path,
                    expected,
                    actual,
                }) => {
                    assert_eq!(path, target);
                    assert_eq!(expected, bogus_expected);
                    assert_ne!(actual, bogus_expected);
                }
                other => panic!("expected HashMismatch, got {other:?}"),
            }

            // Mismatch must not touch the filesystem at all — no target file, no scratch siblings.
            assert!(!tokio::fs::try_exists(&target).await?);
            let mut entries = tokio::fs::read_dir(&dir).await?;
            assert!(
                entries.next_entry().await?.is_none(),
                "directory should be empty after mismatched verified write"
            );
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_atomic_temp_file_name_pattern() -> Result<(), OxenError> {
        // Verify scratch files follow `<target_basename>.oxentmp.<random>` so fsck can match them
        // later. The `tests` module is a child of `atomic_file`, so it can construct
        // `AtomicTempFile` directly to observe the on-disk name.
        test::run_empty_dir_test_async(|dir| async move {
            let target = dir.join("HEAD");
            let tmp = AtomicTempFile::create(&target)?;
            let temp_path = tmp.tmp.path().to_path_buf();
            let temp_name = temp_path
                .file_name()
                .expect("temp path has a filename")
                .to_string_lossy();

            assert!(
                temp_name.starts_with("HEAD"),
                "temp name {temp_name:?} should start with the target basename"
            );
            assert!(
                temp_name.contains(ATOMIC_TEMP_INFIX),
                "temp name {temp_name:?} should contain {:?}",
                ATOMIC_TEMP_INFIX
            );
            assert_eq!(temp_path.parent(), Some(dir.as_path()));

            drop(tmp); // NamedTempFile's Drop cleans up since we never committed.
            assert!(!tokio::fs::try_exists(temp_path).await?);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_atomic_stream_streams() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            // Stream a payload through `AtomicFile::stream` from an in-memory `Cursor` —
            // this is the shape the version store will use for large blobs.
            let target = dir.join("blob.bin");
            let payload: Vec<u8> = (0..50_000u32).flat_map(u32::to_le_bytes).collect();
            let mut reader = std::io::Cursor::new(payload.clone());

            AtomicFile::new(&target).stream_async(&mut reader).await?;

            let written = tokio::fs::read(&target).await?;
            assert_eq!(written, payload);

            // Parent dir contains only the target — no stray temp files.
            let mut entries = tokio::fs::read_dir(&dir).await?;
            let mut names = Vec::new();
            while let Some(entry) = entries.next_entry().await? {
                names.push(entry.file_name());
            }
            assert_eq!(names.len(), 1, "unexpected leftover files: {names:?}");
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_atomic_stream_cleans_up_on_read_failure() -> Result<(), OxenError> {
        // A reader that succeeds for one read then errors. This exercises the error path through
        // `AtomicFile::stream`: the read failure is forwarded through the channel, the
        // writer's `?` propagates it, `tmp` drops without commit, and `NamedTempFile`'s Drop
        // unlinks the scratch. The user-visible invariant is "no orphans on failure" — which is
        // what we assert.
        use std::pin::Pin;
        use std::task::{Context, Poll};
        use tokio::io::{AsyncRead, ReadBuf};

        struct FailingReader {
            served_once: bool,
        }
        impl AsyncRead for FailingReader {
            fn poll_read(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
                buf: &mut ReadBuf<'_>,
            ) -> Poll<std::io::Result<()>> {
                if !self.served_once {
                    buf.put_slice(b"some-bytes");
                    self.served_once = true;
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Ready(Err(std::io::Error::other("simulated read failure")))
                }
            }
        }

        test::run_empty_dir_test_async(|dir| async move {
            let target = dir.join("blob.bin");
            let mut reader = FailingReader { served_once: false };

            let result = AtomicFile::new(&target).stream_async(&mut reader).await;
            assert!(result.is_err(), "expected the streaming write to fail");

            // Nothing at the target; nothing left behind as scratch.
            assert!(!tokio::fs::try_exists(&target).await?);
            let mut entries = tokio::fs::read_dir(&dir).await?;
            assert!(
                entries.next_entry().await?.is_none(),
                "directory should be empty after failed write"
            );
            Ok(())
        })
        .await
    }

    #[test]
    fn test_atomic_stream_sync_streams() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Stream a payload through `AtomicFile::stream` via an in-memory Cursor. The 200 KB
            // size comfortably exceeds the default tar block boundary and rules out "small enough
            // to fit in one read" accidents in the helper.
            let target = dir.join("blob.bin");
            let payload: Vec<u8> = (0..50_000u32).flat_map(u32::to_le_bytes).collect();
            let mut reader = std::io::Cursor::new(payload.clone());

            AtomicFile::new(&target).stream(&mut reader)?;

            let written = std::fs::read(&target)?;
            assert_eq!(written, payload);

            // Parent dir contains only the target — no stray `.oxentmp.<uuid>` files.
            let names: Vec<_> = std::fs::read_dir(dir)?
                .filter_map(|e| e.ok().map(|e| e.file_name()))
                .collect();
            assert_eq!(names.len(), 1, "unexpected leftover files: {names:?}");
            assert_eq!(names[0], std::ffi::OsStr::new("blob.bin"));
            Ok(())
        })
    }

    #[test]
    fn test_atomic_stream_sync_cleans_up_on_read_failure() -> Result<(), OxenError> {
        // A reader that succeeds for one read then errors. Drives the helper down the
        // error path: `std::io::copy` returns Err, `commit` is never reached, and the
        // `AtomicTempFile`'s inner `NamedTempFile` Drop unlinks the scratch. The
        // user-visible invariant is "no orphans on failure" — which is what we assert.
        struct FailingReader {
            served_once: bool,
        }
        impl std::io::Read for FailingReader {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if !self.served_once {
                    let bytes = b"some-bytes";
                    let n = bytes.len().min(buf.len());
                    buf[..n].copy_from_slice(&bytes[..n]);
                    self.served_once = true;
                    Ok(n)
                } else {
                    Err(std::io::Error::other("simulated read failure"))
                }
            }
        }

        test::run_empty_dir_test(|dir| {
            let target = dir.join("blob.bin");
            let mut reader = FailingReader { served_once: false };

            let result = AtomicFile::new(&target).stream(&mut reader);
            assert!(result.is_err(), "expected the streaming write to fail");

            // Nothing at the target; nothing left behind as scratch (the `NamedTempFile`
            // inside `AtomicTempFile` must have unlinked the temp on drop).
            assert!(
                !target.exists(),
                "target should not exist after failed write"
            );
            let leftover: Vec<_> = std::fs::read_dir(dir)?.filter_map(|e| e.ok()).collect();
            assert!(
                leftover.is_empty(),
                "directory should be empty after failed write, got {} entries",
                leftover.len()
            );
            Ok(())
        })
    }

    #[tokio::test]
    async fn test_atomic_write_concurrent_writers() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            let target = dir.join("file.txt");

            // Spawn N genuinely-concurrent writes via spawn_blocking (so they run on real OS
            // threads, not serialized on a current-thread runtime). Every one must succeed; the
            // final file must equal exactly one of the writers' payloads; no temp files may be
            // left behind.
            let n: usize = 32;
            let mut handles = Vec::with_capacity(n);
            for i in 0..n {
                let target = target.clone();
                let payload = format!("writer-{i}").into_bytes();
                handles.push(tokio::task::spawn_blocking(move || {
                    AtomicFile::new(&target).write(&payload)
                }));
            }
            for h in handles {
                h.await
                    .expect("join should succeed")
                    .expect("AtomicFile::write should succeed");
            }

            let final_contents = tokio::fs::read(&target).await?;
            let final_str = std::str::from_utf8(&final_contents).expect("contents should be utf-8");
            assert!(
                final_str.starts_with("writer-"),
                "unexpected final contents: {final_str:?}",
            );

            // Parent dir should contain only `file.txt` — no stray `.tmp.<uuid>` leftovers.
            let mut entries = tokio::fs::read_dir(&dir).await?;
            let mut names = Vec::new();
            while let Some(entry) = entries.next_entry().await? {
                names.push(entry.file_name());
            }
            assert_eq!(names.len(), 1, "unexpected leftover files: {names:?}");
            assert_eq!(names[0], std::ffi::OsStr::new("file.txt"));
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_atomic_stream_async_verified_commits_on_match() -> Result<(), OxenError> {
        use xxhash_rust::xxh3::xxh3_128;

        test::run_empty_dir_test_async(|dir| async move {
            let target = dir.join("blob.bin");
            let payload: Vec<u8> = (0..50_000u32).flat_map(u32::to_le_bytes).collect();
            let expected = MerkleHash::new(xxh3_128(&payload));
            let mut reader = std::io::Cursor::new(payload.clone());

            AtomicFile::new(&target)
                .with_hash(expected)
                .stream_async(&mut reader)
                .await?;

            let written = tokio::fs::read(&target).await?;
            assert_eq!(written, payload);

            // No stray `.oxentmp.<uuid>` siblings.
            let mut entries = tokio::fs::read_dir(&dir).await?;
            let mut names = Vec::new();
            while let Some(entry) = entries.next_entry().await? {
                names.push(entry.file_name());
            }
            assert_eq!(names.len(), 1, "unexpected leftover files: {names:?}");
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_atomic_stream_async_verified_aborts_on_mismatch() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            let target = dir.join("blob.bin");
            let payload: Vec<u8> = (0..50_000u32).flat_map(u32::to_le_bytes).collect();
            let mut reader = std::io::Cursor::new(payload);
            let bogus_expected = MerkleHash::new(0xdead_beef_dead_beef_dead_beef_dead_beefu128);

            let result = AtomicFile::new(&target)
                .with_hash(bogus_expected)
                .stream_async(&mut reader)
                .await;

            match result {
                Err(OxenError::HashMismatch {
                    path,
                    expected,
                    actual,
                }) => {
                    assert_eq!(path, target);
                    assert_eq!(expected, bogus_expected);
                    assert_ne!(actual, bogus_expected);
                }
                other => panic!("expected HashMismatch, got {other:?}"),
            }

            // Target must not exist (we never renamed) and no scratch siblings either.
            assert!(!tokio::fs::try_exists(&target).await?);
            let mut entries = tokio::fs::read_dir(&dir).await?;
            assert!(
                entries.next_entry().await?.is_none(),
                "directory should be empty after aborted verified write"
            );
            Ok(())
        })
        .await
    }

    #[test]
    fn test_atomic_stream_verified_commits_on_match() -> Result<(), OxenError> {
        use xxhash_rust::xxh3::xxh3_128;

        test::run_empty_dir_test(|dir| {
            let target = dir.join("blob.bin");
            let payload: Vec<u8> = (0..50_000u32).flat_map(u32::to_le_bytes).collect();
            let expected = MerkleHash::new(xxh3_128(&payload));
            let mut reader = std::io::Cursor::new(payload.clone());

            AtomicFile::new(&target)
                .with_hash(expected)
                .stream(&mut reader)?;

            let written = std::fs::read(&target)?;
            assert_eq!(written, payload);

            // No stray `.oxentmp.<uuid>` siblings.
            let names: Vec<_> = std::fs::read_dir(dir)?
                .filter_map(|e| e.ok().map(|e| e.file_name()))
                .collect();
            assert_eq!(names.len(), 1, "unexpected leftover files: {names:?}");
            Ok(())
        })
    }

    #[test]
    fn test_atomic_stream_verified_aborts_on_mismatch() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let target = dir.join("blob.bin");
            let payload: Vec<u8> = (0..50_000u32).flat_map(u32::to_le_bytes).collect();
            let mut reader = std::io::Cursor::new(payload);
            let bogus_expected = MerkleHash::new(0xdead_beef_dead_beef_dead_beef_dead_beefu128);

            let result = AtomicFile::new(&target)
                .with_hash(bogus_expected)
                .stream(&mut reader);

            match result {
                Err(OxenError::HashMismatch {
                    path,
                    expected,
                    actual,
                }) => {
                    assert_eq!(path, target);
                    assert_eq!(expected, bogus_expected);
                    assert_ne!(actual, bogus_expected);
                }
                other => panic!("expected HashMismatch, got {other:?}"),
            }

            // Target must not exist (we never renamed) and no scratch siblings either.
            assert!(!target.exists());
            let names: Vec<_> = std::fs::read_dir(dir)?
                .filter_map(|e| e.ok().map(|e| e.file_name()))
                .collect();
            assert!(names.is_empty(), "directory should be empty: {names:?}");
            Ok(())
        })
    }

    /// Sub-second mtime to exercise nanosecond stamping. `SystemTime` arithmetic is the
    /// same shape `restore_file` uses to assemble the merkle node's recorded mtime.
    fn fixed_mtime() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::new(1_700_000_000, 123_456_789)
    }

    #[test]
    fn test_atomic_copy_with_mtime_round_trip() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let src = dir.join("src.bin");
            let dst = dir.join("dst.bin");
            let payload: Vec<u8> = (0..5_000u32).flat_map(u32::to_le_bytes).collect();
            std::fs::write(&src, &payload)?;

            let mtime = fixed_mtime();
            AtomicFile::new(&dst).with_mtime(mtime).copy_from(&src)?;

            // Bytes match...
            assert_eq!(std::fs::read(&dst)?, payload);
            // ... and mtime is stamped exactly (test platforms support nanosecond mtime).
            let actual = std::fs::metadata(&dst)?.modified()?;
            assert_eq!(actual, mtime);

            // No leftover scratch siblings of `dst`.
            let names: Vec<_> = std::fs::read_dir(dir)?
                .filter_map(|e| e.ok().map(|e| e.file_name()))
                .collect();
            assert_eq!(names.len(), 2, "expected only src+dst, got: {names:?}");
            Ok(())
        })
    }

    #[test]
    fn test_atomic_copy_with_mtime_overwrites_existing() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let src = dir.join("src.bin");
            let dst = dir.join("dst.bin");
            std::fs::write(&src, b"new content")?;
            std::fs::write(&dst, b"old content with different length")?;

            AtomicFile::new(&dst)
                .with_mtime(fixed_mtime())
                .copy_from(&src)?;

            assert_eq!(std::fs::read(&dst)?, b"new content");
            Ok(())
        })
    }

    #[test]
    fn test_atomic_copy_with_mtime_missing_source_leaves_no_scratch() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let src = dir.join("does-not-exist.bin");
            let dst = dir.join("dst.bin");

            let result = AtomicFile::new(&dst)
                .with_mtime(fixed_mtime())
                .copy_from(&src);
            assert!(result.is_err(), "expected error for missing source");

            // We open `src` first, so no temp file should ever appear next to `dst`.
            assert!(!dst.exists());
            let names: Vec<_> = std::fs::read_dir(dir)?
                .filter_map(|e| e.ok().map(|e| e.file_name()))
                .collect();
            assert!(names.is_empty(), "directory should be empty: {names:?}");
            Ok(())
        })
    }

    #[tokio::test]
    async fn test_atomic_stream_async_with_mtime_round_trip() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            let target = dir.join("blob.bin");
            let payload: Vec<u8> = (0..40_000u32).flat_map(u32::to_le_bytes).collect();
            let mut reader = std::io::Cursor::new(payload.clone());

            let mtime = fixed_mtime();
            AtomicFile::new(&target)
                .with_mtime(mtime)
                .stream_async(&mut reader)
                .await?;

            assert_eq!(tokio::fs::read(&target).await?, payload);
            let actual = tokio::fs::metadata(&target).await?.modified()?;
            assert_eq!(actual, mtime);
            Ok(())
        })
        .await
    }

    /// Earlier-than-expected EOF from the async reader path. With the channel hand-off
    /// protocol the writer should bail without committing; nothing lands at `target`.
    #[tokio::test]
    async fn test_atomic_stream_async_with_mtime_cleans_up_on_read_failure() -> Result<(), OxenError>
    {
        use tokio::io::AsyncRead;
        struct FailingReader {
            yielded: bool,
        }
        impl AsyncRead for FailingReader {
            fn poll_read(
                mut self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
                buf: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                if !self.yielded {
                    buf.put_slice(b"partial");
                    self.yielded = true;
                    std::task::Poll::Ready(Ok(()))
                } else {
                    std::task::Poll::Ready(Err(std::io::Error::other("synthetic read failure")))
                }
            }
        }

        test::run_empty_dir_test_async(|dir| async move {
            let target = dir.join("blob.bin");
            let mut reader = FailingReader { yielded: false };

            let result = AtomicFile::new(&target)
                .with_mtime(fixed_mtime())
                .stream_async(&mut reader)
                .await;
            assert!(result.is_err(), "expected reader failure to surface");
            assert!(
                !target.exists(),
                "target must not appear when stream aborts"
            );

            // Scratch sibling should be dropped (auto-cleanup) — no `.oxentmp.` leftovers.
            let stragglers: Vec<_> = std::fs::read_dir(&dir)?
                .filter_map(|e| e.ok().map(|e| e.file_name()))
                .collect();
            assert!(stragglers.is_empty(), "leftover scratch: {stragglers:?}");
            Ok(())
        })
        .await
    }
}
