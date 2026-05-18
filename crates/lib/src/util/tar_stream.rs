//! Generic streaming tar-gz helpers.
//!
//! **Pack** ([`stream_pack`]): callers hand us an iterator of [`PackEntry`]s. Each entry's
//! body is a [`Read`] — payloads flow through gzip+tar one chunk at a time, never
//! materializing the full archive in memory. The iterator is consumed lazily; only one
//! `PackEntry` (and its in-flight `body` read state) is alive at a time.
//!
//! **Unpack** ([`stream_unpack`]): we drive a per-entry callback that gets a borrowed
//! [`StreamedEntry`] (with the body exposed as `&mut dyn Read` over the archive's
//! current entry). The callback decides what to do — buffer, decode, write, batch — and
//! mutates a caller-supplied `state` value so accumulated data flows back out without
//! globals. Path-traversal and unsupported entry types are rejected before the callback
//! ever sees the entry.
//!
//! Centralizing the gzip+tar pipeline here means each merkle-store backend keeps only the
//! "produce/consume entries" logic; the wire framing lives in one place.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;

/// Errors from the generic streaming tar-gz pipeline.
#[derive(Debug, thiserror::Error)]
pub enum TarStreamError {
    #[error("tar I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Path traversal detected in tar entry: {path}")]
    PathTraversal { path: String },
    #[error("Unsupported tar entry type at {path}: only regular files and directories are allowed")]
    UnsupportedEntry { path: String },
}

/// Tar entry kinds we support emitting and consuming. Anything else (symlinks,
/// device nodes, …) is rejected at the unpack boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Directory,
    File,
}

/// One logical entry the packer is about to write.
///
/// `size` must match the exact number of bytes [`body`] will produce. Tar headers
/// embed the entry length up front, so streaming bodies need their length known
/// before any byte is read. For directories pass `size: 0`.
pub struct PackEntry {
    pub path: PathBuf,
    pub kind: EntryKind,
    pub size: u64,
    pub mode: u32,
    /// The entry body. `'static` (boxed-dyn-default) so heterogeneous sources
    /// (`std::io::Cursor<Vec<u8>>` for in-memory blobs, `std::fs::File` for on-
    /// disk payloads, anything custom) share one iterator item type without
    /// lifetime gymnastics.
    pub body: Box<dyn Read>,
}

/// Stream a sequence of [`PackEntry`]s through gzip+tar into `out`.
///
/// `entries` is consumed lazily: at most one `PackEntry` (and its in-flight
/// `body` read state) is alive at a time, so the producer can yield arbitrarily
/// many entries without the archive itself ever being materialized.
///
/// Each entry's header gets the entry's `mode`, `size`, and an entry-type chosen
/// from [`EntryKind`]. A GNU-format header is used so long paths still travel
/// portably.
///
/// `E` is the caller's error type — the iterator may surface backend-specific
/// errors (e.g. LMDB read failures) through `Result<PackEntry, E>` and they
/// propagate without lossy conversion. Internal tar/gzip I/O errors are wrapped
/// in [`TarStreamError`] and lifted into `E` via `From`.
pub fn stream_pack<W, I, E>(out: W, compression: Compression, entries: I) -> Result<(), E>
where
    W: Write,
    I: IntoIterator<Item = Result<PackEntry, E>>,
    E: From<TarStreamError>,
{
    let enc = GzEncoder::new(out, compression);
    let mut tar = tar::Builder::new(enc);
    for entry in entries {
        let mut entry = entry?;
        let mut header = tar::Header::new_gnu();
        header.set_size(entry.size);
        header.set_mode(entry.mode);
        header.set_entry_type(match entry.kind {
            EntryKind::Directory => tar::EntryType::Directory,
            EntryKind::File => tar::EntryType::Regular,
        });
        header.set_cksum();
        tar.append_data(&mut header, &entry.path, entry.body.as_mut())
            .map_err(TarStreamError::from)?;
    }
    tar.finish().map_err(TarStreamError::from)?;
    tar.into_inner()
        .map_err(TarStreamError::from)?
        .finish()
        .map_err(TarStreamError::from)?;
    Ok(())
}

/// Borrowed view of an in-flight tar entry handed to [`stream_unpack`]'s callback.
///
/// `body` is a `Read` over the archive's current entry payload. The callback may
/// read it fully, partially, or not at all — the tar reader advances over any
/// unread tail automatically before the next entry.
pub struct StreamedEntry<'a> {
    pub path: &'a Path,
    pub kind: EntryKind,
    pub size: u64,
    pub body: &'a mut dyn Read,
}

/// Drive `on_entry` once per tar entry in archive order, threading `state`
/// through. Returns the final `state` so callers can accumulate parsed data,
/// batch commits, etc.
///
/// Validates each entry before calling the callback:
///   * Paths containing a `..` component return [`TarStreamError::PathTraversal`].
///   * Entries that aren't regular files or directories return
///     [`TarStreamError::UnsupportedEntry`].
///
/// Both checks are the chokepoint each merkle-store backend's unpack used to
/// re-implement; collecting them here means callers can't forget either one.
///
/// `E` is the callback's error type — callbacks may surface backend-specific
/// errors (e.g. database write failures) and they propagate without lossy
/// conversion. Internal tar/gzip and validation errors are lifted from
/// [`TarStreamError`] into `E` via `From`.
pub fn stream_unpack<R, S, F, E>(reader: R, mut state: S, mut on_entry: F) -> Result<S, E>
where
    R: Read,
    F: FnMut(StreamedEntry<'_>, &mut S) -> Result<(), E>,
    E: From<TarStreamError>,
{
    let decoder = GzDecoder::new(reader);
    let mut archive = tar::Archive::new(decoder);
    for entry in archive.entries().map_err(TarStreamError::from)? {
        let mut entry = entry.map_err(TarStreamError::from)?;
        let path = entry.path().map_err(TarStreamError::from)?.into_owned();
        if path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(TarStreamError::PathTraversal {
                path: path.display().to_string(),
            }
            .into());
        }
        let raw_type = entry.header().entry_type();
        let kind = if raw_type.is_dir() {
            EntryKind::Directory
        } else if raw_type.is_file() {
            EntryKind::File
        } else {
            return Err(TarStreamError::UnsupportedEntry {
                path: path.display().to_string(),
            }
            .into());
        };
        let size = entry.header().size().map_err(TarStreamError::from)?;
        let streamed = StreamedEntry {
            path: &path,
            kind,
            size,
            body: &mut entry,
        };
        on_entry(streamed, &mut state)?;
    }
    Ok(state)
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use super::*;

    /// Pack three entries (one dir, two files of differing sizes including 0 bytes
    /// and a multi-MiB body) into an in-memory buffer, then unpack and assert exact
    /// path / size / body bytes round-trip.
    #[test]
    fn test_roundtrip_identity() {
        let big_body: Vec<u8> = (0u8..=255).cycle().take(5 * 1024 * 1024 + 7).collect();
        let entries: Vec<Result<PackEntry, TarStreamError>> = vec![
            Ok(PackEntry {
                path: PathBuf::from("topdir"),
                kind: EntryKind::Directory,
                size: 0,
                mode: 0o755,
                body: Box::new(Cursor::new(Vec::new())),
            }),
            Ok(PackEntry {
                path: PathBuf::from("topdir/empty.bin"),
                kind: EntryKind::File,
                size: 0,
                mode: 0o644,
                body: Box::new(Cursor::new(Vec::<u8>::new())),
            }),
            Ok(PackEntry {
                path: PathBuf::from("topdir/big.bin"),
                kind: EntryKind::File,
                size: big_body.len() as u64,
                mode: 0o644,
                body: Box::new(Cursor::new(big_body.clone())),
            }),
        ];

        let mut buf = Vec::new();
        stream_pack::<_, _, TarStreamError>(&mut buf, Compression::fast(), entries)
            .expect("pack failed");

        #[derive(Default)]
        struct St {
            entries: Vec<(PathBuf, EntryKind, u64, Vec<u8>)>,
        }
        let st = stream_unpack::<_, _, _, TarStreamError>(&buf[..], St::default(), |entry, st| {
            let mut body = Vec::new();
            entry.body.read_to_end(&mut body)?;
            st.entries
                .push((entry.path.to_path_buf(), entry.kind, entry.size, body));
            Ok(())
        })
        .expect("unpack failed");

        assert_eq!(st.entries.len(), 3, "expected three entries");
        assert_eq!(st.entries[0].0, PathBuf::from("topdir"));
        assert_eq!(st.entries[0].1, EntryKind::Directory);
        assert_eq!(st.entries[1].0, PathBuf::from("topdir/empty.bin"));
        assert_eq!(st.entries[1].3, Vec::<u8>::new());
        assert_eq!(st.entries[2].0, PathBuf::from("topdir/big.bin"));
        assert_eq!(st.entries[2].3, big_body);
    }

    /// The iterator is consumed lazily: items are only constructed as the tar
    /// pipeline pulls them. Verified by counting `next()` calls on a custom
    /// iterator that increments a shared counter.
    #[test]
    fn test_iterator_consumed_lazily() {
        struct CountingIter {
            counter: Arc<AtomicUsize>,
            remaining: usize,
            idx: usize,
        }
        impl Iterator for CountingIter {
            type Item = Result<PackEntry, TarStreamError>;
            fn next(&mut self) -> Option<Self::Item> {
                if self.remaining == 0 {
                    return None;
                }
                self.remaining -= 1;
                self.counter.fetch_add(1, Ordering::SeqCst);
                let idx = self.idx;
                self.idx += 1;
                Some(Ok(PackEntry {
                    path: PathBuf::from(format!("entry-{idx}")),
                    kind: EntryKind::File,
                    size: 4,
                    mode: 0o644,
                    body: Box::new(Cursor::new(vec![1u8, 2, 3, 4])),
                }))
            }
        }

        let counter = Arc::new(AtomicUsize::new(0));
        let iter = CountingIter {
            counter: counter.clone(),
            remaining: 5,
            idx: 0,
        };

        let mut buf = Vec::new();
        stream_pack::<_, _, TarStreamError>(&mut buf, Compression::fast(), iter)
            .expect("pack failed");
        assert_eq!(
            counter.load(Ordering::SeqCst),
            5,
            "iterator should be polled exactly once per entry"
        );
    }

    /// Hand-build a tar entry with `..` in its path; stream_unpack must reject
    /// it with `PathTraversal` before invoking the callback.
    #[test]
    fn test_path_traversal_rejected() {
        let mut buf = Vec::new();
        {
            let enc = GzEncoder::new(&mut buf, Compression::fast());
            let mut tar = tar::Builder::new(enc);
            let mut header = tar::Header::new_old();
            header.set_size(0);
            header.set_mode(0o644);
            header.set_entry_type(tar::EntryType::Regular);
            // Bypass tar's path validation by writing the name directly into
            // the old-style name field.
            let name_bytes = b"escape/../oops";
            let old = header.as_old_mut();
            old.name[..name_bytes.len()].copy_from_slice(name_bytes);
            header.set_cksum();
            tar.append(&header, Cursor::new(Vec::new()))
                .expect("malicious tar build");
            tar.finish().expect("tar finish");
            tar.into_inner()
                .expect("tar inner")
                .finish()
                .expect("gz finish");
        }

        let callback_invocations = Arc::new(AtomicUsize::new(0));
        let cb = callback_invocations.clone();
        let result = stream_unpack::<_, _, _, TarStreamError>(&buf[..], (), |_entry, _st| {
            cb.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });

        assert!(
            matches!(result, Err(TarStreamError::PathTraversal { .. })),
            "expected PathTraversal, got {result:?}"
        );
        assert_eq!(
            callback_invocations.load(Ordering::SeqCst),
            0,
            "callback should never see a path-traversal entry"
        );
    }

    /// A tar containing a symlink entry must be rejected with `UnsupportedEntry`.
    #[test]
    fn test_unsupported_entry_rejected() {
        let mut buf = Vec::new();
        {
            let enc = GzEncoder::new(&mut buf, Compression::fast());
            let mut tar = tar::Builder::new(enc);
            let mut header = tar::Header::new_gnu();
            header.set_size(0);
            header.set_mode(0o777);
            header.set_entry_type(tar::EntryType::Symlink);
            header.set_link_name("/etc/passwd").expect("set_link_name");
            header.set_cksum();
            tar.append_data(&mut header, "evil_link", Cursor::new(Vec::new()))
                .expect("symlink build");
            tar.finish().expect("tar finish");
            tar.into_inner()
                .expect("tar inner")
                .finish()
                .expect("gz finish");
        }

        let result = stream_unpack::<_, _, _, TarStreamError>(&buf[..], (), |_entry, _st| Ok(()));
        assert!(
            matches!(result, Err(TarStreamError::UnsupportedEntry { .. })),
            "expected UnsupportedEntry, got {result:?}"
        );
    }

    /// If the callback returns an error mid-stream, the error propagates and the
    /// remaining entries are not delivered.
    #[test]
    fn test_callback_short_circuits() {
        let entries: Vec<Result<PackEntry, TarStreamError>> = (0..5)
            .map(|i| {
                Ok(PackEntry {
                    path: PathBuf::from(format!("entry-{i}")),
                    kind: EntryKind::File,
                    size: 1,
                    mode: 0o644,
                    body: Box::new(Cursor::new(vec![i as u8])),
                })
            })
            .collect();
        let mut buf = Vec::new();
        stream_pack::<_, _, TarStreamError>(&mut buf, Compression::fast(), entries)
            .expect("pack failed");

        let observed = Arc::new(Mutex::new(Vec::<PathBuf>::new()));
        let observed_clone = observed.clone();
        let result = stream_unpack::<_, _, _, TarStreamError>(&buf[..], (), move |entry, _st| {
            let count = {
                let mut g = observed_clone.lock().unwrap();
                g.push(entry.path.to_path_buf());
                g.len()
            };
            if count == 2 {
                Err(TarStreamError::Io(std::io::Error::other(
                    "stop after 2 entries",
                )))
            } else {
                Ok(())
            }
        });
        assert!(
            matches!(result, Err(TarStreamError::Io(_))),
            "expected Io error from callback, got {result:?}"
        );
        let seen = observed.lock().unwrap();
        assert_eq!(seen.len(), 2, "expected exactly two entries delivered");
    }

    /// State is threaded through `stream_unpack` and returned at the end —
    /// callers accumulate without static globals.
    #[test]
    fn test_state_threading() {
        let entries: Vec<Result<PackEntry, TarStreamError>> = (0..3)
            .map(|i| {
                Ok(PackEntry {
                    path: PathBuf::from(format!("entry-{i}")),
                    kind: EntryKind::File,
                    size: 4,
                    mode: 0o644,
                    body: Box::new(Cursor::new(vec![i as u8; 4])),
                })
            })
            .collect();
        let mut buf = Vec::new();
        stream_pack::<_, _, TarStreamError>(&mut buf, Compression::fast(), entries)
            .expect("pack failed");

        let total = stream_unpack::<_, _, _, TarStreamError>(&buf[..], 0u64, |entry, st| {
            let mut body = Vec::new();
            entry.body.read_to_end(&mut body)?;
            *st += body.len() as u64;
            Ok(())
        })
        .expect("unpack failed");
        assert_eq!(total, 3 * 4, "state must aggregate body sizes");
    }

    /// If the callback reads only the first few bytes of a large body, the next
    /// entry must still be parsed correctly — the tar reader skips the unread
    /// tail.
    #[test]
    fn test_partial_body_read_advances_to_next_entry() {
        let body_a: Vec<u8> = vec![0xAA; 1024];
        let body_b: Vec<u8> = vec![0xBB; 16];
        let entries: Vec<Result<PackEntry, TarStreamError>> = vec![
            Ok(PackEntry {
                path: PathBuf::from("a.bin"),
                kind: EntryKind::File,
                size: body_a.len() as u64,
                mode: 0o644,
                body: Box::new(Cursor::new(body_a.clone())),
            }),
            Ok(PackEntry {
                path: PathBuf::from("b.bin"),
                kind: EntryKind::File,
                size: body_b.len() as u64,
                mode: 0o644,
                body: Box::new(Cursor::new(body_b.clone())),
            }),
        ];

        let mut buf = Vec::new();
        stream_pack::<_, _, TarStreamError>(&mut buf, Compression::fast(), entries)
            .expect("pack failed");

        #[derive(Default)]
        struct St {
            second_entry_body: Vec<u8>,
        }
        let st = stream_unpack::<_, _, _, TarStreamError>(&buf[..], St::default(), |entry, st| {
            if entry.path == Path::new("a.bin") {
                // Only consume 4 bytes of A's 1024-byte body.
                let mut sink = [0u8; 4];
                entry.body.read_exact(&mut sink)?;
            } else {
                let mut body = Vec::new();
                entry.body.read_to_end(&mut body)?;
                st.second_entry_body = body;
            }
            Ok(())
        })
        .expect("unpack failed");
        assert_eq!(
            st.second_entry_body, body_b,
            "second entry's body must be intact despite first being partially read"
        );
    }

    /// Passing a `size` mismatched to the body bytes is a programming error.
    /// tar refuses to write more bytes than the header claimed.
    #[test]
    fn test_size_smaller_than_body_truncates_at_size() {
        // Body is 10 bytes but we tell tar size=4. tar will write 4 bytes and
        // ignore the remaining body bytes; round-tripping should yield 4 bytes.
        let entries: Vec<Result<PackEntry, TarStreamError>> = vec![Ok(PackEntry {
            path: PathBuf::from("a.bin"),
            kind: EntryKind::File,
            size: 4,
            mode: 0o644,
            body: Box::new(Cursor::new(vec![0u8; 10])),
        })];

        let mut buf = Vec::new();
        stream_pack::<_, _, TarStreamError>(&mut buf, Compression::fast(), entries)
            .expect("pack failed");

        let st =
            stream_unpack::<_, _, _, TarStreamError>(&buf[..], Vec::<u8>::new(), |entry, st| {
                entry.body.read_to_end(st)?;
                Ok(())
            })
            .expect("unpack failed");
        assert_eq!(st.len(), 4, "tar honors header size, truncating the body");
    }

    /// Compression level affects the raw output bytes but not the parsed entry
    /// set. Both fast and default compression must round-trip the same payload.
    #[test]
    fn test_gzip_compression_levels_roundtrip_equally() {
        let body: Vec<u8> = (0u8..=255).cycle().take(64 * 1024).collect();
        let build_entries = || -> Vec<Result<PackEntry, TarStreamError>> {
            vec![Ok(PackEntry {
                path: PathBuf::from("a.bin"),
                kind: EntryKind::File,
                size: body.len() as u64,
                mode: 0o644,
                body: Box::new(Cursor::new(body.clone())),
            })]
        };

        let mut buf_fast = Vec::new();
        stream_pack::<_, _, TarStreamError>(&mut buf_fast, Compression::fast(), build_entries())
            .expect("fast pack failed");
        let mut buf_default = Vec::new();
        stream_pack::<_, _, TarStreamError>(
            &mut buf_default,
            Compression::default(),
            build_entries(),
        )
        .expect("default pack failed");

        let read_one = |buf: &[u8]| -> Vec<u8> {
            stream_unpack::<_, _, _, TarStreamError>(buf, Vec::<u8>::new(), |entry, st| {
                entry.body.read_to_end(st)?;
                Ok(())
            })
            .expect("unpack failed")
        };

        assert_eq!(read_one(&buf_fast), body);
        assert_eq!(read_one(&buf_default), body);
    }
}
