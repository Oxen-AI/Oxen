//! Wrapper around std::fs commands to make them easier to use
//! and eventually abstract away the fs implementation
//!

use async_std::pin::Pin;
use bytes::Bytes;
use futures::StreamExt;
use jwalk::WalkDir;

use thiserror::Error;

use simdutf8::compat::from_utf8;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::ffi::OsString;
use std::fs::File;
use std::io::BufReader;
use std::io::Cursor;
use std::io::prelude::*;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio_stream::Stream;

use crate::constants::CHUNKS_DIR;
use crate::constants::OXEN_HIDDEN_DIR;
use crate::constants::TREE_DIR;
use crate::error::OxenError;
use crate::model::MerkleHash;
use crate::model::merkle_tree::node::FileNode;
use crate::model::metadata::metadata_image::ImgResize;
use crate::model::metadata::metadata_video::VideoThumbnail;
use crate::model::{EntryDataType, LocalRepository};
use crate::opts::CountLinesOpts;
use crate::storage::version_store::VersionStore;
use crate::view::health::DiskUsage;
use crate::{constants, repositories, util};
use filetime::FileTime;
use image::{ImageFormat, ImageReader};
#[cfg(feature = "ffmpeg")]
use thumbnails::Thumbnailer;

#[derive(Debug, Error)]
pub enum FsError {
    #[error("dir bounds violation")]
    DirBoundsViolation,
    #[error("path bounds violation")]
    PathBoundsViolation,
    /// A file deosn't exist.
    #[error("File does not exist: {0}")]
    FileNotFound(PathBuf),
    /// A failure occurred while reading a file.
    #[error("Could not read file: {0} error {1}")]
    FileReadError(PathBuf, std::io::Error),
    /// A failure occurred while copying from one file to another.
    /// Appropriate to use when copying directory contents too.
    #[error("File copy error: {err}\nCould not copy from `{src:?}` to `{dst:?}`")]
    CopyError {
        src: PathBuf,
        dst: PathBuf,
        err: std::io::Error,
    },
    /// A failure occurred while renaming a file.
    /// Appropriate to use when renaming a directory too.
    #[error("File rename error: {err}\nCould not rename `{src:?}` to `{dst:?}`")]
    RenameError {
        src: PathBuf,
        dst: PathBuf,
        err: std::io::Error,
    },
    /// In attempting to write a file, the file could not be created.
    #[error("Could not create file to write {0}\n{1}")]
    FileCreateError(PathBuf, std::io::Error),
    /// In attempting to write a file, after creating the file, the file could not be written to.
    #[error("Could not write file {0}\n{1}")]
    FileWriteError(PathBuf, std::io::Error),
    /// Could not remove a file or a directory.
    #[error("Could not remove {0}\n{1}")]
    RemoveError(PathBuf, std::io::Error),
    /// An attempt was made to create a directory but it failed.
    #[error("Failed to create directory {0}\n{1}")]
    DirCreateError(PathBuf, std::io::Error),
    /// Cannot read the listing of a directory.
    #[error("Could not list directory contents for {0}\n{1}")]
    DirReadError(PathBuf, std::io::Error),
    /// An operation on a file failed because the file is in use by another process.
    /// This problem arises only on Windows.
    #[error("File is in use by another process after {0} attempts: {1}\n{2}")]
    FileInUseError(u32, PathBuf, std::io::Error),
    /// The user's $HOME (~/) directory could not be found.
    #[error("Home directory not found")]
    HomeDirNotFound,
    /// The user's default cache directory could not be found. (And they didn't set OXEN_TMP_DIR).
    #[error("Cache directory not found")]
    CacheDirNotFound,
}

// Deprecated
pub fn oxen_hidden_dir(repo_path: impl AsRef<Path>) -> PathBuf {
    PathBuf::from(repo_path.as_ref()).join(Path::new(constants::OXEN_HIDDEN_DIR))
}

pub fn oxen_tmp_dir() -> Result<PathBuf, FsError> {
    // Override the cache dir with the OXEN_TMP_DIR env var if it is set
    if let Ok(tmp_dir) = std::env::var("OXEN_TMP_DIR") {
        return Ok(PathBuf::from(tmp_dir));
    }

    match dirs::cache_dir() {
        Some(cache_dir) => Ok(cache_dir.join(constants::OXEN)),
        None => Err(FsError::CacheDirNotFound),
    }
}

pub fn oxen_config_dir() -> Result<PathBuf, FsError> {
    // Override the home dir with the OXEN_CONFIG_DIR env var if it is set
    if let Ok(config_dir) = std::env::var("OXEN_CONFIG_DIR") {
        return Ok(PathBuf::from(config_dir));
    }

    match dirs::home_dir() {
        Some(home_dir) => Ok(home_dir.join(constants::CONFIG_DIR).join(constants::OXEN)),
        None => Err(FsError::HomeDirNotFound),
    }
}

pub fn config_filepath(repo_path: &Path) -> PathBuf {
    oxen_hidden_dir(repo_path).join(constants::REPO_CONFIG_FILENAME)
}

pub async fn handle_image_resize(
    version_store: Arc<dyn VersionStore>,
    file_hash: String,
    file_path: &Path,
    img_resize: ImgResize,
) -> Result<
    (
        Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
        u64,
    ),
    OxenError,
> {
    log::debug!("img_resize {img_resize:?}");
    let derived_filename = resized_filename(file_path, img_resize.width, img_resize.height);

    let stream =
        resize_cache_image_version_store(version_store, &file_hash, &derived_filename, img_resize)
            .await?;
    log::debug!("Got the resized image! {derived_filename:?}");

    Ok(stream)
}

/// Constructs '{width}x{height}.{extension}' as an OS-friendly string using the image path's extension.
/// Uses an empty string if the width or height is None or if the image path doesn't have an extension.
pub fn resized_filename(img_path: &Path, width: Option<u32>, height: Option<u32>) -> OsString {
    // {width}x{height}.{extension}
    let mut s = OsString::new();
    push_or(&mut s, width.map(|w| w.to_string()), "");
    s.push("x");
    push_or(&mut s, height.map(|h| h.to_string()), "");
    s.push(".");
    s.push(extension_from_path(img_path));
    s
}

/// Pushes the value into the OsString if Some, otherwise pushes the default value.
fn push_or<A: AsRef<OsStr>, B: AsRef<OsStr>>(s: &mut OsString, value: Option<A>, default: B) {
    match value {
        Some(v) => s.push(v),
        None => s.push(default),
    }
}

/// Returns a reference to the path's extension or an empty string if there is none.
pub fn extension_from_path(path: &Path) -> &OsStr {
    path.extension().unwrap_or_else(|| OsStr::new(""))
}

// Creats a filename for a video thumbnail with the given width, height, and timestamp in the name.
// Formatted as: `"thumbnail_{width_str}x{height_str}_{timestamp_str}.{extension}"`
pub fn video_thumbnail_filename(
    width: Option<u32>,
    height: Option<u32>,
    timestamp: Option<f64>,
) -> OsString {
    // "thumbnail_{width_str}x{height_str}_{timestamp_str}.{extension}"
    let mut s = OsString::new();
    s.push("thumbnail_");
    push_or(&mut s, width.map(|w| w.to_string()), "auto");
    s.push("x");
    push_or(&mut s, height.map(|h| h.to_string()), "auto");
    s.push("_");
    push_or(&mut s, timestamp.map(|t| format!("t{t:.1}")), "t1.0");
    s.push(".jpg"); // extension is hardcoded as "jpg"
    s
}

pub fn chunk_path(repo: &LocalRepository, hash: impl AsRef<str>) -> PathBuf {
    oxen_hidden_dir(&repo.path)
        .join(TREE_DIR)
        .join(CHUNKS_DIR)
        .join(hash.as_ref())
        .join("data")
}

/// Wrapper for [`std::fs::read_to_string`] that logs error and returns it as an [`FsError::FileReadError`].
pub fn read_string(path: impl AsRef<Path>) -> Result<String, FsError> {
    let path = path.as_ref();
    match std::fs::read_to_string(path) {
        Ok(contents) => Ok(contents),
        Err(e_source) => {
            let err = FsError::FileReadError(path.to_path_buf(), e_source);
            log::warn!("{}", err);
            Err(err)
        }
    }
}

/// Wrapper for [`tokio::fs::read_to_string`] that logs error and returns it as an [`FsError::FileReadError`].
pub async fn read_string_a(path: impl AsRef<Path>) -> Result<String, FsError> {
    let path = path.as_ref();
    match tokio::fs::read_to_string(path).await {
        Ok(contents) => Ok(contents),
        Err(e_source) => {
            let err = FsError::FileReadError(path.to_path_buf(), e_source);
            log::warn!("{}", err);
            Err(err)
        }
    }
}

pub fn write_to_path(path: impl AsRef<Path>, value: impl AsRef<str>) -> Result<(), FsError> {
    let path = path.as_ref();
    let value = value.as_ref();

    // Make sure the parent directory exists
    if let Some(parent) = path.parent() {
        create_dir_all(parent)?;
    }

    write_data_s(path, value.as_bytes())
}

pub fn write_data_s(path: &Path, data: &[u8]) -> Result<(), FsError> {
    match std::fs::File::create(path) {
        Ok(mut file) => match file.write_all(data) {
            Ok(_) => Ok(()),
            Err(err) => Err(FsError::FileWriteError(path.to_path_buf(), err)),
        },
        Err(err) => Err(FsError::FileCreateError(path.to_path_buf(), err)),
    }
}

pub async fn write_data_a(path: &Path, data: &[u8]) -> Result<(), FsError> {
    match tokio::fs::File::create(path).await {
        Ok(mut file) => match file.write_all(data).await {
            Ok(_) => Ok(()),
            Err(err) => Err(FsError::FileWriteError(path.to_path_buf(), err)),
        },
        Err(err) => Err(FsError::FileCreateError(path.to_path_buf(), err)),
    }
}

pub fn count_lines(
    path: impl AsRef<Path>,
    opts: CountLinesOpts,
) -> Result<(usize, Option<usize>), OxenError> {
    let path = path.as_ref();
    let file = File::open(path)?;

    let mut reader = BufReader::with_capacity(1024 * 32, file);
    let mut line_count = 1;
    let mut char_count = 0;
    let mut last_buf: Vec<u8> = Vec::new();
    let mut char_option: Option<usize> = None;

    loop {
        let len = {
            let buf = reader.fill_buf()?;

            if buf.is_empty() {
                break;
            }

            if opts.remove_trailing_blank_line {
                last_buf = buf.to_vec();
            }

            if opts.with_chars {
                char_count += bytecount::num_chars(buf);
            }

            line_count += bytecount::count(buf, b'\n');
            buf.len()
        };
        reader.consume(len);
    }

    if let Some(last_byte) = last_buf.last()
        && last_byte == &b'\n'
    {
        line_count -= 1;
    }

    if opts.with_chars {
        char_option = Some(char_count);
    }

    Ok((line_count, char_option))
}

pub fn read_first_n_bytes(path: impl AsRef<Path>, n: usize) -> Result<Vec<u8>, OxenError> {
    let mut file = File::open(path.as_ref())?;
    let mut buffer = vec![0; n];
    let bytes_read = file.read(&mut buffer)?;
    buffer.truncate(bytes_read);
    Ok(buffer)
}

pub fn read_first_line(path: impl AsRef<Path>) -> Result<String, OxenError> {
    let file = File::open(path.as_ref())?;
    read_first_line_from_file(&file)
}

pub fn read_first_line_from_file(file: &File) -> Result<String, OxenError> {
    let reader = BufReader::new(file);
    if let Some(Ok(line)) = reader.lines().next() {
        Ok(line)
    } else {
        Err(OxenError::basic_str(format!(
            "Could not read line from file: {file:?}"
        )))
    }
}

pub fn read_first_byte_from_file(path: impl AsRef<Path>) -> Result<char, OxenError> {
    let mut file = File::open(path)?;
    let mut buffer = [0; 1]; // Single byte buffer
    file.read_exact(&mut buffer)?;
    let first_char = buffer[0] as char;
    Ok(first_char)
}

pub fn list_dirs_in_dir(dir: &Path) -> Result<Vec<PathBuf>, OxenError> {
    let mut dirs: Vec<PathBuf> = vec![];
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            dirs.push(path);
        }
    }
    Ok(dirs)
}

pub async fn list_files_in_dir(dir: &Path) -> Result<Vec<PathBuf>, OxenError> {
    let mut files: Vec<PathBuf> = Vec::new();
    let mut entries = tokio::fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if entry.file_type().await?.is_file() {
            files.push(path);
        }
    }
    Ok(files)
}

pub fn rlist_paths_in_dir(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = vec![];
    if !dir.is_dir() {
        return files;
    }

    for entry in WalkDir::new(dir) {
        match entry {
            Ok(val) => {
                let path = val.path();
                files.push(path);
            }
            Err(err) => eprintln!("rlist_paths_in_dir Could not iterate over dir... {err}"),
        }
    }
    files
}

/// Recursively lists directories in a repo that are not .oxen directories
pub fn rlist_dirs_in_repo(repo: &LocalRepository) -> Vec<PathBuf> {
    let dir = &repo.path;
    let mut dirs: Vec<PathBuf> = vec![];
    if !dir.is_dir() {
        return dirs;
    }

    for entry in WalkDir::new(dir) {
        match entry {
            Ok(val) => {
                let path = val.path();
                if path.is_dir() && !is_in_oxen_hidden_dir(&path) {
                    dirs.push(path);
                }
            }
            Err(err) => log::error!("rlist_dirs_in_repo Could not iterate over dir... {err}"),
        }
    }
    dirs
}

/// Recursively tries to traverse up for an .oxen directory, returns None if not found
pub fn get_repo_root(path: &Path) -> Option<PathBuf> {
    if path.join(OXEN_HIDDEN_DIR).exists() {
        return Some(path.to_path_buf());
    }

    if let Some(parent) = path.parent() {
        get_repo_root(parent)
    } else {
        None
    }
}

pub fn get_repo_root_from_current_dir() -> Option<PathBuf> {
    let Ok(path) = std::env::current_dir() else {
        log::error!("Could not get current directory");
        return None;
    };
    get_repo_root(&path)
}

// TODO: this should be
pub fn copy_dir_all(from: impl AsRef<Path>, to: impl AsRef<Path>) -> Result<(), FsError> {
    // There is not a recursive copy in the standard library, so we implement it here
    let from = from.as_ref();
    let to = to.as_ref();
    log::debug!("copy_dir_all Copy directory from: {from:?} -> to: {to:?}");

    let mut stack = Vec::new();
    stack.push(PathBuf::from(from));

    let output_root = PathBuf::from(to);
    let input_root = PathBuf::from(from).components().count();

    while let Some(working_path) = stack.pop() {
        // log::debug!("copy_dir_all process: {:?}", &working_path);

        // Generate a relative path
        let src: PathBuf = working_path.components().skip(input_root).collect();

        // Create a destination if missing
        let dest = if src.components().count() == 0 {
            output_root.clone()
        } else {
            output_root.join(&src)
        };
        if !dest.exists() {
            // log::debug!("copy_dir_all  mkdir: {:?}", dest);
            util::fs::create_dir_all(&dest)?;
        }

        let dir_entries = match std::fs::read_dir(&working_path) {
            Ok(x) => x,
            Err(e) => return Err(FsError::DirReadError(working_path, e)),
        };

        for failable_entry in dir_entries {
            let entry = match failable_entry {
                Ok(x) => x,
                Err(e) => return Err(FsError::DirReadError(working_path, e)),
            };
            let src = entry.path();
            if src.is_dir() {
                stack.push(src);
            } else {
                match src.file_name() {
                    Some(filename) => {
                        let dst = dest.join(filename);
                        if let Err(err) = std::fs::copy(&src, &dst) {
                            return Err(FsError::CopyError { src, dst, err });
                        }
                    }
                    None => {
                        log::error!("copy_dir_all could not get file_name: {src:?}");
                    }
                }
            }
        }
    }

    Ok(())
}

/// Wrapper around the std::fs::copy command to tell us which file failed to copy
pub fn copy(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<(), FsError> {
    let src = src.as_ref();
    if !src.exists() {
        return Err(FsError::FileNotFound(src.to_path_buf()));
    }

    let dst = dst.as_ref();
    let max_retries = 3;

    let mut attempt = 0;
    while attempt < max_retries {
        match std::fs::copy(src, dst) {
            Ok(_) => return Ok(()),
            Err(err) => {
                if err.raw_os_error() == Some(32) {
                    attempt += 1;
                    if attempt >= max_retries {
                        return Err(FsError::FileInUseError(max_retries, dst.to_path_buf(), err));
                    }
                    // Exponential backoff: 100ms, 200ms, 400ms
                    let sleep_duration = std::time::Duration::from_millis(100 * 2_u64.pow(attempt));
                    std::thread::sleep(sleep_duration);
                    continue;
                } else {
                    return Err(FsError::CopyError {
                        src: src.to_path_buf(),
                        dst: dst.to_path_buf(),
                        err,
                    });
                }
            }
        }
    }

    // This should never be reached due to the return statements above
    unreachable!()
}

/// Wrapper around the std::fs::rename command to tell us which file failed to copy
pub fn rename(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<(), FsError> {
    let src = src.as_ref();
    if !src.exists() {
        return Err(FsError::FileNotFound(src.to_path_buf()));
    }

    let dst = dst.as_ref();

    // Platform-specific behavior
    // This function currently corresponds to the rename function on Unix and the MoveFileEx function with the MOVEFILE_REPLACE_EXISTING flag on Windows.
    if cfg!(windows) {
        // If we are moving, make sure to make the parent
        if let Some(parent) = dst.parent() {
            create_dir_all(parent)?;
        }

        // copy then delete on windows :shrug:
        if src.is_file() {
            copy(src, dst)?;
            remove_file(src)
        } else {
            copy_dir_all(src, dst)?;
            remove_dir_all(src)
        }
    } else {
        match std::fs::rename(src, dst) {
            Ok(_) => Ok(()),
            Err(err) => Err(FsError::RenameError {
                src: src.to_path_buf(),
                dst: dst.to_path_buf(),
                err,
            }),
        }
    }
}

/// Wrapper around the tokio::fs::copy which makes the parent directory of the dst if it doesn't exist
pub async fn copy_mkdir(src: &Path, dst: &Path) -> Result<(), FsError> {
    if !src.exists() {
        return Err(FsError::FileNotFound(src.to_path_buf()));
    }

    if let Some(parent) = dst.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|e| FsError::DirCreateError(parent.to_path_buf(), e))?;
    }

    match tokio::fs::copy(src, dst).await {
        Ok(_) => Ok(()),
        Err(err) => Err(FsError::CopyError {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            err,
        }),
    }
}

/// Wrapper around the util::fs::create_dir_all command to tell us which file it failed on
/// creates a directory if they don't exist
pub fn create_dir_all(src: impl AsRef<Path>) -> Result<(), FsError> {
    let src = src.as_ref();

    if src.exists() {
        return Ok(());
    }

    match std::fs::create_dir_all(src) {
        Ok(_) => Ok(()),
        Err(e_source) => {
            let err = FsError::DirCreateError(src.to_path_buf(), e_source);
            log::error!("{err}");
            Err(err)
        }
    }
}

/// Wrapper around the util::fs::create_dir command to tell us which file it failed on
/// creates a directory if they don't exist
pub fn create_dir(src: impl AsRef<Path>) -> Result<(), FsError> {
    let src = src.as_ref();
    if src.exists() {
        if !src.is_dir() {
            return Err(FsError::DirCreateError(
                src.to_path_buf(),
                std::io::Error::new(
                    std::io::ErrorKind::NotADirectory,
                    "exists but is not a directory",
                ),
            ));
        }
        return Ok(());
    }

    match std::fs::create_dir(src) {
        Ok(_) => Ok(()),
        Err(err) => {
            log::error!("create_dir {src:?} {err}");
            Err(FsError::DirCreateError(src.to_path_buf(), err))
        }
    }
}

/// Wrapper around the util::fs::remove_dir_all command to tell us which file it failed on
pub fn remove_dir_all(src: impl AsRef<Path>) -> Result<(), FsError> {
    let src = src.as_ref();
    match std::fs::remove_dir_all(src) {
        Ok(_) => Ok(()),
        Err(err) => {
            log::error!("remove_dir_all {src:?} {err}");
            Err(FsError::RemoveError(src.to_path_buf(), err))
        }
    }
}

/// Wrapper around the std::fs::write command to tell us which file it failed on
pub fn write(src: impl AsRef<Path>, data: impl AsRef<[u8]>) -> Result<(), FsError> {
    let src = src.as_ref();
    match std::fs::write(src, data) {
        Ok(_) => Ok(()),
        Err(e_source) => {
            let error = FsError::FileWriteError(src.to_path_buf(), e_source);
            log::error!("{error}");
            Err(error)
        }
    }
}

/// Wrapper around the std::fs::remove_file command to tell us which file it failed on
pub fn remove_file(src: impl AsRef<Path>) -> Result<(), FsError> {
    let src = src.as_ref();
    log::debug!("Removing file: {}", src.display());
    match std::fs::remove_file(src) {
        Ok(_) => Ok(()),
        Err(e_source) => {
            let error = FsError::RemoveError(src.to_path_buf(), e_source);
            log::error!("{error}");
            Err(error)
        }
    }
}

/// Wrapper around util::fs::metadata to give us a better error on failure
pub fn metadata(path: impl AsRef<Path>) -> Result<std::fs::Metadata, OxenError> {
    let path = path.as_ref();
    match std::fs::metadata(path) {
        Ok(file) => Ok(file),
        Err(err) => {
            log::debug!("metadata {path:?} {err}");
            Err(OxenError::file_metadata_error(path, err))
        }
    }
}

/// Wrapper around std::fs::File::create to give us a better error on failure
pub fn file_create(path: impl AsRef<Path>) -> Result<std::fs::File, OxenError> {
    let path = path.as_ref();
    match std::fs::File::create(path) {
        Ok(file) => Ok(file),
        Err(err) => {
            log::error!("file_create {path:?} {err}");
            Err(OxenError::file_create_error(path, err))
        }
    }
}

/// Looks at both the extension and the first bytes of the file to determine if it is tabular
pub fn is_tabular_from_extension(data_path: impl AsRef<Path>, file_path: impl AsRef<Path>) -> bool {
    let data_path = data_path.as_ref();
    let file_path = file_path.as_ref();
    if has_ext(file_path, "json")
        && let Ok(c) = read_first_byte_from_file(data_path)
        && "[" == c.to_string()
    {
        return true;
    }

    has_tabular_extension(file_path)
}

/// Looks at the extension of the file to determine if it is tabular
pub fn has_tabular_extension(file_path: impl AsRef<Path>) -> bool {
    let file_path = file_path.as_ref();
    let exts: HashSet<String> = vec!["csv", "tsv", "parquet", "arrow", "ndjson", "jsonl"]
        .into_iter()
        .map(String::from)
        .collect();
    contains_ext(file_path, &exts)
}

pub fn is_tabular(path: &Path) -> bool {
    is_tabular_from_extension(path, path)
}

pub fn is_image(path: &Path) -> bool {
    let exts: HashSet<String> = vec!["jpg", "png"].into_iter().map(String::from).collect();
    contains_ext(path, &exts)
}

pub fn is_markdown(path: &Path) -> bool {
    let exts: HashSet<String> = vec!["md"].into_iter().map(String::from).collect();
    contains_ext(path, &exts)
}

pub fn is_video(path: &Path) -> bool {
    let exts: HashSet<String> = vec!["mp4"].into_iter().map(String::from).collect();
    contains_ext(path, &exts)
}

pub fn is_audio(path: &Path) -> bool {
    let exts: HashSet<String> = vec!["mp3", "wav"].into_iter().map(String::from).collect();
    contains_ext(path, &exts)
}

pub fn is_utf8(path: &Path) -> bool {
    const SAMPLE_SIZE: usize = 4096;

    let bytes = match read_first_n_bytes(path, SAMPLE_SIZE) {
        Ok(b) => b,
        Err(_) => return false,
    };

    if bytes.is_empty() {
        return true;
    }

    match from_utf8(&bytes) {
        Ok(_) => true,
        Err(e) => e.error_len().is_none(),
    }
}

pub fn data_type_from_extension(path: &Path) -> EntryDataType {
    let ext = path.extension().unwrap_or_default().to_string_lossy();
    match ext.as_ref() {
        "json" => EntryDataType::Tabular,
        "csv" => EntryDataType::Tabular,
        "tsv" => EntryDataType::Tabular,
        "parquet" => EntryDataType::Tabular,
        "arrow" => EntryDataType::Tabular,
        "ndjson" => EntryDataType::Tabular,
        "jsonl" => EntryDataType::Tabular,

        "md" => EntryDataType::Text,
        "txt" => EntryDataType::Text,
        "html" => EntryDataType::Text,
        "xml" => EntryDataType::Text,
        "yaml" => EntryDataType::Text,
        "yml" => EntryDataType::Text,
        "toml" => EntryDataType::Text,

        "png" => EntryDataType::Image,
        "jpg" => EntryDataType::Image,
        "jpeg" => EntryDataType::Image,
        "gif" => EntryDataType::Image,
        "bmp" => EntryDataType::Image,
        "tiff" => EntryDataType::Image,
        "heic" => EntryDataType::Image,
        "heif" => EntryDataType::Image,
        "webp" => EntryDataType::Image,

        "mp4" => EntryDataType::Video,
        "avi" => EntryDataType::Video,
        "mov" => EntryDataType::Video,

        "mp3" => EntryDataType::Audio,
        "wav" => EntryDataType::Audio,
        "aac" => EntryDataType::Audio,
        "ogg" => EntryDataType::Audio,
        "flac" => EntryDataType::Audio,
        "opus" => EntryDataType::Audio,

        _ => EntryDataType::Binary,
    }
}

pub fn file_mime_type(path: &Path) -> String {
    file_mime_type_from_extension(path, path)
}

// We have this split out because we get the mime type from the extension
// but the data type from the contents
// and the version path does not always have the extension in newer versions of oxen
pub fn file_mime_type_from_extension(data_path: &Path, file_path: &Path) -> String {
    match infer::get_from_path(data_path) {
        Ok(Some(kind)) => {
            // log::debug!("file_mime_type {:?} {}", data_path, kind.mime_type());
            String::from(kind.mime_type())
        }
        _ => {
            if is_markdown(file_path) {
                String::from("text/markdown")
            } else if is_utf8(data_path) {
                String::from("text/plain")
            } else if data_path.is_dir() {
                String::from("inode/directory")
            } else {
                // https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types
                // application/octet-stream is the default value for all other cases.
                // An unknown file type should use this type.
                // Browsers are particularly careful when manipulating these files to
                // protect users from software vulnerabilities and possible dangerous behavior.
                String::from("application/octet-stream")
            }
        }
    }
}

pub fn datatype_from_mimetype(data_path: &Path, mime_type: &str) -> EntryDataType {
    datatype_from_mimetype_from_extension(data_path, data_path, mime_type)
}

// We have this split out because we get the mime type from the extension
// but the data type from the contents
// and the version path does not always have the extension in newer versions of oxen
pub fn datatype_from_mimetype_from_extension(
    data_path: &Path,
    file_path: &Path,
    mime_type: &str,
) -> EntryDataType {
    match mime_type {
        // Image
        "image/jpeg" => EntryDataType::Image,
        "image/png" => EntryDataType::Image,
        "image/gif" => EntryDataType::Image,
        "image/webp" => EntryDataType::Image,
        "image/x-canon-cr2" => EntryDataType::Image,
        "image/tiff" => EntryDataType::Image,
        "image/bmp" => EntryDataType::Image,
        "image/heif" => EntryDataType::Image,
        "image/avif" => EntryDataType::Image,

        // Video
        "video/mp4" => EntryDataType::Video,
        "video/x-m4v" => EntryDataType::Video,
        "video/x-msvideo" => EntryDataType::Video,
        "video/quicktime" => EntryDataType::Video,
        "video/mpeg" => EntryDataType::Video,
        "video/webm" => EntryDataType::Video,
        "video/x-matroska" => EntryDataType::Video,
        "video/x-flv" => EntryDataType::Video,
        "video/x-ms-wmv" => EntryDataType::Video,

        // Audio
        "audio/midi" => EntryDataType::Audio,
        "audio/mpeg" => EntryDataType::Audio,
        "audio/m4a" => EntryDataType::Audio,
        "audio/x-wav" => EntryDataType::Audio,
        "audio/ogg" => EntryDataType::Audio,
        "audio/x-flac" => EntryDataType::Audio,
        "audio/aac" => EntryDataType::Audio,
        "audio/x-aiff" => EntryDataType::Audio,
        "audio/x-dsf" => EntryDataType::Audio,
        "audio/x-ape" => EntryDataType::Audio,

        mime_type => {
            // log::debug!(
            //     "datatype_from_mimetype trying to infer {:?} {:?} {}",
            //     data_path,
            //     file_path,
            //     mime_type
            // );
            // Catch text and dataframe types from file extension
            if is_tabular_from_extension(data_path, file_path) {
                EntryDataType::Tabular
            } else if mime_type.starts_with("text/") {
                EntryDataType::Text
            } else {
                // split on the first half of the mime type to fall back to audio, video, image
                let mime_type = mime_type.split('/').next().unwrap_or("");
                match mime_type {
                    "audio" => EntryDataType::Audio,
                    "video" => EntryDataType::Video,
                    "image" => EntryDataType::Image,
                    _ => EntryDataType::Binary,
                }
            }
        }
    }
}

pub fn file_data_type(path: &Path) -> EntryDataType {
    let mimetype = file_mime_type(path);
    datatype_from_mimetype(path, mimetype.as_str())
}

pub fn file_extension(path: &Path) -> String {
    match path.extension() {
        Some(extension) => match extension.to_str() {
            Some(ext) => ext.to_string(),
            None => "".to_string(),
        },
        None => "".to_string(),
    }
}

pub fn contains_ext(path: &Path, exts: &HashSet<String>) -> bool {
    match path.extension() {
        Some(extension) => match extension.to_str() {
            Some(ext) => exts.contains(ext.to_lowercase().as_str()),
            None => false,
        },
        None => false,
    }
}

pub fn has_ext(path: &Path, ext: &str) -> bool {
    match path.extension() {
        Some(extension) => extension == ext,
        None => false,
    }
}

pub fn replace_file_name_keep_extension(path: &Path, new_filename: String) -> PathBuf {
    let mut result = path.to_owned();
    result.set_file_name(new_filename);
    if let Some(extension) = path.extension() {
        result.set_extension(extension);
    }
    result
}

pub fn recursive_files_with_extensions(dir: &Path, exts: &HashSet<String>) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = vec![];
    if !dir.is_dir() {
        return files;
    }

    for entry in WalkDir::new(dir) {
        match entry {
            Ok(val) => {
                let path = val.path();
                if contains_ext(&path, exts) {
                    files.push(path);
                }
            }
            Err(err) => {
                eprintln!("recursive_files_with_extensions Could not iterate over dir... {err}")
            }
        }
    }
    files
}

pub fn count_files_in_dir(dir: &Path) -> usize {
    let mut count: usize = 0;
    if dir.is_dir() {
        match std::fs::read_dir(dir) {
            Ok(entries) => {
                for entry in entries {
                    match entry {
                        Ok(entry) => {
                            let path = entry.path();
                            if !is_in_oxen_hidden_dir(&path) && path.is_file() {
                                count += 1;
                            }
                        }
                        Err(err) => log::warn!("error reading dir entry: {err}"),
                    }
                }
            }
            Err(err) => log::warn!("error reading dir: {err}"),
        }
    }
    count
}

pub fn count_items_in_dir(dir: &Path) -> usize {
    let mut count: usize = 0;
    if dir.is_dir() {
        match std::fs::read_dir(dir) {
            Ok(entries) => {
                for entry in entries {
                    match entry {
                        Ok(entry) => {
                            let path = entry.path();
                            if !is_in_oxen_hidden_dir(&path) {
                                count += 1;
                            }
                        }
                        Err(err) => log::warn!("error reading dir entry: {err}"),
                    }
                }
            }
            Err(err) => log::warn!("error reading dir: {err}"),
        }
    }
    count
}

pub fn rcount_files_in_dir(dir: &Path) -> usize {
    let mut count: usize = 0;
    if !dir.is_dir() {
        return count;
    }

    for entry in WalkDir::new(dir) {
        match entry {
            Ok(val) => {
                let path = val.path();
                // if it's not the hidden oxen dir and is not a directory
                if !is_in_oxen_hidden_dir(&path) && !path.is_dir() {
                    // log::debug!("Found file {count}: {:?}", path);
                    count += 1;
                }
            }
            Err(err) => eprintln!("rcount_files_in_dir Could not iterate over dir... {err}"),
        }
    }
    count
}

pub fn rlist_files_in_dir(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = vec![];
    if !dir.is_dir() {
        return files;
    }

    for entry in WalkDir::new(dir) {
        match entry {
            Ok(val) => {
                let path = val.path();
                // if it's not the hidden oxen dir and is not a directory
                if !is_in_oxen_hidden_dir(&path) && !path.is_dir() {
                    // log::debug!("Found file {count}: {:?}", path);
                    files.push(path);
                }
            }
            Err(err) => eprintln!("rcount_files_in_dir Could not iterate over dir... {err}"),
        }
    }
    files
}

pub fn is_in_oxen_hidden_dir(path: &Path) -> bool {
    for component in path.components() {
        if let Some(path_str) = component.as_os_str().to_str()
            && path_str.eq(constants::OXEN_HIDDEN_DIR)
        {
            return true;
        }
    }
    false
}

pub fn is_canonical(path: impl AsRef<Path>) -> Result<bool, OxenError> {
    let path = path.as_ref();
    let canon_path = canonicalize(path)?;

    if path == canon_path {
        log::debug!("path {path:?} IS canonical");
        return Ok(true);
    }

    log::debug!("path {path:?} is NOT canonical");
    Ok(false)
}

// Return canonicalized path if possible. Falls back to converting to an absolute path without
// symlink resolution, which is needed on filesystems that don't support canonicalization (e.g.
// Windows imdisk ramdisks).
pub fn canonicalize(path: impl AsRef<Path>) -> Result<PathBuf, OxenError> {
    let path = path.as_ref();
    match dunce::canonicalize(path) {
        Ok(canon_path) => Ok(canon_path),
        Err(e)
            if e.kind() == std::io::ErrorKind::Unsupported
                // On Windows, ERROR_INVALID_FUNCTION (os error 1) from ramdisk drivers maps
                // to Uncategorized rather than Unsupported, so also check the raw code.
                || (cfg!(windows) && e.raw_os_error() == Some(1)) =>
        {
            // Fallback: convert to absolute path without symlink resolution. This is needed on
            // filesystems whose drivers don't implement canonicalization (e.g. Windows imdisk
            // ramdisks).
            if path.is_absolute() {
                Ok(path.to_path_buf())
            } else {
                Ok(std::path::absolute(path)?)
            }
        }
        Err(e) => Err(OxenError::basic_str(format!(
            "path {path:?} cannot be canonicalized: {e}"
        ))),
    }
}

// Get the full path of a non-canonical parent from a canonical child path
pub fn full_path_from_child_path(
    parent: impl AsRef<Path>,
    child: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let parent_path = parent.as_ref();
    let child_path = child.as_ref();

    let parent_stem = stem_from_canonical_child_path(parent_path, child_path)?;
    Ok(parent_stem.join(parent_path))
}

// Find the stem that completes the absolute path of the parent from its child
// This is useful to find the absolute path of a repo when directly canonicalizing its path isn't possible, and we're calling add with an absolute path
// If the file is under the oxen control of the repo, then this will recover the necessary stem to get the correct canonical path to that repo
fn stem_from_canonical_child_path(
    parent_path: impl AsRef<Path>,
    child_path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let parent_path = parent_path.as_ref();
    let child_path = child_path.as_ref();

    let child_components: Vec<Component> = child_path.components().collect();
    let parent_components: Vec<Component> = parent_path.components().collect();

    let relative_path = path_relative_to_dir(child_path, parent_path)?;
    let relative_components: Vec<Component> = relative_path.components().collect();

    if child_components.len() < parent_components.len() + relative_components.len() {
        return Err(OxenError::basic_str(format!(
            "Invalid path relationship: child path {child_path:?} is not under parent path {parent_path:?}"
        )));
    }

    let ending_index = child_components.len() - relative_components.len() - parent_components.len();
    let path_slice = &child_components[..ending_index];
    let result: PathBuf = path_slice.iter().collect();
    Ok(result)
}

pub fn path_relative_to_dir(
    path: impl AsRef<Path>,
    dir: impl AsRef<Path>,
) -> Result<PathBuf, FsError> {
    let path = path.as_ref();
    let dir = dir.as_ref();

    // log::debug!("path_relative_to_dir starting path: {path:?}");
    // log::debug!("path_relative_to_dir staring dir: {dir:?}");

    // Split paths into components
    let path_components: Vec<Component> = path.components().collect();
    let dir_components: Vec<Component> = dir.components().collect();

    if path_components.is_empty() || dir == path {
        return Ok(PathBuf::new());
    }

    if dir_components.is_empty() || dir_components.len() > path_components.len() {
        // Iterate through the components instead of returning original to normalize path for windows
        let mut result = PathBuf::new();
        let path_slice = &path_components;

        // Adjust for special paths like '.', '..', etc
        for component in path_slice.iter() {
            if matches!(component, Component::Normal(_)) {
                result.push::<&Path>(component.as_ref());
            }
        }

        return Ok(result);
    }

    // Get iterators for the component vectors
    let mut path_iter = path_components.iter();
    let mut dir_iter = dir_components.iter();
    let starting_dir_iter = dir_iter.clone();

    let mut dir_component = dir_iter.next().unwrap();
    let mut matches = 0;
    let mut start_index = 0;

    for _ in 0..(path_components.len()) {
        let Some(path_component) = path_iter.next() else {
            return Err(FsError::PathBoundsViolation);
        };
        let path_str = path_component.as_os_str();
        let dir_str = dir_component.as_os_str();

        if path_str == dir_str {
            matches += 1;
            if matches == dir_components.len() {
                let result = {
                    let mut result = PathBuf::new();
                    let path_slice = &path_components[(start_index + 1)..];

                    // Adjust for special paths like '.', '..', etc
                    for component in path_slice.iter() {
                        if matches!(component, Component::Normal(_)) {
                            result.push::<&Path>(component.as_ref());
                        }
                    }
                    result
                };
                // log::debug!("result: {result:?}");
                return Ok(result);
            }
            start_index += 1;
            dir_component = match dir_iter.next() {
                Some(updated) => updated,
                None => return Err(FsError::DirBoundsViolation),
            };
            continue;
        }

        // Check length first as an optimization
        if path_str.len() == dir_str.len() {
            // On Windows, if the components don't match, it may be because of casing inconsistency
            // So, if the raw components don't match, convert them to lowercase strings
            let path_lower = path_str.to_string_lossy().to_lowercase();
            let dir_lower = dir_str.to_string_lossy().to_lowercase();

            if path_lower == dir_lower {
                matches += 1;
                if matches == dir_components.len() {
                    let mut result = PathBuf::new();
                    let path_slice = &path_components[(start_index + 1)..];

                    for component in path_slice.iter() {
                        if matches!(component, Component::Normal(_)) {
                            result.push::<&Path>(component.as_ref());
                        }
                    }
                    // log::debug!("result: {result:?}");
                    return Ok(result);
                }
                start_index += 1;
                dir_component = dir_iter.next().expect("Dir bounds violated");
                continue;
            }
        }

        // If the components don't match, reset dir_iter and dir_component
        dir_iter = starting_dir_iter.clone();
        dir_component = dir_iter.next().unwrap();
        start_index += 1;
    }

    // If the loop finishes, the path cannot be found relative to the dir
    // Returning the original path is the expected behavior
    let mut result = PathBuf::new();
    let path_slice = &path_components;

    for component in path_slice.iter() {
        result.push::<&Path>(component.as_ref());
    }
    Ok(result)
}

// Check whether a path can be found relative to a dir
pub fn is_relative_to_dir(path: impl AsRef<Path>, dir: impl AsRef<Path>) -> bool {
    let path = path.as_ref();
    let dir = dir.as_ref();

    let path_components: Vec<Component> = path.components().collect();
    let dir_components: Vec<Component> = dir.components().collect();

    if path_components.is_empty() || dir == path {
        return true;
    }

    if dir_components.is_empty() || dir_components.len() > path_components.len() {
        return false;
    }

    // Get iterators for the component vectors
    let mut path_iter = path_components.iter();
    let mut dir_iter = dir_components.iter();
    let starting_dir_iter = dir_iter.clone();

    let mut dir_component = dir_iter.next().unwrap();
    let mut matches = 0;

    for _ in 0..(path_components.len()) {
        let path_component = path_iter.next().expect("Path bounds violated");
        let path_str = path_component.as_os_str();
        let dir_str = dir_component.as_os_str();

        if path_str == dir_str {
            matches += 1;
            if matches == dir_components.len() {
                return true;
            }
            dir_component = dir_iter.next().expect("Dir bounds violated");
            continue;
        }

        if path_str.len() == dir_str.len() {
            let path_lower = path_str.to_string_lossy().to_lowercase();
            let dir_lower = dir_str.to_string_lossy().to_lowercase();

            if path_lower == dir_lower {
                matches += 1;
                if matches == dir_components.len() {
                    return true;
                }
                dir_component = dir_iter.next().expect("Dir bounds violated");
                continue;
            }
        }

        // If the components don't match, reset dir_iter and dir_component
        dir_iter = starting_dir_iter.clone();
        dir_component = dir_iter.next().unwrap();
        matches = 0;
    }

    // If the loop finishes, the path cannot be found relative to the dir
    false
}

pub fn linux_path_str(string: &str) -> String {
    // Convert string to bytes, replacing '\\' with '/' if necessary
    let bytes = string.as_bytes();
    let mut new_bytes: Vec<u8> = Vec::new();
    for byte in bytes.iter() {
        if *byte == b'\\' {
            new_bytes.push(b'/');
        } else {
            new_bytes.push(*byte);
        }
    }
    String::from_utf8(new_bytes).unwrap()
}

pub fn linux_path(path: &Path) -> PathBuf {
    // Convert string to bytes, replacing '\\' with '/' if necessary
    let string = path.to_str().unwrap();
    Path::new(&linux_path_str(string)).to_path_buf()
}

pub fn remove_leading_slash(path: &Path) -> PathBuf {
    let mut components = path.components();

    // If the first component of the path is '/', skip it and reconstruct the path
    if components.next() == Some(std::path::Component::RootDir) {
        components.collect()
    } else {
        path.to_path_buf()
    }
}

pub fn disk_usage_for_path(path: &Path) -> Result<DiskUsage, OxenError> {
    log::debug!("disk_usage_for_path: {path:?}");
    let disks = sysinfo::Disks::new_with_refreshed_list();

    if disks.is_empty() {
        return Err(OxenError::basic_str("No disks found"));
    }

    // try to choose the disk that the path is on
    let mut selected_disk = disks.first().unwrap();
    for disk in &disks {
        let disk_mount_len = disk.mount_point().to_str().unwrap_or_default().len();
        let selected_disk_mount_len = selected_disk
            .mount_point()
            .to_str()
            .unwrap_or_default()
            .len();

        // pick the disk with the longest mount point that is a prefix of the path
        if path.starts_with(disk.mount_point()) && disk_mount_len > selected_disk_mount_len {
            selected_disk = disk;
            break;
        }
    }

    log::debug!("disk_usage_for_path selected disk: {selected_disk:?}");
    let total_gb = selected_disk.total_space() as f64 / bytesize::GB as f64;
    let free_gb = selected_disk.available_space() as f64 / bytesize::GB as f64;
    let used_gb = total_gb - free_gb;
    let percent_used = used_gb / total_gb;

    Ok(DiskUsage {
        total_gb,
        used_gb,
        free_gb,
        percent_used,
    })
}
pub fn open_file(path: impl AsRef<Path>) -> Result<File, OxenError> {
    match File::open(path.as_ref()) {
        Ok(file) => Ok(file),
        Err(err) => Err(OxenError::basic_str(format!(
            "Failed to open file: {:?}\n{:?}",
            path.as_ref(),
            err
        ))),
    }
}

async fn detect_image_format_from_version(
    versioned_store: Arc<dyn VersionStore>,
    hash: &str,
) -> Result<ImageFormat, OxenError> {
    let mut stream = versioned_store.get_version_stream(hash).await?;

    let mut header = Vec::with_capacity(16);
    while header.len() < 16 {
        if let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| {
                OxenError::basic_str(format!("Failed to read version file {hash}: {e}"))
            })?;
            let to_take = 16 - header.len();
            header.extend_from_slice(&chunk[..to_take.min(chunk.len())]);
        } else {
            break; // EOF
        }
    }

    if header.is_empty() {
        return Err(OxenError::basic_str(format!(
            "Version file {hash} is empty"
        )));
    }

    let format = image::guess_format(&header)
        .map_err(|_| OxenError::basic_str(format!("Unknown image format for version: {hash}")))?;

    Ok(format)
}

pub async fn resize_cache_image_version_store(
    version_store: Arc<dyn VersionStore>,
    img_hash: &str,
    derived_filename: &OsStr,
    resize: ImgResize,
) -> Result<
    (
        Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>,
        u64,
    ),
    OxenError,
> {
    let derived_filename_s = derived_filename.to_string_lossy().replace("\u{FFFD}", "");

    if version_store
        .derived_version_exists(img_hash, &derived_filename_s)
        .await?
    {
        log::debug!("In the resize cache! {derived_filename_s}");
        let content_length = version_store
            .get_version_derived_size(img_hash, &derived_filename_s)
            .await?;
        let stream = version_store
            .get_version_derived_stream(img_hash, &derived_filename_s)
            .await?;
        return Ok((stream.boxed(), content_length));
    }

    log::debug!("create resized image {derived_filename_s} from hash {img_hash}");
    let image_format = detect_image_format_from_version(Arc::clone(&version_store), img_hash).await;
    let image_data = version_store.get_version(img_hash).await?;

    let img = match image_format {
        Ok(format) => {
            let reader = Cursor::new(&image_data);
            image::load(reader, format)?
        }
        Err(_) => {
            log::debug!("Could not detect image format, opening file without format");
            let reader = Cursor::new(&image_data);

            ImageReader::new(reader).with_guessed_format()?.decode()?
        }
    };

    let resized_img = if let Some(resize_width) = resize.width
        && let Some(resize_height) = resize.height
    {
        img.resize_exact(
            resize_width,
            resize_height,
            image::imageops::FilterType::Lanczos3,
        )
    } else if let Some(resize_width) = resize.width {
        img.resize(
            resize_width,
            resize_width,
            image::imageops::FilterType::Lanczos3,
        )
    } else if let Some(resize_height) = resize.height {
        img.resize(
            resize_height,
            resize_height,
            image::imageops::FilterType::Lanczos3,
        )
    } else {
        img
    };

    let image_format = ImageFormat::from_path(derived_filename)?;
    let buf = {
        let mut buf = Vec::new();
        resized_img.write_to(&mut Cursor::new(&mut buf), image_format)?;
        buf
    };
    version_store
        .store_version_derived(img_hash, &derived_filename_s, &buf)
        .await?;
    let content_length = buf.len() as u64;

    let stream: Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> =
        futures::stream::once(async move { Ok(Bytes::from(buf)) }).boxed();

    Ok((stream, content_length))
}

/// Generate a video thumbnail using thumbnails crate.
/// This function extracts a frame from the video and saves it as an image thumbnail.
#[cfg(feature = "ffmpeg")]
async fn generate_video_thumbnail_version_store(
    version_store: Arc<dyn VersionStore>,
    video_hash: &str,
    derived_filename: &str,
    thumbnail: VideoThumbnail,
) -> Result<(), OxenError> {
    log::debug!(
        "generate thumbnail derived_filename {derived_filename} from video hash {video_hash}"
    );
    if version_store
        .derived_version_exists(video_hash, derived_filename)
        .await?
    {
        return Ok(());
    }

    // Get the video file path from version store
    let version_path = version_store.get_version_path(video_hash).await?;

    // Determine output dimensions
    // The thumbnails crate maintains aspect ratio, so we use max dimensions
    let (output_width, output_height) = match (thumbnail.width, thumbnail.height) {
        (Some(w), Some(h)) => (w, h),
        (Some(w), None) => (w, w), // Use width for both if only width specified
        (None, Some(h)) => (h, h), // Use height for both if only height specified
        (None, None) => (320, 240),
    };

    // Note: The thumbnails crate doesn't support timestamp selection directly.
    // It extracts from the beginning of the video.
    // If timestamp support is needed, we may need to use ffmpeg directly or another approach.
    let _timestamp = thumbnail.timestamp.unwrap_or(1.0);

    // Run blocking ffmpeg thumbnail generation on a separate thread
    let buf = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, OxenError> {
        // Create thumbnailer with specified dimensions
        let thumbnailer = Thumbnailer::new(output_width, output_height);

        // Generate thumbnail from video file
        let thumb_image = thumbnailer
            .get(&version_path)
            .map_err(|e| OxenError::basic_str(format!("Failed to generate thumbnail: {e}.")))?;

        let mut buf = Vec::new();
        thumb_image
            .write_to(&mut Cursor::new(&mut buf), image::ImageFormat::Jpeg)
            .map_err(|e| OxenError::basic_str(format!("Failed to encode thumbnail: {e}")))?;
        Ok(buf)
    })
    .await??;

    // Save the thumbnail image
    version_store
        .store_version_derived(video_hash, derived_filename, &buf)
        .await?;

    log::debug!("saved thumbnail {derived_filename}");
    Ok(())
}

/// Handle video thumbnail generation: generate thumbnail if needed and return a stream.
/// Only enabled if the 'ffmpeg' feature is enabled.
#[allow(unused_variables)]
pub async fn handle_video_thumbnail(
    version_store: Arc<dyn VersionStore>,
    file_hash: String,
    video_thumbnail: VideoThumbnail,
) -> Result<Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>>, OxenError> {
    #[cfg(not(feature = "ffmpeg"))]
    {
        let _ = (version_store, file_hash, video_thumbnail);
        Err(OxenError::ThumbnailingNotEnabled)
    }

    #[cfg(feature = "ffmpeg")]
    {
        log::debug!("video_thumbnail {video_thumbnail:?}");
        let derived_filename = video_thumbnail_filename(
            video_thumbnail.width,
            video_thumbnail.height,
            video_thumbnail.timestamp,
        );

        generate_video_thumbnail_version_store(
            version_store.clone(),
            &file_hash,
            &derived_filename,
            video_thumbnail,
        )
        .await?;

        log::debug!("In the thumbnail cache! {derived_filename}");
        let stream = version_store
            .get_version_derived_stream(&file_hash, &derived_filename)
            .await?;
        Ok(stream.boxed())
    }
}

pub fn to_unix_str(path: impl AsRef<Path>) -> String {
    path.as_ref()
        .to_str()
        .unwrap_or_default()
        .replace('\\', "/")
}

pub fn is_glob_path(path: impl AsRef<Path>) -> bool {
    let glob_chars = ['*', '?', '[', ']'];
    glob_chars
        .iter()
        .any(|c| path.as_ref().to_str().unwrap_or_default().contains(*c))
}

pub fn remove_paths(src: &Path) -> Result<(), FsError> {
    if src.is_dir() {
        log::debug!("Calling remove_dir_all: {src:?}");
        remove_dir_all(src)
    } else {
        log::debug!("Calling remove_file: {src:?}");
        remove_file(src)
    }
}

pub fn is_modified_from_node_with_metadata(
    path: &Path,
    node: &FileNode,
    metadata: Result<std::fs::Metadata, OxenError>,
) -> Result<bool, OxenError> {
    if !path.exists() {
        log::debug!("is_modified_from_node found non-existent path {path:?}. Returning false");
        return Ok(false);
    }

    let metadata = metadata?;
    // Second, check the length of the file
    let file_size = metadata.len();
    let node_size = node.num_bytes();

    if file_size != node_size {
        return Ok(true);
    }

    // Third, check the last modified times
    let file_last_modified = FileTime::from_last_modification_time(&metadata);
    let node_last_modified = util::fs::last_modified_time(
        node.last_modified_seconds(),
        node.last_modified_nanoseconds(),
    );

    if file_last_modified == node_last_modified {
        return Ok(false);
    }

    // Fourth, check the metadata hashes
    let node_metadata_hash = node.metadata_hash();
    let file_metadata_hash = {
        let mime_type = util::fs::file_mime_type(path);
        let data_type = util::fs::datatype_from_mimetype(path, mime_type.as_str());

        let file_metadata = repositories::metadata::get_file_metadata(path, &data_type)?;
        util::hasher::maybe_get_metadata_hash(&file_metadata)?
    };

    if node_metadata_hash.is_some()
        && file_metadata_hash.is_some()
        && *node_metadata_hash.unwrap() != MerkleHash::new(file_metadata_hash.unwrap())
    {
        return Ok(true);
    }

    // Finally, check the hashes
    let node_hash = node.hash().to_u128();
    let working_hash = util::hasher::get_hash_given_metadata(path, &metadata)?;

    if node_hash == working_hash {
        Ok(false)
    } else {
        Ok(true)
    }
}

pub fn is_modified_from_node(path: &Path, node: &FileNode) -> Result<bool, OxenError> {
    is_modified_from_node_with_metadata(path, node, util::fs::metadata(path))
}

// Calculate a node's last modified time

pub fn last_modified_time(last_modified_seconds: i64, last_modified_nanoseconds: u32) -> FileTime {
    let node_modified_nanoseconds = std::time::SystemTime::UNIX_EPOCH
        + std::time::Duration::from_secs(last_modified_seconds as u64)
        + std::time::Duration::from_nanos(last_modified_nanoseconds as u64);

    FileTime::from_system_time(node_modified_nanoseconds)
}

/// Validates and normalizes a user-provided path to ensure it is safe.
/// Returns the normalized path if valid, or an OxenError describing the issue.
///
/// Validation rules:
/// - Must be a relative path (no absolute paths or root components)
/// - Cannot contain parent directory references (..)
/// - Cannot contain empty segments
/// - Current directory references (.) are skipped
pub fn validate_and_normalize_path(path: impl AsRef<Path>) -> Result<PathBuf, OxenError> {
    let path = path.as_ref();

    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(segment) => {
                let segment_str = segment.to_string_lossy();
                // Reject empty segments (e.g., from "foo//bar")
                if segment_str.is_empty() {
                    return Err(OxenError::basic_str("path contains empty segments"));
                }
                normalized.push(segment);
            }
            Component::ParentDir => {
                return Err(OxenError::basic_str(
                    "path cannot contain parent directory references (..)",
                ));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(OxenError::basic_str("path must be relative, not absolute"));
            }
            Component::CurDir => {
                // Skip "." components (current directory)
            }
        }
    }

    // Ensure we have a valid path after normalization
    if normalized.as_os_str().is_empty() {
        return Err(OxenError::basic_str(
            "path resolves to empty after normalization",
        ));
    }

    Ok(normalized)
}

/// Unpack an async-tar archive to a destination directory without calling `canonicalize`. This is
/// needed because `archive.unpack()` and `entry.unpack_in()` internally call
/// `std::fs::canonicalize`, which fails on filesystems that don't support it (e.g. Windows imdisk
/// ramdisks). Path traversal is checked by rejecting parent components.
pub async fn unpack_async_tar_archive<R: futures_util::AsyncRead + Unpin>(
    archive: async_tar::Archive<R>,
    dst: &Path,
) -> Result<(), crate::error::OxenError> {
    create_dir_all(dst)?;

    let mut entries = archive.entries()?;
    while let Some(entry) = entries.next().await {
        let mut file = entry?;
        let path = file.path()?.to_path_buf();

        let entry_type = file.header().entry_type();
        if !entry_type.is_file() && !entry_type.is_dir() {
            return Err(crate::error::OxenError::internal_error(format!(
                "Unsupported archive entry type for {}: only regular files and directories \
                 are allowed",
                path.display()
            )));
        }

        let mut file_dst = dst.to_path_buf();
        for part in path.components() {
            match part {
                Component::Normal(part) => file_dst.push(part),
                Component::ParentDir => {
                    return Err(crate::error::OxenError::internal_error(format!(
                        "Path traversal detected in archive entry: {}",
                        path.display()
                    )));
                }
                _ => continue,
            }
        }

        // Skip empty paths (e.g. entries that were only "." or "/")
        if file_dst == dst {
            continue;
        }

        if entry_type.is_dir() {
            create_dir_all(&file_dst)?;
        } else {
            if let Some(parent) = file_dst.parent() {
                create_dir_all(parent)?;
            }
            file.unpack(&file_dst).await.map_err(|e| {
                crate::error::OxenError::basic_str(format!(
                    "Failed to unpack {}: {e}",
                    file_dst.display()
                ))
            })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::model::EntryDataType;
    use crate::test;
    use crate::util;

    use std::path::Path;

    #[test]
    fn file_path_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data")
            .join("test")
            .join("other")
            .join("file.txt");
        let dir = Path::new("data").join("test");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("other").join("file.txt"));

        Ok(())
    }

    #[test]
    fn file_path_2_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data")
            .join("test")
            .join("other")
            .join("file.txt");
        let dir = Path::new("data").join("test").join("other");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("file.txt"));

        Ok(())
    }

    #[test]
    fn file_path_3_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data")
            .join("test")
            .join("runs")
            .join("54321")
            .join("file.txt");
        let dir = Path::new("data").join("test").join("runs").join("54321");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("file.txt"));

        Ok(())
    }

    #[test]
    fn full_file_path_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data")
            .join("test")
            .join("other")
            .join("file.txt");
        let dir = Path::new("data").join("test").join("other");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("file.txt"));

        Ok(())
    }

    #[test]
    fn dir_path_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data").join("test").join("other");
        let dir = Path::new("data").join("test");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("other"));

        Ok(())
    }

    #[test]
    fn dir_path_relative_to_another_dir() -> Result<(), OxenError> {
        let file = Path::new("data").join("test").join("other").join("dir");
        let dir = Path::new("data").join("test");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("other").join("dir"));

        Ok(())
    }

    #[test]
    fn path_relative_to_unrelated_dir() -> Result<(), OxenError> {
        let file = Path::new("data").join("test").join("other").join("dir");
        let dir = Path::new("some").join("repo");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(
            relative,
            Path::new("data").join("test").join("other").join("dir")
        );

        Ok(())
    }

    #[test]
    fn detect_file_type() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let python_file = "add_1.py";
            let python_with_interpreter_file = "add_2.py";

            test::write_txt_file_to_path(
                repo.path.join(python_file),
                r"import os


def add(a, b):
    return a + b",
            )?;

            test::write_txt_file_to_path(
                repo.path.join(python_with_interpreter_file),
                r"#!/usr/bin/env python3
import os


def add(a, b):
    return a + b",
            )?;

            assert_eq!(
                EntryDataType::Text,
                util::fs::file_data_type(&repo.path.join(python_file))
            );

            assert_eq!(
                EntryDataType::Text,
                util::fs::file_data_type(&repo.path.join(python_with_interpreter_file))
            );

            assert_eq!(
                EntryDataType::Tabular,
                util::fs::file_data_type(
                    &repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("bounding_box.csv")
                )
            );
            assert_eq!(
                EntryDataType::Text,
                util::fs::file_data_type(
                    &repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("annotations.txt")
                )
            );

            let test_id_file = repo.path.join("test_id.txt");
            let test_id_file_no_ext = repo.path.join("test_id");
            util::fs::copy(
                test::REPO_ROOT
                    .join("data")
                    .join("test")
                    .join("text")
                    .join("test_id.txt"),
                &test_id_file,
            )?;
            util::fs::copy(
                test::REPO_ROOT
                    .join("data")
                    .join("test")
                    .join("text")
                    .join("test_id.txt"),
                &test_id_file_no_ext,
            )?;

            assert_eq!(EntryDataType::Text, util::fs::file_data_type(&test_id_file));
            assert_eq!(
                EntryDataType::Text,
                util::fs::file_data_type(&test_id_file_no_ext)
            );
            assert_eq!(
                EntryDataType::Image,
                util::fs::file_data_type(&repo.path.join("test").join("1.jpg"))
            );

            Ok(())
        })
    }

    #[test]
    fn detect_file_type_json_array() -> Result<(), OxenError> {
        test::run_empty_dir_test(|_| {
            assert_eq!(
                EntryDataType::Tabular,
                util::fs::file_data_type(
                    test::REPO_ROOT
                        .join("data")
                        .join("test")
                        .join("json")
                        .join("tabular.json")
                        .as_path()
                )
            );

            Ok(())
        })
    }

    #[test]
    fn replace_file_name_keep_extension_no_extension() -> Result<(), OxenError> {
        let prior_path = Path::new("adjfkaljeklwjkljdaklfd.txt");
        let prior_path_no_extension = Path::new("bdsfadfklajfkelj");
        let prior_path_arbitrary = Path::new("jdakfljdfskl.boom");

        let new_filename = "data".to_string();
        assert_eq!(
            util::fs::replace_file_name_keep_extension(prior_path, new_filename.clone()),
            Path::new("data.txt")
        );

        assert_eq!(
            util::fs::replace_file_name_keep_extension(
                prior_path_no_extension,
                new_filename.clone()
            ),
            Path::new("data")
        );

        assert_eq!(
            util::fs::replace_file_name_keep_extension(prior_path_arbitrary, new_filename),
            Path::new("data.boom")
        );

        Ok(())
    }

    #[test]
    fn to_unix_str() {
        assert_eq!(
            util::fs::to_unix_str(Path::new("data\\test\\file.txt")),
            "data/test/file.txt"
        );
    }

    #[tokio::test]
    async fn test_list_files_in_dir() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            // Create a few files and a subdirectory
            tokio::fs::write(dir.join("a.txt"), b"a").await?;
            tokio::fs::write(dir.join("b.csv"), b"b").await?;
            tokio::fs::create_dir(dir.join("subdir")).await?;
            tokio::fs::write(dir.join("subdir").join("nested.txt"), b"n").await?;

            let mut files = util::fs::list_files_in_dir(&dir).await?;
            files.sort();

            // Should only contain the two top-level files, not the subdir or its contents
            assert_eq!(files.len(), 2);
            assert!(files.contains(&dir.join("a.txt")));
            assert!(files.contains(&dir.join("b.csv")));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_files_in_dir_empty() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            let files = util::fs::list_files_in_dir(&dir).await?;
            assert!(files.is_empty());
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_files_in_dir_nonexistent() {
        let result = util::fs::list_files_in_dir(Path::new("/nonexistent/path")).await;
        assert!(result.is_err());
    }
}
