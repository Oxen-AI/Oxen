use rocksdb::{DBWithThreadMode, MultiThreaded};

use actix_web::{web, Error};
use futures::StreamExt;
use parking_lot::Mutex;
use reqwest::header::HeaderValue;
use reqwest::Client;
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use zip::ZipArchive;

use crate::constants::STAGED_DIR;
use crate::core::staged::staged_db_manager::with_staged_db_manager;
use crate::core::v_latest::add::{
    add_file_node_to_staged_db, get_file_node, get_status_and_add_file,
    process_add_file_with_staged_db_manager,
};
use crate::core::v_latest::index::CommitMerkleTree;
use crate::core::{self, db};
use crate::error::OxenError;
use crate::model::workspace::Workspace;
use crate::model::LocalRepository;
use crate::model::{Commit, StagedEntryStatus};
use crate::repositories;
use crate::util;
use crate::view::{ErrorFileInfo, FileWithHash};

const BUFFER_SIZE_THRESHOLD: usize = 262144; // 256kb
const MAX_CONTENT_LENGTH: u64 = 1024 * 1024 * 1024; // 1GB limit
const MAX_DECOMPRESSED_SIZE: u64 = 1024 * 1024 * 1024; // 1GB limit
const MAX_COMPRESSION_RATIO: u64 = 100; // Maximum allowed

// TODO: Do we depreciate this, if we always upload to version store?
pub async fn add(workspace: &Workspace, filepath: impl AsRef<Path>) -> Result<PathBuf, OxenError> {
    let filepath = filepath.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    let base_repo = &workspace.base_repo;

    // Stage the file using the repositories::add method
    let commit = workspace.commit.clone();
    p_add_file(base_repo, workspace_repo, &Some(commit), filepath).await?;

    // Return the relative path of the file in the workspace
    let relative_path = util::fs::path_relative_to_dir(filepath, &workspace_repo.path)?;
    Ok(relative_path)
}

// TODO: Support for removing dirs
// p_rm_file cannot remove dirs
pub async fn rm(workspace: &Workspace, filepath: impl AsRef<Path>) -> Result<PathBuf, OxenError> {
    let filepath = filepath.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    let base_repo = &workspace.base_repo;

    // Stage the file using the repositories::rm method
    p_rm_file(base_repo, workspace_repo, filepath).await?;

    // Return the relative path of the file in the workspace
    let relative_path = util::fs::path_relative_to_dir(filepath, &workspace_repo.path)?;
    Ok(relative_path)
}

pub fn add_version_file(
    workspace: &Workspace,
    version_path: impl AsRef<Path>,
    dst_path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    // version_path is where the file is stored, dst_path is the relative path to the repo path
    let version_path = version_path.as_ref();
    let dst_path = dst_path.as_ref();

    let workspace_repo = &workspace.workspace_repo;
    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));

    with_staged_db_manager(workspace_repo, |staged_db_manager| {
        get_status_and_add_file(
            workspace_repo,
            version_path,
            dst_path,
            staged_db_manager,
            &seen_dirs,
        )
    })?;

    Ok(dst_path.to_path_buf())
}

pub fn add_version_files(
    repo: &LocalRepository,
    workspace: &Workspace,
    files_with_hash: &[FileWithHash],
    directory: impl AsRef<str>,
) -> Result<Vec<ErrorFileInfo>, OxenError> {
    let version_store = repo.version_store()?;

    let directory = directory.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));

    let mut err_files: Vec<ErrorFileInfo> = vec![];
    with_staged_db_manager(workspace_repo, |staged_db_manager| {
        for item in files_with_hash.iter() {
            let version_path = version_store.get_version_path(&item.hash)?;
            let target_path = PathBuf::from(directory).join(&item.path);

            match get_status_and_add_file(
                workspace_repo,
                &version_path,
                &target_path,
                staged_db_manager,
                &seen_dirs,
            ) {
                Ok(_) => (),
                Err(e) => {
                    err_files.push(ErrorFileInfo {
                        hash: item.hash.clone(),
                        path: Some(item.path.clone()),
                        error: format!("Failed to add file to staged db: {}", e),
                    });
                    continue;
                }
            }
        }
        Ok(err_files)
    })
}

pub fn track_modified_data_frame(
    workspace: &Workspace,
    filepath: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let filepath = filepath.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    let base_repo = &workspace.base_repo;

    // Stage the file using the repositories::add method
    let commit = workspace.commit.clone();
    p_modify_file(base_repo, workspace_repo, &Some(commit), filepath)?;

    // Return the relative path of the file in the workspace
    let relative_path = util::fs::path_relative_to_dir(filepath, &workspace_repo.path)?;
    Ok(relative_path)
}

pub async fn remove_files_from_staged_db(
    workspace: &Workspace,
    paths: Vec<PathBuf>,
) -> Result<Vec<PathBuf>, OxenError> {
    let mut err_files = vec![];

    for path in paths {
        match delete(workspace, &path) {
            Ok(_) => {}
            Err(e) => {
                log::debug!("Error removing file path {path:?}: {:?}", e);
                err_files.push(path);
            }
        }
    }

    Ok(err_files)
}

pub fn delete(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let path = path.as_ref();
    let workspace_repo = &workspace.workspace_repo;

    let path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;
    with_staged_db_manager(workspace_repo, |staged_db_manager| {
        staged_db_manager.delete_entry(&path)
    })
}

pub fn exists(workspace: &Workspace, path: impl AsRef<Path>) -> Result<bool, OxenError> {
    let path = path.as_ref();
    let workspace_repo = &workspace.workspace_repo;

    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&workspace_repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open_for_read_only(&opts, dunce::simplified(&db_path), false)?;

    let path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;
    let relative_path_str = path.to_str().unwrap();
    let result = staged_db.key_may_exist(relative_path_str);
    Ok(result)
}

pub async fn import(
    url: &str,
    auth: &str,
    directory: PathBuf,
    mut filename: String,
    workspace: &Workspace,
) -> Result<(), OxenError> {
    // Sanitize filename
    filename = filename
        .chars()
        .map(|c| if c.is_whitespace() { '_' } else { c })
        .filter(|&c| c.is_alphanumeric() || c == '.' || c == '-' || c == '_')
        .collect::<String>();

    if filename.is_empty() {
        return Err(OxenError::file_import_error(format!(
            "URL has an invalid filename {}",
            url
        )));
    }

    log::debug!("files::import_file Got uploaded file name: {}", filename);

    let auth_header_value = HeaderValue::from_str(auth).map_err(|_e| {
        OxenError::file_import_error(format!("Invalid header auth value {}", auth))
    })?;

    fetch_file(url, auth_header_value, directory, filename, workspace).await?;

    Ok(())
}

async fn fetch_file(
    url: &str,
    auth_header_value: HeaderValue,
    directory: PathBuf,
    filename: String,
    workspace: &Workspace,
) -> Result<(), OxenError> {
    let response = Client::new()
        .get(url)
        .header("Authorization", auth_header_value)
        .send()
        .await
        .map_err(|e| OxenError::file_import_error(format!("Fetch file request failed: {}", e)))?;

    let resp_headers = response.headers();

    let content_type = resp_headers
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| OxenError::file_import_error("Fetch file response missing content type"))?;

    let content_length = response.content_length().ok_or_else(|| {
        OxenError::file_import_error("Fetch file response missing content length")
    })?;

    if content_length > MAX_CONTENT_LENGTH {
        return Err(OxenError::file_import_error(format!(
            "Content length {} exceeds maximum allowed size of 1GB",
            content_length
        )));
    }
    let is_zip = content_type.contains("zip");

    log::debug!("files::import_file Got filename : {filename:?}");

    let filepath = directory.join(filename);
    log::debug!("files::import_file got download filepath: {:?}", filepath);

    // handle download stream
    let mut stream = response.bytes_stream();
    let mut buffer = web::BytesMut::new();
    let mut save_path = PathBuf::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| OxenError::file_import_error("Error reading file stream"))?;
        let processed_chunk = chunk.to_vec();
        buffer.extend_from_slice(&processed_chunk);

        if buffer.len() > BUFFER_SIZE_THRESHOLD {
            save_path = save_stream(workspace, &filepath, buffer.split().freeze().to_vec())
                .await
                .map_err(|e| {
                    OxenError::file_import_error(format!(
                        "Error occurred when saving file stream: {}",
                        e
                    ))
                })?;
        }
    }

    if !buffer.is_empty() {
        save_path = save_stream(workspace, &filepath, buffer.freeze().to_vec())
            .await
            .map_err(|e| {
                OxenError::file_import_error(format!(
                    "Error occurred when saving file stream: {}",
                    e
                ))
            })?;
    }
    log::debug!("workspace::files::import_file save_path is {:?}", save_path);

    // check if the file size matches
    let bytes_written = if save_path.exists() {
        util::fs::metadata(&save_path)?.len()
    } else {
        0
    };

    log::debug!(
        "workspace::files::import_file has written {:?} bytes. It's expecting {:?} bytes",
        bytes_written,
        content_length
    );

    if bytes_written != content_length {
        return Err(OxenError::file_import_error(
            "Content length does not match. File incomplete.",
        ));
    }

    // decompress and stage file
    if is_zip {
        let files = decompress_zip(&save_path).await?;
        log::debug!("workspace::files::import_file unzipped file");

        for file in files.iter() {
            log::debug!("file::import add file {:?}", file);
            let path = repositories::workspaces::files::add(workspace, file).await?;
            log::debug!("file::import add file ✅ success! staged file {:?}", path);
        }
    } else {
        log::debug!("file::import add file {:?}", &filepath);
        let path = repositories::workspaces::files::add(workspace, &save_path).await?;
        log::debug!("file::import add file ✅ success! staged file {:?}", path);
    }

    Ok(())
}

pub async fn save_stream(
    workspace: &Workspace,
    filepath: &PathBuf,
    chunk: Vec<u8>,
) -> Result<PathBuf, Error> {
    // This function append and save file chunk
    log::debug!(
        "liboxen::workspace::files::save_stream writing {} bytes to file",
        chunk.len()
    );

    let workspace_dir = workspace.dir();

    log::debug!("liboxen::workspace::files::save_stream Got workspace dir: {workspace_dir:?}");

    let full_dir = workspace_dir.join(filepath);

    log::debug!("liboxen::workspace::files::save_stream Got full dir: {full_dir:?}");

    if let Some(parent) = full_dir.parent() {
        std::fs::create_dir_all(parent)?;
    }

    log::debug!(
        "liboxen::workspace::files::save_stream successfully created full dir: {full_dir:?}"
    );

    let full_dir_cpy = full_dir.clone();

    let mut file = web::block(move || {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(full_dir_cpy)
    })
    .await??;

    log::debug!("liboxen::workspace::files::save_stream is writing to file: {file:?}");

    web::block(move || file.write_all(&chunk).map(|_| file)).await??;

    Ok(full_dir)
}

async fn decompress_zip(zip_filepath: &PathBuf) -> Result<Vec<PathBuf>, OxenError> {
    // File unzipped into the same directory
    let mut files: Vec<PathBuf> = vec![];
    let file = File::open(zip_filepath)?;
    let mut archive = ZipArchive::new(file)
        .map_err(|e| OxenError::basic_str(format!("Failed to access zip file: {}", e)))?;

    // Calculate total uncompressed size
    let mut total_size: u64 = 0;
    for i in 0..archive.len() {
        let zip_file = archive.by_index(i).map_err(|e| {
            OxenError::basic_str(format!("Failed to access zip file at index {}: {}", i, e))
        })?;

        let uncompressed_size = zip_file.size();
        let compressed_size = zip_file.compressed_size();

        // Check individual file compression ratio
        let compression_ratio = uncompressed_size / compressed_size;
        if compressed_size > 0 && (compression_ratio) > MAX_COMPRESSION_RATIO {
            return Err(OxenError::basic_str(format!(
                "Suspicious zip compression ratio: {} detected",
                compression_ratio
            )));
        }

        total_size += uncompressed_size;

        // Check total size limit
        if total_size > MAX_DECOMPRESSED_SIZE {
            return Err(OxenError::file_import_error(
                "Decompressed size exceeds size limit of 1GB",
            ));
        }
    }

    log::debug!(
        "liboxen::files::decompress_zip zip filepath is {:?}",
        zip_filepath
    );

    // Get the canonical (absolute) path of the parent directory
    let parent = match zip_filepath.parent() {
        Some(p) => p.canonicalize()?,
        None => std::env::current_dir()?,
    };

    // iterate thru zip archive and save the decompressed file
    for i in 0..archive.len() {
        let mut zip_file = archive.by_index(i).map_err(|e| {
            OxenError::basic_str(format!("Failed to access zip file at index {}: {}", i, e))
        })?;

        let mut zipfile_name = zip_file.mangled_name();

        // Sanitize filename
        if let Some(zipfile_name_str) = zipfile_name.to_str() {
            if zipfile_name_str.chars().any(|c| c.is_whitespace()) {
                let new_name = zipfile_name_str
                    .chars()
                    .map(|c| if c.is_whitespace() { '_' } else { c })
                    .collect::<String>();
                zipfile_name = PathBuf::from(new_name);
            }
        }

        // Validate path components to prevent directory traversal
        let safe_path = sanitize_path(&zipfile_name)?;
        let outpath = parent.join(&safe_path);

        // Verify the final path is within the parent directory
        if !outpath.starts_with(&parent) {
            return Err(OxenError::basic_str(format!(
                "Attempted path traversal detected: {:?}",
                outpath
            )));
        }

        log::debug!("files::decompress_zip unzipping file to: {:?}", outpath);

        if let Some(outdir) = outpath.parent() {
            util::fs::create_dir_all(outdir)?;
        }

        if zip_file.is_dir() {
            util::fs::create_dir_all(&outpath)?;
        } else {
            let mut outfile = File::create(&outpath)?;
            let mut buffer = vec![0; BUFFER_SIZE_THRESHOLD];

            loop {
                let n = zip_file.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                outfile.write_all(&buffer[..n])?;
            }
        }

        files.push(outpath);
    }

    log::debug!(
        "files::decompress_zip removing zip file: {:?}",
        zip_filepath
    );

    // remove the zip file after decompress
    std::fs::remove_file(zip_filepath)?;

    Ok(files)
}

// Helper function to sanitize path and prevent directory traversal
fn sanitize_path(path: &PathBuf) -> Result<PathBuf, OxenError> {
    let mut components = Vec::new();

    for component in path.components() {
        match component {
            Component::Normal(c) => components.push(c),
            Component::CurDir => {} // Skip current directory components (.)
            Component::ParentDir | Component::Prefix(_) | Component::RootDir => {
                return Err(OxenError::basic_str(format!(
                    "Invalid path component in zip file: {:?}",
                    path
                )));
            }
        }
    }

    let safe_path = components.iter().collect::<PathBuf>();
    Ok(safe_path)
}

async fn p_add_file(
    base_repo: &LocalRepository,
    workspace_repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
) -> Result<(), OxenError> {
    let version_store = base_repo.version_store()?;
    let mut maybe_dir_node = None;
    if let Some(head_commit) = maybe_head_commit {
        let path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;
        let parent_path = path.parent().unwrap_or(Path::new(""));
        maybe_dir_node = CommitMerkleTree::dir_with_children(base_repo, head_commit, parent_path)?;
    }

    // Skip if it's not a file
    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
    let relative_path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;
    let full_path = workspace_repo.path.join(&relative_path);
    if !full_path.is_file() {
        log::debug!("is not a file - skipping add on {:?}", full_path);
        return Ok(());
    }

    // See if this is a new file or a modified file
    let file_status =
        core::v_latest::add::determine_file_status(&maybe_dir_node, &file_name, &full_path)?;
    log::debug!("File status: {file_status:?}");
    // Store the file in the version store using the hash as the key
    let hash_str = file_status.hash.to_string();
    version_store
        .store_version_from_path(&hash_str, &full_path)
        .await?;

    let conflicts: HashSet<PathBuf> = repositories::merge::list_conflicts(workspace_repo)?
        .into_iter()
        .map(|conflict| conflict.merge_entry.path)
        .collect();

    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));

    process_add_file_with_staged_db_manager(
        workspace_repo,
        &workspace_repo.path,
        &file_status,
        path,
        &seen_dirs,
        &conflicts,
    )
}

// TODO: Make separate method for removing dirs
async fn p_rm_file(
    base_repo: &LocalRepository,
    workspace_repo: &LocalRepository,
    path: &Path,
) -> Result<(), OxenError> {
    let head_commit = repositories::commits::head_commit(base_repo)?;
    let parent_path = path.parent().unwrap_or(Path::new(""));
    let maybe_dir_node = CommitMerkleTree::dir_with_children(base_repo, &head_commit, parent_path)?;

    let relative_path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;

    // Get file node to remove
    let file_node = get_file_node(&maybe_dir_node, path)?;
    let Some(file_node) = file_node else {
        return Err(OxenError::basic_str(format!(
            "Error: Cannot remove file `{path:?}` because it is not committed"
        )));
    };

    core::v_latest::rm::remove_file(workspace_repo, &relative_path, &file_node)?;

    Ok(())
}

fn p_modify_file(
    base_repo: &LocalRepository,
    workspace_repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
) -> Result<(), OxenError> {
    let mut maybe_file_node = None;
    if let Some(head_commit) = maybe_head_commit {
        maybe_file_node = repositories::tree::get_file_by_path(base_repo, head_commit, path)?;
    }

    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));
    if let Some(mut file_node) = maybe_file_node {
        file_node.set_name(path.to_str().unwrap());
        log::debug!("p_modify_file file_node: {}", file_node);
        add_file_node_to_staged_db(
            workspace_repo,
            path,
            StagedEntryStatus::Modified,
            &file_node,
            &seen_dirs,
        )
    } else {
        Err(OxenError::basic_str("file not found in head commit"))
    }
}
