use rocksdb::{DBWithThreadMode, MultiThreaded};

use actix_web::{web, Error};
use futures::StreamExt;
use reqwest::header::HeaderValue;
use reqwest::Client;
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use zip::ZipArchive;

use crate::constants::FILES_DIR;
use crate::constants::STAGED_DIR;
use crate::constants::VERSIONS_DIR;
use crate::core::db;
use crate::core::v_latest::add::{add_file_node_to_staged_db, process_add_file};
use crate::core::v_latest::index::CommitMerkleTree;
use crate::error::OxenError;
use crate::model::merkle_tree::node::StagedMerkleTreeNode;
use crate::model::workspace::Workspace;
use crate::model::LocalRepository;
use crate::model::{Commit, StagedEntryStatus};
use crate::repositories;
use crate::util;

const BUFFER_SIZE_THRESHOLD: usize = 262144; // 256kb
const MAX_CONTENT_LENGTH: u64 = 1024 * 1024 * 1024; // 1GB limit
const MAX_DECOMPRESSED_SIZE: u64 = 1024 * 1024 * 1024; // 1GB limit
const MAX_COMPRESSION_RATIO: u64 = 100; // Maximum allowed

pub fn add(workspace: &Workspace, filepath: impl AsRef<Path>) -> Result<PathBuf, OxenError> {
    let filepath = filepath.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    let base_repo = &workspace.base_repo;

    // Stage the file using the repositories::add method
    let commit = workspace.commit.clone();
    p_add_file(base_repo, workspace_repo, &Some(commit), filepath)?;

    // Return the relative path of the file in the workspace
    let relative_path = util::fs::path_relative_to_dir(filepath, &workspace_repo.path)?;
    Ok(relative_path)
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

pub fn delete(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let path = path.as_ref();
    let workspace_repo = &workspace.workspace_repo;

    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&workspace_repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    let path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;
    let relative_path_str = path.to_str().unwrap();
    staged_db.delete(relative_path_str)?;
    Ok(())
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
        std::fs::metadata(&save_path)?.len()
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
            let path = repositories::workspaces::files::add(workspace, file)?;
            log::debug!("file::import add file ✅ success! staged file {:?}", path);
        }
    } else {
        log::debug!("file::import add file {:?}", &filepath);
        let path = repositories::workspaces::files::add(workspace, &save_path)?;
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
    //File unzipped into the same directory
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
            std::fs::create_dir_all(outdir)?;
        }

        if zip_file.is_dir() {
            std::fs::create_dir_all(&outpath)?;
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

fn p_add_file(
    base_repo: &LocalRepository,
    workspace_repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let versions_path = util::fs::oxen_hidden_dir(&base_repo.path)
        .join(VERSIONS_DIR)
        .join(FILES_DIR);
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&workspace_repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    let mut maybe_dir_node = None;
    if let Some(head_commit) = maybe_head_commit {
        let path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;
        let parent_path = path.parent().unwrap_or(Path::new(""));
        maybe_dir_node = CommitMerkleTree::dir_with_children(base_repo, head_commit, parent_path)?;
    }

    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));
    process_add_file(
        workspace_repo,
        &workspace_repo.path,
        &versions_path,
        &staged_db,
        &maybe_dir_node,
        path,
        &seen_dirs,
    )
}

fn p_modify_file(
    base_repo: &LocalRepository,
    workspace_repo: &LocalRepository,
    maybe_head_commit: &Option<Commit>,
    path: &Path,
) -> Result<Option<StagedMerkleTreeNode>, OxenError> {
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&workspace_repo.path).join(STAGED_DIR);
    log::debug!(
        "p_modify_file path: {:?} staged db_path: {:?}",
        path,
        db_path
    );
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    let mut maybe_file_node = None;
    if let Some(head_commit) = maybe_head_commit {
        maybe_file_node = repositories::tree::get_file_by_path(base_repo, head_commit, path)?;
    }

    if let Some(mut file_node) = maybe_file_node {
        file_node.set_name(path.to_str().unwrap());
        log::debug!("p_modify_file file_node: {}", file_node);
        add_file_node_to_staged_db(&staged_db, path, StagedEntryStatus::Modified, &file_node)
    } else {
        Err(OxenError::basic_str("file not found in head commit"))
    }
}
