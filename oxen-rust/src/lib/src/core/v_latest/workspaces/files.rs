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
    process_add_file_with_staged_db_manager, stage_file_with_hash,
};
use crate::core::{self, db};
use crate::error::OxenError;
use crate::model::file::TempFilePathNew;
use crate::model::merkle_tree::node::EMerkleTreeNode;
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::user::User;
use crate::model::workspace::Workspace;
use crate::model::{Branch, Commit, StagedEntryStatus};
use crate::model::{LocalRepository, NewCommitBody};
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

pub async fn rm(
    workspace: &Workspace,
    filepath: impl AsRef<Path>,
) -> Result<Vec<ErrorFileInfo>, OxenError> {
    let filepath = filepath.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    let base_repo = &workspace.base_repo;

    // Stage the file using the repositories::rm method
    let err_files = p_rm(base_repo, workspace_repo, &workspace.commit, filepath).await?;

    // Return the Err files
    Ok(err_files)
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

// Skips re-computing the hash in the add logic
pub fn add_version_file_with_hash(
    workspace: &Workspace,
    version_path: impl AsRef<Path>,
    dst_path: impl AsRef<Path>,
    file_hash: &str,
) -> Result<PathBuf, OxenError> {
    // version_path is where the file is stored, dst_path is the relative path to the repo
    let version_path = version_path.as_ref();
    let dst_path = dst_path.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));

    with_staged_db_manager(workspace_repo, |staged_db_manager| {
        stage_file_with_hash(
            workspace,
            version_path,
            dst_path,
            file_hash,
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
                Ok(_) => {
                    // Add parents to staged db
                    // let parent_dirs = item.parents;
                }
                Err(e) => {
                    log::error!("error with adding file: {e:?}");
                    err_files.push(ErrorFileInfo {
                        hash: item.hash.clone(),
                        path: Some(item.path.clone()),
                        error: format!("Failed to add file to staged db: {e}"),
                    });
                    continue;
                }
            }
        }
        log::debug!(
            "add_version_files complete with {:?} err_files",
            err_files.len()
        );
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
                log::debug!("Error removing file path {path:?}: {e:?}");
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
            "URL has an invalid filename {url}"
        )));
    }

    log::debug!("files::import_file Got uploaded file name: {filename}");

    let auth_header_value = HeaderValue::from_str(auth)
        .map_err(|_e| OxenError::file_import_error(format!("Invalid header auth value {auth}")))?;

    fetch_file(url, auth_header_value, directory, filename, workspace).await?;

    Ok(())
}

pub async fn upload_zip(
    commit_message: &str,
    user: &User,
    temp_files: Vec<TempFilePathNew>,
    workspace: &Workspace,
    branch: &Branch,
) -> Result<Commit, OxenError> {
    // Unzip the files and add
    for temp_file in temp_files {
        let files = decompress_zip(&temp_file.temp_file_path)?;

        for file in files.iter() {
            // Skip files in __MACOSX directories
            if file
                .components()
                .any(|component| component.as_os_str().to_string_lossy() == "__MACOSX")
            {
                log::debug!("Skipping __MACOSX file: {file:?}");
                continue;
            }

            repositories::workspaces::files::add(workspace, file).await?;
        }
    }

    let data = NewCommitBody {
        message: commit_message.to_string(),
        author: user.name.clone(),
        email: user.email.clone(),
    };

    let res = repositories::workspaces::commit(workspace, &data, &branch.name).await;
    match res {
        Ok(commit) => {
            log::debug!("workspace::commit ✅ success! commit {commit:?}");
            Ok(commit)
        }
        Err(OxenError::WorkspaceBehind(workspace)) => {
            log::error!(
                "unable to commit branch {:?}. Workspace behind",
                branch.name
            );
            Err(OxenError::WorkspaceBehind(workspace))
        }
        Err(err) => {
            log::error!("unable to commit branch {:?}. Err: {}", branch.name, err);
            Err(err)
        }
    }
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
        .map_err(|e| OxenError::file_import_error(format!("Fetch file request failed: {e}")))?;

    let resp_headers = response.headers();

    let content_type = resp_headers
        .get("content-type")
        .and_then(|h| h.to_str().ok())
        .ok_or_else(|| OxenError::file_import_error("Fetch file response missing content type"))?;

    let content_length = response.content_length();
    if let Some(content_length) = content_length {
        if content_length > MAX_CONTENT_LENGTH {
            return Err(OxenError::file_import_error(format!(
                "Content length {content_length} exceeds maximum allowed size of 1GB"
            )));
        }
    }
    let is_zip = content_type.contains("zip");

    log::debug!("files::import_file Got filename : {filename:?}");

    let filepath = directory.join(filename);
    log::debug!("files::import_file got download filepath: {filepath:?}");

    // handle download stream
    let mut stream = response.bytes_stream();
    let mut buffer = web::BytesMut::new();
    let mut save_path = PathBuf::new();
    let mut bytes_downloaded: u64 = 0;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| OxenError::file_import_error("Error reading file stream"))?;
        let processed_chunk = chunk.to_vec();
        buffer.extend_from_slice(&processed_chunk);
        bytes_downloaded += processed_chunk.len() as u64;

        if bytes_downloaded > MAX_CONTENT_LENGTH {
            delete_file(workspace, &filepath)?;
            return Err(OxenError::file_import_error(
                "Content length exceeds maximum allowed size of 1GB",
            ));
        }
        if buffer.len() > BUFFER_SIZE_THRESHOLD {
            save_path = save_stream(workspace, &filepath, buffer.split().freeze().to_vec())
                .await
                .map_err(|e| {
                    OxenError::file_import_error(format!(
                        "Error occurred when saving file stream: {e}"
                    ))
                })?;
        }
    }

    if !buffer.is_empty() {
        save_path = save_stream(workspace, &filepath, buffer.freeze().to_vec())
            .await
            .map_err(|e| {
                OxenError::file_import_error(format!("Error occurred when saving file stream: {e}"))
            })?;
    }
    log::debug!("workspace::files::import_file save_path is {save_path:?}");

    // check if the file size matches
    if let Some(content_length) = content_length {
        let bytes_written = if save_path.exists() {
            util::fs::metadata(&save_path)?.len()
        } else {
            0
        };

        log::debug!(
            "workspace::files::import_file has written {bytes_written:?} bytes. It's expecting {content_length:?} bytes"
        );

        if bytes_written != content_length {
            return Err(OxenError::file_import_error(
                "Content length does not match. File incomplete.",
            ));
        }
    }

    // decompress and stage file
    if is_zip {
        let files = decompress_zip(&save_path)?;
        log::debug!("workspace::files::import_file unzipped file");

        for file in files.iter() {
            log::debug!("file::import add file {file:?}");
            let path = repositories::workspaces::files::add(workspace, file).await?;
            log::debug!("file::import add file ✅ success! staged file {path:?}");
        }
    } else {
        log::debug!("file::import add file {:?}", &filepath);
        let path = repositories::workspaces::files::add(workspace, &save_path).await?;
        log::debug!("file::import add file ✅ success! staged file {path:?}");
    }

    Ok(())
}

fn delete_file(workspace: &Workspace, path: impl AsRef<Path>) -> Result<(), OxenError> {
    let path = path.as_ref();
    let workspace_repo = &workspace.workspace_repo;
    let relative_path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;
    let full_path = workspace_repo.path.join(&relative_path);

    if full_path.exists() {
        std::fs::remove_file(&full_path).map_err(|e| {
            OxenError::file_import_error(format!(
                "Failed to remove file {}: {}",
                full_path.display(),
                e
            ))
        })?;
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

pub fn decompress_zip(zip_filepath: &PathBuf) -> Result<Vec<PathBuf>, OxenError> {
    // File unzipped into the same directory
    let mut files: Vec<PathBuf> = vec![];
    let file = File::open(zip_filepath)?;
    let mut archive = ZipArchive::new(file)
        .map_err(|e| OxenError::basic_str(format!("Failed to access zip file: {e}")))?;

    // Calculate total uncompressed size
    let mut total_size: u64 = 0;
    for i in 0..archive.len() {
        let zip_file = archive.by_index(i).map_err(|e| {
            OxenError::basic_str(format!("Failed to access zip file at index {i}: {e}"))
        })?;

        let uncompressed_size = zip_file.size();
        let compressed_size = zip_file.compressed_size();

        // Check individual file compression ratio
        if compressed_size > 0 {
            let compression_ratio = uncompressed_size / compressed_size;
            if compression_ratio > MAX_COMPRESSION_RATIO {
                return Err(OxenError::basic_str(format!(
                    "Suspicious zip compression ratio: {compression_ratio} detected"
                )));
            }
        } else if uncompressed_size > 0 {
            // If compressed size is 0 but uncompressed isn't, that's suspicious
            return Err(OxenError::basic_str(
                "Suspicious zip file: compressed size is 0 but uncompressed size is not",
            ));
        }
        // If both are 0, it's likely a directory entry, which is fine

        total_size += uncompressed_size;

        // Check total size limit
        if total_size > MAX_DECOMPRESSED_SIZE {
            return Err(OxenError::file_import_error(
                "Decompressed size exceeds size limit of 1GB",
            ));
        }
    }

    log::debug!("liboxen::files::decompress_zip zip filepath is {zip_filepath:?}");

    // Get the canonical (absolute) path of the parent directory
    let parent = match zip_filepath.parent() {
        Some(p) => p.canonicalize()?,
        None => std::env::current_dir()?,
    };

    // iterate thru zip archive and save the decompressed file
    for i in 0..archive.len() {
        let mut zip_file = archive.by_index(i).map_err(|e| {
            OxenError::basic_str(format!("Failed to access zip file at index {i}: {e}"))
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
                "Attempted path traversal detected: {outpath:?}"
            )));
        }

        log::debug!("files::decompress_zip unzipping file to: {outpath:?}");

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

        files.push(outpath.clone());
    }

    log::debug!("files::decompress_zip removing zip file: {zip_filepath:?}");

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
                    "Invalid path component in zip file: {path:?}"
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
        maybe_dir_node =
            repositories::tree::get_dir_with_children(base_repo, head_commit, parent_path, None)?;
    }

    // Skip if it's not a file
    let file_name = path.file_name().unwrap_or_default().to_string_lossy();
    let relative_path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;
    let full_path = workspace_repo.path.join(&relative_path);
    if !full_path.is_file() {
        log::debug!("is not a file - skipping add on {full_path:?}");
        return Ok(());
    }

    // See if this is a new file or a modified file
    let file_status =
        core::v_latest::add::determine_file_status(&maybe_dir_node, &file_name, &full_path)?;

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

async fn p_rm(
    base_repo: &LocalRepository,
    workspace_repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
) -> Result<Vec<ErrorFileInfo>, OxenError> {
    log::debug!("p_rm: deleting file {path:?}");
    let relative_path = util::fs::path_relative_to_dir(path, &workspace_repo.path)?;

    let parent_path = path.parent().unwrap_or(Path::new(""));
    let maybe_dir_node =
        repositories::tree::get_dir_with_children(base_repo, commit, parent_path, None)?;

    let file_name = util::fs::path_relative_to_dir(path, parent_path)?;
    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));
    let mut err_files: Vec<ErrorFileInfo> = vec![];
    if let Some(mut file_node) = get_file_node(&maybe_dir_node, &file_name)? {
        file_node.set_name(&path.to_string_lossy());
        err_files.extend(core::v_latest::rm::remove_file_with_db_manager(
            workspace_repo,
            &relative_path,
            &file_node,
            &seen_dirs,
        )?);
    } else if has_dir_node(&maybe_dir_node, file_name)? {
        if let Some(dir_node) = repositories::tree::get_dir_with_children_recursive(
            base_repo,
            commit,
            &relative_path,
            None,
        )? {
            core::v_latest::rm::remove_dir_with_db_manager(
                workspace_repo,
                &dir_node,
                &relative_path,
                &seen_dirs,
            )?;
        };
    } else {
        // If the path has neither a file node or dir node in the tree, it cannot be staged for removal
        // Return as err_file
        err_files.push(ErrorFileInfo {
            hash: "".to_string(),
            path: Some(path.to_path_buf()),
            error: "Cannot call `oxen rm` on uncommitted files".to_string(),
        });
    }

    Ok(err_files)
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
        log::debug!("p_modify_file file_node: {file_node}");
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

fn has_dir_node(
    dir_node: &Option<MerkleTreeNode>,
    path: impl AsRef<Path>,
) -> Result<bool, OxenError> {
    if let Some(node) = dir_node {
        if let Some(node) = node.get_by_path(path)? {
            if let EMerkleTreeNode::Directory(_dir_node) = &node.node {
                Ok(true)
            } else {
                Ok(false)
            }
        } else {
            Ok(false)
        }
    } else {
        Ok(false)
    }
}

/// Move or rename a file within a workspace.
/// This stages the old path as "Removed" and the new path as "Added".
pub async fn mv(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    new_path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let path = path.as_ref();
    let new_path = new_path.as_ref();
    let workspace_repo = &workspace.workspace_repo;

    // First, try to read existing staged entry for the source path
    let staged_entry = with_staged_db_manager(workspace_repo, |staged_db_manager| {
        staged_db_manager.read_from_staged_db(path)
    })?;

    // Get the file node - either from staged_db or from the base repo
    let file_node = if let Some(entry) = staged_entry {
        entry.node.file()?
    } else {
        // File not staged, get it from the base repo
        repositories::tree::get_file_by_path(&workspace.base_repo, &workspace.commit, path)?
            .ok_or_else(|| {
                OxenError::basic_str(format!("File not found in workspace or base repo: {path:?}"))
            })?
    };

    // Create the new file node with updated name (full path for the new location)
    let mut new_file_node = file_node.clone();
    new_file_node.set_name(new_path.to_str().unwrap());

    // Create a file node for the removed entry with the full original path as name
    let mut removed_file_node = file_node.clone();
    removed_file_node.set_name(path.to_str().unwrap());

    // Check if the source file exists in the base repo (needs to be staged for removal)
    let source_exists_in_base =
        repositories::tree::get_file_by_path(&workspace.base_repo, &workspace.commit, path)?
            .is_some();

    // Check if a file exists at the new path in the base repo (determines if it's modified or added)
    let dest_exists_in_base =
        repositories::tree::get_file_by_path(&workspace.base_repo, &workspace.commit, new_path)?
            .is_some();

    let new_status = if dest_exists_in_base {
        StagedEntryStatus::Modified
    } else {
        StagedEntryStatus::Added
    };

    let seen_dirs = Arc::new(Mutex::new(HashSet::new()));

    with_staged_db_manager(workspace_repo, |staged_db_manager| {
        // Add the file node at the new path
        staged_db_manager.upsert_file_node(new_path, new_status, &new_file_node)?;

        // If the source file exists in the base repo, stage it for removal
        // If it was only staged (not in base repo), just delete the staged entry
        if source_exists_in_base {
            // Stage the original path as removed (using removed_file_node with full path as name)
            staged_db_manager.upsert_file_node(
                path,
                StagedEntryStatus::Removed,
                &removed_file_node,
            )?;

            // Add parent directories for the removed path
            if let Some(parents) = path.parent() {
                for dir in parents.ancestors() {
                    staged_db_manager.add_directory(dir, &seen_dirs)?;
                    if dir == Path::new("") {
                        break;
                    }
                }
            }
        } else {
            // Just delete the staged entry if file wasn't in base repo
            staged_db_manager.delete_entry(path)?;
        }

        // Add parent directories for the new path
        if let Some(parents) = new_path.parent() {
            for dir in parents.ancestors() {
                staged_db_manager.add_directory(dir, &seen_dirs)?;
                if dir == Path::new("") {
                    break;
                }
            }
        }

        Ok(())
    })?;

    let relative_path = util::fs::path_relative_to_dir(new_path, &workspace_repo.path)?;
    Ok(relative_path)
}
