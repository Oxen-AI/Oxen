use liboxen::constants;
use liboxen::constants::COMMITS_DIR;
use liboxen::constants::DIRS_DIR;
use liboxen::constants::DIR_HASHES_DIR;
use liboxen::constants::HASH_FILE;
use liboxen::constants::HISTORY_DIR;
use liboxen::constants::OBJECTS_DIR;
use liboxen::constants::VERSION_FILE_NAME;

use liboxen::core::commit_sync_status;
use liboxen::error::OxenError;
use liboxen::model::{Commit, LocalRepository};
use liboxen::opts::PaginateOpts;
use liboxen::repositories;
use liboxen::util;
use liboxen::view::branch::BranchName;
use liboxen::view::tree::merkle_hashes::MerkleHashes;
use liboxen::view::MerkleHashesResponse;
use liboxen::view::{
    CommitResponse, ListCommitResponse, PaginatedCommits, Pagination, RootCommitResponse,
    StatusMessage,
};
use os_path::OsPath;

use crate::app_data::OxenAppData;
use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::parse_resource;
use crate::params::PageNumQuery;
use crate::params::{app_data, path_param};

use actix_web::{web, Error, HttpRequest, HttpResponse};
use bytesize::ByteSize;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::stream::StreamExt as _;
use serde::Deserialize;
use std::fs::OpenOptions;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use tar::Archive;

#[derive(Deserialize, Debug)]
pub struct ChunkedDataUploadQuery {
    hash: String,             // UUID to tie all the chunks together (hash of the contents)
    chunk_num: usize,         // which chunk it is, so that we can combine it all in the end
    total_chunks: usize,      // how many chunks to expect
    total_size: usize,        // total size so we can know when we are finished
    is_compressed: bool,      // whether or not we need to decompress the archive
    filename: Option<String>, // maybe a file name if !compressed
}

// List commits for a repository
pub async fn index(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let commits = repositories::commits::list(&repo).unwrap_or_default();
    Ok(HttpResponse::Ok().json(ListCommitResponse::success(commits)))
}

pub async fn history(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource_param = path_param(&req, "resource")?;

    let pagination = PaginateOpts {
        page_num: query.page.unwrap_or(constants::DEFAULT_PAGE_NUM),
        page_size: query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE),
    };

    if repositories::is_empty(&repo)? {
        return Ok(HttpResponse::Ok().json(PaginatedCommits::success(
            vec![],
            Pagination::empty(pagination),
        )));
    }

    log::debug!("commit_history resource_param: {:?}", resource_param);

    // This checks if the parameter received from the client is two commits split by "..", in this case we don't parse the resource
    let (resource, revision, commit) = if resource_param.contains("..") {
        (None, Some(resource_param), None)
    } else {
        let resource = parse_resource(&req, &repo)?;
        let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;
        (Some(resource), None, Some(commit))
    };

    match &resource {
        Some(resource) if resource.path != Path::new("") => {
            log::debug!("commit_history resource_param: {:?}", resource);
            let commits = repositories::commits::list_by_path_from_paginated(
                &repo,
                commit.as_ref().unwrap(), // Safe unwrap: `commit` is Some if `resource` is Some
                &resource.path,
                pagination,
            )?;
            log::debug!("commit_history got {} commits", commits.commits.len());
            Ok(HttpResponse::Ok().json(commits))
        }
        _ => {
            // Handling the case where resource is None or its path is empty
            log::debug!("commit_history revision: {:?}", revision);
            let revision_id = revision.as_ref().or_else(|| commit.as_ref().map(|c| &c.id));
            if let Some(revision_id) = revision_id {
                let commits =
                    repositories::commits::list_from_paginated(&repo, revision_id, pagination)?;
                log::debug!("commit_history got {} commits", commits.commits.len());
                // log::debug!("commit_history commits: {:?}", commits.commits);
                Ok(HttpResponse::Ok().json(commits))
            } else {
                Err(OxenHttpError::NotFound)
            }
        }
    }
}

// List all commits in the repository
pub async fn list_all(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let pagination = PaginateOpts {
        page_num: query.page.unwrap_or(constants::DEFAULT_PAGE_NUM),
        page_size: query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE),
    };
    let paginated_commits = repositories::commits::list_all_paginated(&repo, pagination)?;

    Ok(HttpResponse::Ok().json(paginated_commits))
}

pub async fn list_missing(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    // Parse commit ids from a body and return the missing ids
    let data: Result<MerkleHashes, serde_json::Error> = serde_json::from_str(&body);
    let Ok(merkle_hashes) = data else {
        log::error!("list_missing invalid JSON: {:?}", body);
        return Ok(HttpResponse::BadRequest().json(StatusMessage::error("Invalid JSON")));
    };

    log::debug!(
        "list_missing checking {} commit hashes",
        merkle_hashes.hashes.len()
    );
    let missing_commits =
        repositories::tree::list_missing_node_hashes(&repo, &merkle_hashes.hashes)?;
    log::debug!(
        "list_missing found {} missing commits",
        missing_commits.len()
    );
    let response = MerkleHashesResponse {
        status: StatusMessage::resource_found(),
        hashes: missing_commits,
    };
    Ok(HttpResponse::Ok().json(response))
}

pub async fn mark_hashes_as_synced(
    req: HttpRequest,
    mut body: web::Payload,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.unwrap());
    }

    let request: MerkleHashes = serde_json::from_slice(&bytes)?;
    let hashes = request.hashes;
    log::debug!(
        "mark_hashes_as_synced marking {} commit hashes",
        &hashes.len()
    );
    commit_sync_status::mark_commit_hashes_as_synced(&repository, &hashes)?;
    log::debug!("successfully marked {} commit hashes", &hashes.len());
    Ok(HttpResponse::Ok().json(MerkleHashesResponse {
        status: StatusMessage::resource_found(),
        hashes,
    }))
}

pub async fn show(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let commit_id = path_param(&req, "commit_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let commit = repositories::commits::get_by_id(&repo, &commit_id)?
        .ok_or(OxenError::revision_not_found(commit_id.into()))?;

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_found(),
        commit,
    }))
}

pub async fn parents(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let commit_or_branch = path_param(&req, "commit_or_branch")?;
    let repository = get_repo(&app_data.path, namespace, name)?;
    let commit = repositories::revisions::get(&repository, &commit_or_branch)?
        .ok_or(OxenError::revision_not_found(commit_or_branch.into()))?;
    let parents = repositories::commits::list_from(&repository, &commit.id)?;
    Ok(HttpResponse::Ok().json(ListCommitResponse {
        status: StatusMessage::resource_found(),
        commits: parents,
    }))
}

/// Download the database that holds all the commits and their parents
pub async fn download_commits_db(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let buffer = compress_commits_db(&repository)?;
    Ok(HttpResponse::Ok().body(buffer))
}

/// Take the commits db and compress it into a tarball buffer we can return
fn compress_commits_db(repository: &LocalRepository) -> Result<Vec<u8>, OxenError> {
    // Tar and gzip the commit db directory
    // zip up the rocksdb in history dir, and post to server
    let commit_dir = util::fs::oxen_hidden_dir(&repository.path).join(COMMITS_DIR);
    // This will be the subdir within the tarball
    let tar_subdir = Path::new(COMMITS_DIR);

    log::debug!("Compressing commit db from dir {:?}", commit_dir);
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    tar.append_dir_all(tar_subdir, commit_dir)?;
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!("Compressed commit dir size is {}", ByteSize::b(total_size));

    Ok(buffer)
}

/// Download the database of all entries given a specific commit
pub async fn download_dir_hashes_db(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    // base_head is the base and head commit id separated by ..
    let base_head = path_param(&req, "base_head")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    // Let user pass in base..head to download a range of commits
    // or we just get all the commits from the base commit to the first commit
    let commits = if base_head.contains("..") {
        let split = base_head.split("..").collect::<Vec<&str>>();
        if split.len() != 2 {
            return Err(OxenHttpError::BadRequest("Invalid base_head".into()));
        }
        let base_commit_id = split[0];
        let head_commit_id = split[1];
        let base_commit = repositories::revisions::get(&repository, base_commit_id)?
            .ok_or(OxenError::revision_not_found(base_commit_id.into()))?;
        let head_commit = repositories::revisions::get(&repository, head_commit_id)?
            .ok_or(OxenError::revision_not_found(head_commit_id.into()))?;

        let commits = repositories::commits::list_between(&repository, &base_commit, &head_commit)?;
        for commit in &commits {
            log::debug!("download_dir_hashes_db: list_between commit: {}", commit);
        }
        commits
    } else {
        repositories::commits::list_from(&repository, &base_head)?
    };
    let buffer = compress_commits(&repository, &commits)?;

    Ok(HttpResponse::Ok().body(buffer))
}

/// Download the database of all entries given a specific commit
pub async fn download_commit_entries_db(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let commit_or_branch = path_param(&req, "commit_or_branch")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let commit = repositories::revisions::get(&repository, &commit_or_branch)?
        .ok_or(OxenError::revision_not_found(commit_or_branch.into()))?;
    let buffer = compress_commit(&repository, &commit)?;

    Ok(HttpResponse::Ok().body(buffer))
}

// Allow downloading of multiple commits for efficiency
fn compress_commits(
    repository: &LocalRepository,
    commits: &[Commit],
) -> Result<Vec<u8>, OxenError> {
    // Tar and gzip all the commit dir_hashes db directories
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    let dirs_to_compress = vec![DIRS_DIR, DIR_HASHES_DIR];
    log::debug!("Compressing {} commits", commits.len());
    for commit in commits {
        let commit_dir = util::fs::oxen_hidden_dir(&repository.path)
            .join(HISTORY_DIR)
            .join(commit.id.clone());
        // This will be the subdir within the tarball
        let tar_subdir = Path::new(HISTORY_DIR).join(commit.id.clone());

        log::debug!("Compressing commit {} from dir {:?}", commit.id, commit_dir);

        for dir in &dirs_to_compress {
            let full_path = commit_dir.join(dir);
            let tar_path = tar_subdir.join(dir);
            if full_path.exists() {
                tar.append_dir_all(&tar_path, full_path)?;
            }
        }
    }
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!(
        "Compressed {} commits, size is {}",
        commits.len(),
        ByteSize::b(total_size)
    );

    Ok(buffer)
}

// Allow downloading of sub-dirs for efficiency
fn compress_commit(repository: &LocalRepository, commit: &Commit) -> Result<Vec<u8>, OxenError> {
    // Tar and gzip the commit db directory
    // zip up the rocksdb in history dir, and download from server
    let commit_dir = util::fs::oxen_hidden_dir(&repository.path)
        .join(HISTORY_DIR)
        .join(commit.id.clone());
    // This will be the subdir within the tarball
    let tar_subdir = Path::new(HISTORY_DIR).join(commit.id.clone());

    log::debug!("Compressing commit {} from dir {:?}", commit.id, commit_dir);
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    let dirs_to_compress = vec![DIRS_DIR, DIR_HASHES_DIR];

    for dir in &dirs_to_compress {
        let full_path = commit_dir.join(dir);
        let tar_path = tar_subdir.join(dir);
        if full_path.exists() {
            tar.append_dir_all(&tar_path, full_path)?;
        }
    }

    // Examine the full file structure of the tar

    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!(
        "Compressed commit {} size is {}",
        commit.id,
        ByteSize::b(total_size)
    );

    Ok(buffer)
}

/// This creates an empty commit on the given branch
pub async fn create(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("Got commit data: {}", body);

    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

    let new_commit: Commit = match serde_json::from_str(&body) {
        Ok(commit) => commit,
        Err(_) => {
            log::error!("commits create got invalid commit data {}", body);
            return Err(OxenHttpError::BadRequest("Invalid commit data".into()));
        }
    };
    log::debug!("commits create got new commit: {:?}", new_commit);

    let bn: BranchName =
        match serde_json::from_str(&body) {
            Ok(name) => name,
            Err(_) => return Err(OxenHttpError::BadRequest(
                "Must supply `branch_name` in body. Upgrade CLI to greater than v0.6.1 if failing."
                    .into(),
            )),
        };

    // Create Commit from uri params
    match repositories::commits::create_empty_commit(&repository, bn.branch_name, &new_commit) {
        Ok(commit) => Ok(HttpResponse::Ok().json(CommitResponse {
            status: StatusMessage::resource_created(),
            commit: commit.to_owned(),
        })),
        Err(OxenError::RootCommitDoesNotMatch(commit_id)) => {
            log::error!("Err create_commit: RootCommitDoesNotMatch {}", commit_id);
            Err(OxenHttpError::BadRequest("Remote commit history does not match local commit history. Make sure you are pushing to the correct remote.".into()))
        }
        Err(err) => {
            log::error!("Err create_commit: {}", err);
            Err(OxenHttpError::InternalServerError)
        }
    }
}

/// Controller to upload large chunks of data that will be combined at the end
pub async fn upload_chunk(
    req: HttpRequest,
    mut chunk: web::Payload,                   // the chunk of the file body,
    query: web::Query<ChunkedDataUploadQuery>, // gives the file
) -> Result<HttpResponse, OxenHttpError> {
    log::debug!("in upload_chunk controller");
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, name)?;

    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
    let id = query.hash.clone();
    let size = query.total_size;
    let chunk_num = query.chunk_num;
    let total_chunks = query.total_chunks;

    log::debug!(
        "upload_chunk got chunk {chunk_num}/{total_chunks} of upload {id} of total size {size}"
    );

    // Create a tmp dir for this upload
    let tmp_dir = hidden_dir.join("tmp").join("chunked").join(id);
    let chunk_file = tmp_dir.join(format!("chunk_{chunk_num:016}"));

    // mkdir if !exists
    if !tmp_dir.exists() {
        if let Err(err) = util::fs::create_dir_all(&tmp_dir) {
            log::error!(
                "upload_chunk could not complete chunk upload, mkdir failed: {:?}",
                err
            );
            return Ok(
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            );
        }
    }

    // Read bytes from body
    let mut bytes = web::BytesMut::new();
    while let Some(item) = chunk.next().await {
        bytes.extend_from_slice(&item.unwrap());
    }

    // Write to tmp file
    log::debug!("upload_chunk writing file {:?}", chunk_file);
    match OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&chunk_file)
    {
        Ok(mut f) => {
            match f.write_all(&bytes) {
                Ok(_) => {
                    // Successfully wrote chunk
                    log::debug!("upload_chunk successfully wrote chunk {:?}", chunk_file);

                    // TODO: there is a race condition here when multiple chunks
                    // are uploaded in parallel Currently doesn't hurt anything,
                    // but we should find a more elegant solution because we're
                    // doing a lot of extra work unpacking tarballs multiple
                    // times.
                    check_if_upload_complete_and_unpack(
                        hidden_dir,
                        tmp_dir,
                        total_chunks,
                        size,
                        query.is_compressed,
                        query.filename.to_owned(),
                    );

                    Ok(HttpResponse::Ok().json(StatusMessage::resource_created()))
                }
                Err(err) => {
                    log::error!(
                        "upload_chunk could not complete chunk upload, file write_all failed: {:?} -> {:?}",
                        err,
                        chunk_file
                    );
                    Ok(HttpResponse::InternalServerError()
                        .json(StatusMessage::internal_server_error()))
                }
            }
        }
        Err(err) => {
            log::error!(
                "upload_chunk could not complete chunk upload, file create failed: {:?} -> {:?}",
                err,
                chunk_file
            );
            Ok(HttpResponse::InternalServerError().json(StatusMessage::internal_server_error()))
        }
    }
}

fn check_if_upload_complete_and_unpack(
    hidden_dir: PathBuf,
    tmp_dir: PathBuf,
    total_chunks: usize,
    total_size: usize,
    is_compressed: bool,
    filename: Option<String>,
) {
    let mut files = util::fs::list_files_in_dir(&tmp_dir);

    log::debug!(
        "check_if_upload_complete_and_unpack checking if complete... {} / {}",
        files.len(),
        total_chunks
    );

    if total_chunks < files.len() {
        return;
    }
    files.sort();

    let mut uploaded_size: u64 = 0;
    for file in files.iter() {
        match util::fs::metadata(file) {
            Ok(metadata) => {
                uploaded_size += metadata.len();
            }
            Err(err) => {
                log::warn!("Err getting metadata on {:?}\n{:?}", file, err);
            }
        }
    }

    log::debug!(
        "check_if_upload_complete_and_unpack checking if complete... {} / {}",
        uploaded_size,
        total_size
    );

    // I think windows has a larger size than linux...so can't do a simple check here
    // But if we have all the chunks we should be good

    if (uploaded_size as usize) >= total_size {
        // std::thread::spawn(move || {
        // Get tar.gz bytes for history/COMMIT_ID data
        log::debug!(
            "check_if_upload_complete_and_unpack decompressing {} bytes to {:?}",
            total_size,
            hidden_dir
        );

        // TODO: Cleanup these if / else / match statements
        // Combine into actual file data
        if is_compressed {
            match unpack_compressed_data(&files, &hidden_dir) {
                Ok(_) => {
                    log::debug!(
                        "check_if_upload_complete_and_unpack unpacked {} files successfully",
                        files.len()
                    );
                }
                Err(err) => {
                    log::error!(
                        "check_if_upload_complete_and_unpack could not unpack compressed data {:?}",
                        err
                    );
                }
            }
        } else {
            match filename {
                Some(filename) => {
                    match unpack_to_file(&files, &hidden_dir, &filename) {
                        Ok(_) => {
                            log::debug!("check_if_upload_complete_and_unpack unpacked {} files successfully", files.len());
                        }
                        Err(err) => {
                            log::error!("check_if_upload_complete_and_unpack could not unpack compressed data {:?}", err);
                        }
                    }
                }
                None => {
                    log::error!(
                        "check_if_upload_complete_and_unpack must supply filename if !compressed"
                    );
                }
            }
        }

        // Cleanup tmp files
        match util::fs::remove_dir_all(&tmp_dir) {
            Ok(_) => {
                log::debug!(
                    "check_if_upload_complete_and_unpack removed tmp dir {:?}",
                    tmp_dir
                );
            }
            Err(err) => {
                log::error!(
                    "check_if_upload_complete_and_unpack could not remove tmp dir {:?} {:?}",
                    tmp_dir,
                    err
                );
            }
        }
        // });
    }
}

pub async fn upload_tree(
    req: HttpRequest,
    mut body: web::Payload,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let client_head_id = path_param(&req, "commit_id")?;
    let repo = get_repo(&app_data.path, namespace, name)?;
    // Get head commit on sever repo
    let server_head_commit = repositories::commits::head_commit(&repo)?;

    // Unpack in tmp/tree/commit_id
    let tmp_dir = util::fs::oxen_hidden_dir(&repo.path).join("tmp");

    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.unwrap());
    }

    let total_size: u64 = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    log::debug!(
        "Got compressed data for tree {} -> {}",
        client_head_id,
        ByteSize::b(total_size)
    );

    log::debug!("Decompressing {} bytes to {:?}", bytes.len(), tmp_dir);

    let mut archive = Archive::new(GzDecoder::new(&bytes[..]));

    unpack_tree_tarball(&tmp_dir, &mut archive);

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_found(),
        commit: server_head_commit.to_owned(),
    }))
}

pub async fn root_commit(req: HttpRequest) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, name)?;

    let root = repositories::commits::root_commit_maybe(&repo)?;

    Ok(HttpResponse::Ok().json(RootCommitResponse {
        status: StatusMessage::resource_found(),
        commit: root,
    }))
}

fn unpack_compressed_data(files: &[PathBuf], hidden_dir: &Path) -> Result<(), OxenError> {
    let mut buffer: Vec<u8> = Vec::new();
    for file in files.iter() {
        log::debug!("Reading file bytes {:?}", file);
        let mut f = std::fs::File::open(file).map_err(|e| OxenError::file_open_error(file, e))?;

        f.read_to_end(&mut buffer)
            .map_err(|e| OxenError::file_read_error(file, e))?;
    }

    // Unpack tarball to our hidden dir
    let mut archive = Archive::new(GzDecoder::new(&buffer[..]));
    unpack_entry_tarball(hidden_dir, &mut archive);

    Ok(())
}

fn unpack_to_file(files: &[PathBuf], hidden_dir: &Path, filename: &str) -> Result<(), OxenError> {
    // Append each buffer to the end of the large file
    // TODO: better error handling...
    log::debug!("Got filename {}", filename);

    // return path with native slashes
    let os_path = OsPath::from(filename).to_pathbuf();
    log::debug!("Got native filename {:?}", os_path);

    let mut full_path = hidden_dir.join(os_path);
    full_path =
        util::fs::replace_file_name_keep_extension(&full_path, VERSION_FILE_NAME.to_owned());
    log::debug!("Unpack to {:?}", full_path);
    if let Some(parent) = full_path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent).map_err(|e| OxenError::dir_create_error(parent, e))?;
        }
    }

    let mut outf = std::fs::File::create(&full_path)
        .map_err(|e| OxenError::file_create_error(&full_path, e))?;

    for file in files.iter() {
        log::debug!("Reading file bytes {:?}", file);
        let mut buffer: Vec<u8> = Vec::new();

        let mut f = std::fs::File::open(file).map_err(|e| OxenError::file_open_error(file, e))?;

        f.read_to_end(&mut buffer)
            .map_err(|e| OxenError::file_read_error(file, e))?;

        log::debug!("Read {} file bytes from file {:?}", buffer.len(), file);

        match outf.write_all(&buffer) {
            Ok(_) => {
                log::debug!("Unpack successful! {:?}", full_path);
            }
            Err(err) => {
                log::error!("Could not write all data to disk {:?}", err);
            }
        }
    }
    Ok(())
}

/// Controller to upload the commit database
pub async fn upload(
    req: HttpRequest,
    mut body: web::Payload, // the actual file body
) -> Result<HttpResponse, OxenHttpError> {
    log::debug!("in regular upload controller");
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &name)?;

    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);

    // Read bytes from body
    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.unwrap());
    }

    // Compute total size as u64
    let total_size: u64 = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    log::debug!(
        "Got compressed data for repo {}/{} -> {}",
        namespace,
        name,
        ByteSize::b(total_size)
    );

    // Unpack in background thread because could take awhile
    // std::thread::spawn(move || {
    // Get tar.gz bytes for history/COMMIT_ID data
    log::debug!("Decompressing {} bytes to {:?}", bytes.len(), hidden_dir);
    // Unpack tarball to our hidden dir
    let mut archive = Archive::new(GzDecoder::new(&bytes[..]));
    unpack_entry_tarball(&hidden_dir, &mut archive);
    // });

    Ok(HttpResponse::Ok().json(StatusMessage::resource_created()))
}

/// Notify that the push should be complete, and we should start doing our background processing
pub async fn complete(req: HttpRequest) -> Result<HttpResponse, Error> {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    // name to the repo, should be in url path so okay to unwrap
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let commit_id: &str = req.match_info().get("commit_id").unwrap();

    match repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name) {
        Ok(Some(repo)) => {
            match repositories::commits::get_by_id(&repo, commit_id) {
                Ok(Some(commit)) => {
                    let response = CommitResponse {
                        status: StatusMessage::resource_created(),
                        commit: commit.clone(),
                    };
                    Ok(HttpResponse::Ok().json(response))
                }
                Ok(None) => {
                    log::error!("Could not find commit [{}]", commit_id);
                    Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
                }
                Err(err) => {
                    log::error!("Error finding commit [{}]: {}", commit_id, err);
                    Ok(HttpResponse::InternalServerError()
                        .json(StatusMessage::internal_server_error()))
                }
            }
        }
        Ok(None) => {
            log::debug!("404 could not get repo {}", repo_name,);
            Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
        }
        Err(repo_err) => {
            log::error!("Err get_by_name: {}", repo_err);
            Ok(HttpResponse::InternalServerError().json(StatusMessage::internal_server_error()))
        }
    }
}

fn unpack_tree_tarball(tmp_dir: &Path, archive: &mut Archive<GzDecoder<&[u8]>>) {
    match archive.entries() {
        Ok(entries) => {
            for file in entries {
                if let Ok(mut file) = file {
                    let path = file.path().unwrap();
                    log::debug!("unpack_tree_tarball path {:?}", path);
                    let stripped_path = if path.starts_with(HISTORY_DIR) {
                        match path.strip_prefix(HISTORY_DIR) {
                            Ok(stripped) => stripped,
                            Err(err) => {
                                log::error!("Could not strip prefix from path {:?}", err);
                                return;
                            }
                        }
                    } else {
                        &path
                    };

                    let mut new_path = PathBuf::from(tmp_dir);
                    new_path.push(stripped_path);

                    if let Some(parent) = new_path.parent() {
                        if !parent.exists() {
                            std::fs::create_dir_all(parent).expect("Could not create parent dir");
                        }
                    }
                    log::debug!("unpack_tree_tarball new_path {:?}", path);
                    file.unpack(&new_path).unwrap();
                } else {
                    log::error!("Could not unpack file in archive...");
                }
            }
        }
        Err(err) => {
            log::error!("Could not unpack tree database from archive...");
            log::error!("Err: {:?}", err);
        }
    }
}

fn unpack_entry_tarball(hidden_dir: &Path, archive: &mut Archive<GzDecoder<&[u8]>>) {
    // Unpack and compute HASH and save next to the file to speed up computation later
    // log::debug!("unpack_entry_tarball hidden_dir {:?}", hidden_dir);

    match archive.entries() {
        Ok(entries) => {
            for file in entries {
                match file {
                    Ok(mut file) => {
                        // Why hash now? To make sure everything synced properly
                        // When we want to check is_synced, it is expensive to rehash everything
                        // But since upload is network bound already, hashing here makes sense, and we will just
                        // load the HASH file later
                        let path = file.path().unwrap();
                        let mut version_path = PathBuf::from(hidden_dir);
                        // log::debug!("unpack_entry_tarball path {:?}", path);

                        if path.starts_with("versions") && path.to_string_lossy().contains("files")
                        {
                            // Unpack version files to common name (data.extension) regardless of the name sent from the client
                            let new_path = util::fs::replace_file_name_keep_extension(
                                &path,
                                VERSION_FILE_NAME.to_owned(),
                            );
                            version_path.push(new_path);
                            // log::debug!("unpack_entry_tarball version_path {:?}", version_path);

                            if let Some(parent) = version_path.parent() {
                                if !parent.exists() {
                                    std::fs::create_dir_all(parent)
                                        .expect("Could not create parent dir");
                                }
                            }
                            file.unpack(&version_path).unwrap();
                            // log::debug!("unpack_entry_tarball unpacked! {:?}", version_path);

                            let hash_dir = version_path.parent().unwrap();
                            let hash_file = hash_dir.join(HASH_FILE);
                            let hash = util::hasher::hash_file_contents(&version_path).unwrap();
                            util::fs::write_to_path(&hash_file, &hash)
                                .expect("Could not write hash file");
                        } else if path.starts_with(OBJECTS_DIR) {
                            let temp_objects_dir = hidden_dir.join("tmp");
                            if !temp_objects_dir.exists() {
                                std::fs::create_dir_all(&temp_objects_dir).unwrap();
                            }

                            file.unpack_in(&temp_objects_dir).unwrap();
                        } else {
                            // For non-version files, use filename sent by client
                            file.unpack_in(hidden_dir).unwrap();
                        }
                    }
                    Err(err) => {
                        log::error!("Could not unpack file in archive...");
                        log::error!("Err: {:?}", err);
                    }
                }
            }
        }
        Err(err) => {
            log::error!("Could not unpack entries from archive...");
            log::error!("Err: {:?}", err);
        }
    }

    log::debug!("Done decompressing.");
}

#[cfg(test)]
mod tests {

    use actix_web::body::to_bytes;
    use actix_web::{web, App};
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::path::Path;
    use std::thread;

    use liboxen::constants::OXEN_HIDDEN_DIR;
    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;
    use liboxen::view::{ListCommitResponse, StatusMessage};

    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::params::PageNumQuery;
    use crate::test::{self, init_test_env};

    #[actix_web::test]
    async fn test_controllers_commits_index_empty() -> Result<(), OxenError> {
        init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let name = "Testing-Name";
        test::create_local_repo(&sync_dir, namespace, name)?;

        let uri = format!("/oxen/{namespace}/{name}/commits");
        let req = test::repo_request(&sync_dir, &uri, namespace, name);

        let resp = controllers::commits::index(req).await.unwrap();

        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListCommitResponse = serde_json::from_str(text)?;
        assert_eq!(list.commits.len(), 0);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_commits_list_two_commits() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;

        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "hello")?;
        repositories::add(&repo, path)?;
        repositories::commit(&repo, "first commit")?;
        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "world")?;
        repositories::add(&repo, path)?;
        repositories::commit(&repo, "second commit")?;

        let uri = format!("/oxen/{namespace}/{name}/commits");
        let req = test::repo_request(&sync_dir, &uri, namespace, name);

        let resp = controllers::commits::index(req).await.unwrap();
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListCommitResponse = serde_json::from_str(text)?;
        assert_eq!(list.commits.len(), 2);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_commits_list_commits_on_branch() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;

        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "hello")?;
        repositories::add(&repo, path)?;
        repositories::commit(&repo, "first commit")?;

        let branch_name = "feature/list-commits";
        repositories::branches::create_checkout(&repo, branch_name)?;

        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "world")?;
        repositories::add(&repo, path)?;
        repositories::commit(&repo, "second commit")?;

        let uri = format!("/oxen/{namespace}/{repo_name}/commits/history/{branch_name}");
        let req = test::repo_request_with_param(
            &sync_dir,
            &uri,
            namespace,
            repo_name,
            "resource",
            branch_name,
        );

        let query: web::Query<PageNumQuery> =
            web::Query::from_query("page=1&page_size=10").unwrap();
        let resp = controllers::commits::history(req, query).await.unwrap();
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListCommitResponse = serde_json::from_str(text)?;
        assert_eq!(list.commits.len(), 2);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    // Switch branches, add a commit, and only list commits from first branch
    #[actix_web::test]
    async fn test_controllers_commits_list_some_commits_on_branch() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file)?;
        repositories::commit(&repo, "First commit")?;
        let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "hello")?;
        repositories::add(&repo, path)?;
        repositories::commit(&repo, "first commit")?;

        let branch_name = "feature/list-commits";
        repositories::branches::create_checkout(&repo, branch_name)?;

        let path = liboxen::test::add_txt_file_to_dir(&repo.path, "world")?;
        repositories::add(&repo, path)?;
        repositories::commit(&repo, "second commit")?;

        // List commits from the first branch
        let uri = format!(
            "/oxen/{}/{}/commits/history/{}",
            namespace, repo_name, og_branch.name
        );
        let req = test::repo_request_with_param(
            &sync_dir,
            &uri,
            namespace,
            repo_name,
            "resource",
            og_branch.name,
        );

        let query: web::Query<PageNumQuery> =
            web::Query::from_query("page=1&page_size=10").unwrap();
        let resp = controllers::commits::history(req, query).await.unwrap();
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListCommitResponse = serde_json::from_str(text)?;
        // there should be 2 instead of the 3 total
        assert_eq!(list.commits.len(), 2);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_commits_upload() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        let hello_file = repo.path.join("hello.txt");
        util::fs::write_to_path(&hello_file, "Hello")?;
        repositories::add(&repo, &hello_file)?;
        let commit = repositories::commit(&repo, "First commit")?;

        // create random tarball to post.. currently no validation that it is a valid commit dir
        let path_to_compress = format!("history/{}", commit.id);
        let commit_dir_name = format!("data/test/runs/{}", commit.id);
        let commit_dir = Path::new(&commit_dir_name);
        std::fs::create_dir_all(commit_dir)?;
        // Write a random file to it
        let zipped_filename = "blah.txt";
        let zipped_file_contents = "sup";
        let random_file = commit_dir.join(zipped_filename);
        util::fs::write_to_path(&random_file, zipped_file_contents)?;

        println!("Compressing commit {}...", commit.id);
        let enc = GzEncoder::new(Vec::new(), Compression::default());
        let mut tar = tar::Builder::new(enc);

        tar.append_dir_all(&path_to_compress, commit_dir)?;
        tar.finish()?;
        let payload: Vec<u8> = tar.into_inner()?.finish()?;
        println!("Uploading commit {}... {} bytes", commit.id, payload.len());

        let uri = format!("/oxen/{}/{}/commits/upload", namespace, repo_name);
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone()))
                .route(
                    "/oxen/{namespace}/{repo_name}/commits/upload",
                    web::post().to(controllers::commits::upload),
                ),
        )
        .await;

        let req = actix_web::test::TestRequest::post()
            .uri(&uri)
            .set_payload(payload)
            .to_request();

        let resp = actix_web::test::call_service(&app, req).await;
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        println!("Upload response: {}", body);
        let resp: StatusMessage = serde_json::from_str(body)?;
        assert_eq!(resp.status, "success");

        // We unzip in a background thread, so give it a second
        thread::sleep(std::time::Duration::from_secs(1));

        // Make sure we unzipped the tar ball
        let uploaded_file = sync_dir
            .join(namespace)
            .join(repo_name)
            .join(OXEN_HIDDEN_DIR)
            .join(path_to_compress)
            .join(zipped_filename);
        println!("Looking for file: {uploaded_file:?}");
        assert!(uploaded_file.exists());
        assert_eq!(
            util::fs::read_from_path(&uploaded_file)?,
            zipped_file_contents
        );

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;
        util::fs::remove_dir_all(commit_dir)?;

        Ok(())
    }
}
