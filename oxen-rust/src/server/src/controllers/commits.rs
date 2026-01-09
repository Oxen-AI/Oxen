use liboxen::constants;
use liboxen::constants::COMMITS_DIR;
use liboxen::constants::DIRS_DIR;
use liboxen::constants::DIR_HASHES_DIR;
use liboxen::constants::HISTORY_DIR;
use liboxen::constants::VERSION_FILE_NAME;

use liboxen::core::commit_sync_status;
use liboxen::error::OxenError;
use liboxen::model::{Commit, LocalRepository};
use liboxen::opts::PaginateOpts;
use liboxen::perf_guard;
use liboxen::repositories;
use liboxen::util;
use liboxen::view::branch::BranchName;
use liboxen::view::compare::{CompareCommits, CompareCommitsResponse};
use liboxen::view::entries::ListCommitEntryResponse;
use liboxen::view::tree::merkle_hashes::MerkleHashes;
use liboxen::view::MerkleHashesResponse;
use liboxen::view::{
    CommitResponse, ListCommitResponse, PaginatedCommits, Pagination, RootCommitResponse,
    StatusMessage,
};

use crate::app_data::OxenAppData;
use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::parse_resource;
use crate::params::PageNumQuery;
use crate::params::{app_data, parse_base_head, path_param, resolve_base_head};

use actix_web::{web, Error, HttpRequest, HttpResponse};
use async_compression::tokio::bufread::GzipDecoder;
use bytesize::ByteSize;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::stream::StreamExt as _;
use os_path::OsPath;
use serde::Deserialize;
use std::fs::OpenOptions;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use tokio::io::BufReader;
use tokio_tar::Archive;
use utoipa::IntoParams;

#[derive(Deserialize, Debug, IntoParams)]
pub struct ChunkedDataUploadQuery {
    #[param(example = "a2c3d4e5f67890b1c2d3e4f5a6b7c8d9")]
    pub hash: String,
    #[param(example = 1)]
    pub chunk_num: usize,
    #[param(example = 10)]
    pub total_chunks: usize,
    #[param(example = 100000000)]
    pub total_size: usize,
    #[param(example = true)]
    pub is_compressed: bool,
    #[param(example = "images/cow.jpg")]
    pub filename: Option<String>,
}

#[derive(Deserialize, IntoParams)]
pub struct ListMissingFilesQuery {
    #[param(example = "abc1234567890def1234567890fedcba")]
    pub base: Option<String>,
    #[param(example = "84c76a5b2e9a2637f9091991475c404d")]
    pub head: String,
}

/// Get commit history
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/commits/history/{resource}",
    operation_id = "commit_history",
    tag = "Commits",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("resource" = String, Path, description = "Commit ID, Branch name, or path to a file/directory", example = "main/data/train/image.jpg"),
        PageNumQuery
    ),
    responses(
        (status = 200, description = "Paginated list of commits with total count and cache status", body = PaginatedCommits),
        (status = 404, description = "Repository or resource not found")
    )
)]
pub async fn history(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> Result<HttpResponse, OxenHttpError> {
    let _perf = perf_guard!("commits::history_endpoint");

    let _perf_parse = perf_guard!("commits::history_parse_params");
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
    drop(_perf_parse);

    log::debug!("commit_history resource_param: {resource_param:?}");

    let _perf_resource = perf_guard!("commits::history_parse_resource");
    // This checks if the parameter received from the client is two commits split by "..", in this case we don't parse the resource
    let (resource, revision, commit) = if resource_param.contains("..") {
        (None, Some(resource_param), None)
    } else {
        let resource = parse_resource(&req, &repo)?;
        let commit = resource.clone().commit.ok_or(OxenHttpError::NotFound)?;
        (Some(resource), None, Some(commit))
    };
    drop(_perf_resource);

    match &resource {
        Some(resource) if resource.path != Path::new("") => {
            log::debug!("commit_history resource_param: {resource:?}");
            let _perf_list = perf_guard!("commits::history_list_by_path");
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
            log::debug!("commit_history revision: {revision:?}");
            let revision_id = revision.as_ref().or_else(|| commit.as_ref().map(|c| &c.id));
            if let Some(revision_id) = revision_id {
                let _perf_list = perf_guard!("commits::history_list_from_revision");
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

/// List all commits
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/commits/all",
    operation_id = "list_all_commits",
    description = "List all commits in a repository",
    tag = "Commits",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        PageNumQuery
    ),
    responses(
        (status = 200, description = "Paginated list of all commits", body = PaginatedCommits),
        (status = 404, description = "Repository not found")
    )
)]
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

/// List commits
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/compare/{base_head}/commits",
    description = "List the commits between the provided base commit and head commit",
    tag = "list_between",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "satellite-images"),
        ("base_head" = String, Path, description = "The base and head revisions separated by '..'", example = "main..feature/add-labels"),
        ("page" = Option<usize>, Query, description = "Page number for pagination (starts at 1)"),
        ("page_size" = Option<usize>, Query, description = "Page size for pagination")
    ),
    responses(
        (status = 200, description = "Commits found successfully", body = CompareCommitsResponse),
        (status = 404, description = "Repository or one of the revisions not found")
    )
)]
pub async fn list_commits(
    req: HttpRequest,
    query: web::Query<PageNumQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let base_head = path_param(&req, "base_head")?;

    // Get the repository or return error
    let repository = get_repo(&app_data.path, namespace, name)?;

    // Page size and number
    let page = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    // Parse the base and head from the base..head string
    let (base, head) = parse_base_head(&base_head)?;
    let (base_commit, head_commit) = resolve_base_head(&repository, &base, &head)?;

    let base_commit = base_commit.ok_or(OxenError::revision_not_found(base.into()))?;
    let head_commit = head_commit.ok_or(OxenError::revision_not_found(head.into()))?;

    let commits = repositories::commits::list_between(&repository, &base_commit, &head_commit)?;
    let (paginated, pagination) = util::paginate(commits, page, page_size);

    let compare = CompareCommits {
        base_commit,
        head_commit,
        commits: paginated,
    };

    let view = CompareCommitsResponse {
        status: StatusMessage::resource_found(),
        compare,
        pagination,
    };
    Ok(HttpResponse::Ok().json(view))
}

/// List missing commits
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/commits/missing",
    operation_id = "list_missing_commits",
    description = "From a list of commit hashes, list the ones not present on the server",
    tag = "Commits",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    request_body(
        content = MerkleHashes,
        description = "List of commit hashes present on the client.",
        example = json!({
            "hashes": ["abc1234567890def1234567890fedcba", "84c76a5b2e9a2637f9091991475c404d"]
        })
    ),
    responses(
        (status = 200, description = "List of commit hashes missing on the server", body = MerkleHashesResponse),
        (status = 400, description = "Invalid JSON body"),
        (status = 404, description = "Repository not found")
    )
)]
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
        log::error!("list_missing invalid JSON: {body:?}");
        return Ok(HttpResponse::BadRequest().json(StatusMessage::error("Invalid JSON")));
    };

    log::debug!(
        "list_missing checking {} commit hashes",
        merkle_hashes.hashes.len()
    );
    let missing_commits =
        repositories::tree::list_unsynced_commit_hashes(&repo, &merkle_hashes.hashes)?;
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

/// List missing files from commits
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/commits/missing_files",
    operation_id = "list_missing_files",
    description = "Lists files that are referenced in a commit, but not present on the server. Accepts a commit range",
    tag = "Commits",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ListMissingFilesQuery
    ),
    responses(
        (status = 200, description = "List of missing file entries", body = ListCommitEntryResponse),
        (status = 404, description = "Repository or commit not found")
    )
)]
pub async fn list_missing_files(
    req: HttpRequest,
    query: web::Query<ListMissingFilesQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let base_commit = match &query.base {
        Some(base) => repositories::commits::get_by_id(&repo, base)?,
        None => None,
    };

    let head_commit = repositories::commits::get_by_id(&repo, &query.head)?
        .ok_or(OxenError::revision_not_found(query.head.clone().into()))?;

    let missing_files = repositories::entries::list_missing_files_in_commit_range(
        &repo,
        &base_commit,
        &head_commit,
    )
    .await?;

    let response = ListCommitEntryResponse {
        status: StatusMessage::resource_found(),
        entries: missing_files,
    };
    Ok(HttpResponse::Ok().json(response))
}

/// Mark commits as synced
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/commits/synced",
    operation_id = "mark_commits_synced",
    tag = "Commits",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    request_body(
        content = MerkleHashes,
        description = "List of commit hashes successfully synced to the server.",
        example = json!({
            "hashes": ["abc1234567890def1234567890fedcba", "84c76a5b2e9a2637f9091991475c404d"]
        })
    ),
    responses(
        (status = 200, description = "Commits marked as synced", body = MerkleHashesResponse),
        (status = 404, description = "Repository not found")
    )
)]
pub async fn mark_commits_as_synced(
    req: HttpRequest,
    mut body: web::Payload,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.map_err(|_| OxenHttpError::FailedToReadRequestPayload)?);
    }

    let request: MerkleHashes = serde_json::from_slice(&bytes)?;
    let hashes = request.hashes;
    log::debug!(
        "mark_commits_as_synced marking {} commit hashes",
        &hashes.len()
    );

    for hash in &hashes {
        commit_sync_status::mark_commit_as_synced(&repository, hash)?;
    }

    log::debug!("successfully marked {} commit hashes", &hashes.len());
    Ok(HttpResponse::Ok().json(MerkleHashesResponse {
        status: StatusMessage::resource_found(),
        hashes,
    }))
}

/// Get commit
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/commits/{commit_id}",
    operation_id = "get_commit",
    tag = "Commits",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("commit_id" = String, Path, description = "Hash ID of the commit", example = "84c76a5b2e9a2637f9091991475c404d"),
    ),
    responses(
        (status = 200, description = "Commit", body = CommitResponse),
        (status = 404, description = "Commit not found")
    )
)]
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

/// Get a commit's parents
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/commits/{commit_or_branch}/parents",
    operation_id = "get_commit_parents",
    tag = "Commits",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("commit_or_branch" = String, Path, description = "Commit ID or Branch name", example = "main"),
    ),
    responses(
        (status = 200, description = "List of parent commits", body = ListCommitResponse),
        (status = 404, description = "Commit or Branch not found")
    )
)]
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

/// Download commits DB
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/commits_db",
    operation_id = "download_commits_db",
    tag = "Commits",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    responses(
        (status = 200, description = "Tarball of commits DB"),
        (status = 404, description = "Repository not found")
    )
)]
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

    log::debug!("Compressing commit db from dir {commit_dir:?}");
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    tar.append_dir_all(tar_subdir, commit_dir)?;
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!("Compressed commit dir size is {}", ByteSize::b(total_size));

    Ok(buffer)
}

/// Download dir hashes DB
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/commits/{base_head}/dir_hashes_db",
    operation_id = "download_dir_hashes_db",
    tag = "Commits",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("base_head" = String, Path, description = "Commit ID range (base..head) or single commit ID", example = "abc1234..84c76a5"),
    ),
    responses(
        (status = 200, description = "Tarball of dir hashes DB"),
        (status = 400, description = "Invalid base_head format"),
        (status = 404, description = "Repository or commit not found")
    )
)]
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

        repositories::commits::list_between(&repository, &base_commit, &head_commit)?
    } else {
        repositories::commits::list_from(&repository, &base_head)?
    };
    let buffer = compress_commits(&repository, &commits)?;

    Ok(HttpResponse::Ok().body(buffer))
}

/// Download commit entries DB
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/commits/{commit_or_branch}/commit_entries_db",
    operation_id = "download_commit_entries_db",
    tag = "Commits",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("commit_or_branch" = String, Path, description = "Commit ID or Branch name", example = "main"),
    ),
    responses(
        (status = 200, description = "Tarball of commit entries DB"),
        (status = 404, description = "Repository or commit not found")
    )
)]
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

/// Upload commit
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/commits",
    operation_id = "create_commit",
    description = "Uploads a commit to a branch the server. This will create an empty commit. To create a commit with children, use the upload_tree endpoint",
    tag = "Commits",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    request_body(
        content = Commit,
        description = "Commit object and target branch name.",
        example = json!({
            "author": "Bessie Oxington",
            "email": "bessie@oxen.ai",
            "message": "Empty commit for testing",
            "id": "abc1234567890def1234567890fedcba",
            "branch_name": "main"
        }),
    ),
    responses(
        (status = 200, description = "Commit created", body = CommitResponse),
        (status = 400, description = "Invalid commit data or mismatched remote history"),
    )
)]
pub async fn create(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("Got commit data: {body}");

    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

    let new_commit: Commit = match serde_json::from_str(&body) {
        Ok(commit) => commit,
        Err(_) => {
            log::error!("commits create got invalid commit data {body}");
            return Err(OxenHttpError::BadRequest("Invalid commit data".into()));
        }
    };
    log::debug!("commits create got new commit: {new_commit:?}");

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
            log::error!("Err create_commit: RootCommitDoesNotMatch {commit_id}");
            Err(OxenHttpError::BadRequest("Remote commit history does not match local commit history. Make sure you are pushing to the correct remote.".into()))
        }
        Err(err) => {
            log::error!("Err create_commit: {err}");
            Err(OxenHttpError::InternalServerError)
        }
    }
}

/// Upload data chunk
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/commits/upload_chunk",
    operation_id = "upload_chunk",
    description = "Uploads a chunk of file data, for use in large file uploads",
    tag = "Commits",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ChunkedDataUploadQuery
    ),
    request_body(
        content_type = "application/octet-stream",
        description = "Chunk of data (binary bytes)",
        content = Vec<u8>
    ),
    responses(
        (status = 200, description = "Chunk uploaded successfully", body = StatusMessage),
    )
)]
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
            log::error!("upload_chunk could not complete chunk upload, mkdir failed: {err:?}");
            return Ok(
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            );
        }
    }

    // Read bytes from body
    let mut bytes = web::BytesMut::new();
    while let Some(item) = chunk.next().await {
        bytes.extend_from_slice(&item.map_err(|_| OxenHttpError::FailedToReadRequestPayload)?);
    }

    // Write to tmp file
    log::debug!("upload_chunk writing file {chunk_file:?}");
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
                    log::debug!("upload_chunk successfully wrote chunk {chunk_file:?}");

                    // TODO: there is a race condition here when multiple chunks
                    // are uploaded in parallel Currently doesn't hurt anything,
                    // but we should find a more elegant solution because we're
                    // doing a lot of extra work unpacking tarballs multiple
                    // times.
                    check_if_upload_complete_and_unpack(
                        &repo,
                        tmp_dir,
                        total_chunks,
                        size,
                        query.is_compressed,
                        query.filename.to_owned(),
                    )
                    .await;

                    Ok(HttpResponse::Ok().json(StatusMessage::resource_created()))
                }
                Err(err) => {
                    log::error!(
                        "upload_chunk could not complete chunk upload, file write_all failed: {err:?} -> {chunk_file:?}"
                    );
                    Ok(HttpResponse::InternalServerError()
                        .json(StatusMessage::internal_server_error()))
                }
            }
        }
        Err(err) => {
            log::error!(
                "upload_chunk could not complete chunk upload, file create failed: {err:?} -> {chunk_file:?}"
            );
            Ok(HttpResponse::InternalServerError().json(StatusMessage::internal_server_error()))
        }
    }
}

async fn check_if_upload_complete_and_unpack(
    repo: &LocalRepository,
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
                log::warn!("Err getting metadata on {file:?}\n{err:?}");
            }
        }
    }

    log::debug!(
        "check_if_upload_complete_and_unpack checking if complete... {uploaded_size} / {total_size}"
    );

    // I think windows has a larger size than linux...so can't do a simple check here
    // But if we have all the chunks we should be good

    if (uploaded_size as usize) >= total_size {
        // std::thread::spawn(move || {
        // Get tar.gz bytes for history/COMMIT_ID data
        log::debug!(
            "check_if_upload_complete_and_unpack decompressing {} bytes to {:?}",
            total_size,
            repo.path
        );

        // TODO: Cleanup these if / else / match statements
        // Combine into actual file data
        if is_compressed {
            match unpack_compressed_data(&files, repo).await {
                Ok(_) => {
                    log::debug!(
                        "check_if_upload_complete_and_unpack unpacked {} files successfully",
                        files.len()
                    );
                }
                Err(err) => {
                    log::error!(
                        "check_if_upload_complete_and_unpack could not unpack compressed data {err:?}"
                    );
                }
            }
        } else {
            match filename {
                Some(filename) => {
                    match unpack_to_file(&files, repo, &filename) {
                        Ok(_) => {
                            log::debug!("check_if_upload_complete_and_unpack unpacked {} files successfully", files.len());
                        }
                        Err(err) => {
                            log::error!("check_if_upload_complete_and_unpack could not unpack compressed data {err:?}");
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
                log::debug!("check_if_upload_complete_and_unpack removed tmp dir {tmp_dir:?}");
            }
            Err(err) => {
                log::error!(
                    "check_if_upload_complete_and_unpack could not remove tmp dir {tmp_dir:?} {err:?}"
                );
            }
        }
        // });
    }
}

fn unpack_to_file(
    files: &[PathBuf],
    repo: &LocalRepository,
    filename: &str,
) -> Result<(), OxenError> {
    // Append each buffer to the end of the large file
    // TODO: better error handling...
    log::debug!("Got filename {filename}");

    // return path with native slashes
    let os_path = OsPath::from(filename).to_pathbuf();
    log::debug!("Got native filename {os_path:?}");

    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
    let mut full_path = hidden_dir.join(os_path);
    full_path =
        util::fs::replace_file_name_keep_extension(&full_path, VERSION_FILE_NAME.to_owned());
    log::debug!("Unpack to {full_path:?}");
    if let Some(parent) = full_path.parent() {
        util::fs::create_dir_all(parent)?;
    }

    let mut outf = std::fs::File::create(&full_path)
        .map_err(|e| OxenError::file_create_error(&full_path, e))?;

    for file in files.iter() {
        log::debug!("Reading file bytes {file:?}");
        let mut buffer: Vec<u8> = Vec::new();

        let mut f = std::fs::File::open(file).map_err(|e| OxenError::file_open_error(file, e))?;

        f.read_to_end(&mut buffer)
            .map_err(|e| OxenError::file_read_error(file, e))?;

        log::debug!("Read {} file bytes from file {:?}", buffer.len(), file);

        match outf.write_all(&buffer) {
            Ok(_) => {
                log::debug!("Unpack successful! {full_path:?}");
            }
            Err(err) => {
                log::error!("Could not write all data to disk {err:?}");
            }
        }
    }
    Ok(())
}

async fn unpack_compressed_data(
    files: &[PathBuf],
    repo: &LocalRepository,
) -> Result<(), OxenError> {
    let mut buffer: Vec<u8> = Vec::new();
    for file in files.iter() {
        log::debug!("Reading file bytes {file:?}");
        let mut f = std::fs::File::open(file).map_err(|e| OxenError::file_open_error(file, e))?;

        f.read_to_end(&mut buffer)
            .map_err(|e| OxenError::file_read_error(file, e))?;
    }

    // Unpack tarball to our hidden dir using async streaming
    unpack_entry_tarball_async(repo, &buffer).await?;

    Ok(())
}

/// Upload commits DB
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/commits/upload",
    operation_id = "upload_commits_db",
    tag = "Commits",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    request_body(
        content_type = "application/octet-stream",
        description = "Compressed commit database (tar.gz)",
        content = Vec<u8>
    ),
    responses(
        (status = 200, description = "Commits DB uploaded successfully", body = StatusMessage),
    )
)]
pub async fn upload(
    req: HttpRequest,
    mut body: web::Payload, // the actual file body
) -> Result<HttpResponse, OxenHttpError> {
    log::debug!("in regular upload controller");
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &name)?;

    // Read bytes from body
    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.map_err(|_| OxenHttpError::FailedToReadRequestPayload)?);
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
    log::debug!(
        "Decompressing {} bytes to repo at {}",
        bytes.len(),
        repo.path.display()
    );
    // Unpack tarball to repo using async streaming
    unpack_entry_tarball_async(&repo, &bytes).await?;
    // });

    Ok(HttpResponse::Ok().json(StatusMessage::resource_created()))
}

/// Notify upload complete
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/commits/{commit_id}/complete",
    operation_id = "commit_upload_complete",
    description = "Notifies the server that the commit has finished uploading.",
    tag = "Commits",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("commit_id" = String, Path, description = "ID of the commit to complete", example = "84c76a5b2e9a2637f9091991475c404d"),
    ),
    responses(
        (status = 200, description = "Commit push completed successfully", body = CommitResponse),
        (status = 404, description = "Repository or commit not found"),
    )
)]
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
                    log::error!("Could not find commit [{commit_id}]");
                    Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
                }
                Err(err) => {
                    log::error!("Error finding commit [{commit_id}]: {err}");
                    Ok(HttpResponse::InternalServerError()
                        .json(StatusMessage::internal_server_error()))
                }
            }
        }
        Ok(None) => {
            log::debug!("404 could not get repo {repo_name}",);
            Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
        }
        Err(repo_err) => {
            log::error!("Err get_by_name: {repo_err}");
            Ok(HttpResponse::InternalServerError().json(StatusMessage::internal_server_error()))
        }
    }
}

/// Upload commit tree
#[utoipa::path(
    post,
    path = "/api/repos/{namespace}/{repo_name}/commits/{commit_id}/upload_tree",
    operation_id = "upload_tree",
    tag = "Commits",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
        ("commit_id" = String, Path, description = "Client head commit ID", example = "84c76a5b2e9a2637f9091991475c404d"),
    ),
    request_body(
        content_type = "application/octet-stream",
        description = "Compressed tree data (tar.gz)",
        content = Vec<u8>
    ),
    responses(
        (status = 200, description = "Tree uploaded successfully", body = CommitResponse),
    )
)]
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
        bytes.extend_from_slice(&item.map_err(|_| OxenHttpError::FailedToReadRequestPayload)?);
    }

    let total_size: u64 = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    log::debug!(
        "Got compressed data for tree {} -> {}",
        client_head_id,
        ByteSize::b(total_size)
    );

    log::debug!("Decompressing {} bytes to {:?}", bytes.len(), tmp_dir);

    // let mut archive = Archive::new(GzDecoder::new(&bytes[..]));

    unpack_tree_tarball(&tmp_dir, &bytes).await?;

    Ok(HttpResponse::Ok().json(CommitResponse {
        status: StatusMessage::resource_found(),
        commit: server_head_commit.to_owned(),
    }))
}

/// Get root commit
#[utoipa::path(
    get,
    path = "/api/repos/{namespace}/{repo_name}/commits/root",
    operation_id = "get_root_commit",
    tag = "Commits",
    security( ("api_key" = []) ),
    params(
        ("namespace" = String, Path, description = "Namespace of the repository", example = "ox"),
        ("repo_name" = String, Path, description = "Name of the repository", example = "ImageNet-1k"),
    ),
    responses(
        (status = 200, description = "Root commit found (None if empty repository)", body = RootCommitResponse),
    )
)]
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

async fn unpack_tree_tarball(tmp_dir: &Path, data: &[u8]) -> Result<(), OxenError> {
    let reader = Cursor::new(data);
    let buf_reader = BufReader::new(reader);
    let decoder = GzipDecoder::new(buf_reader);
    let mut archive = Archive::new(decoder);

    let mut entries = match archive.entries() {
        Ok(entries) => entries,
        Err(e) => {
            log::error!("Could not unpack tree database from archive...");
            log::error!("Err: {e:?}");
            return Err(OxenError::basic_str("Failed to get archive entries"));
        }
    };

    while let Some(entry) = entries.next().await {
        if let Ok(mut file) = entry {
            let path = file.path().unwrap();
            log::debug!("unpack_tree_tarball path {path:?}");
            let stripped_path = if path.starts_with(HISTORY_DIR) {
                match path.strip_prefix(HISTORY_DIR) {
                    Ok(stripped) => stripped,
                    Err(err) => {
                        log::error!("Could not strip prefix from path {err:?}");
                        return Err(OxenError::basic_str("Failed to strip path prefix"));
                    }
                }
            } else {
                &path
            };

            let mut new_path = PathBuf::from(tmp_dir);
            new_path.push(stripped_path);

            if let Some(parent) = new_path.parent() {
                util::fs::create_dir_all(parent).expect("Could not create parent dir");
            }
            log::debug!("unpack_tree_tarball new_path {path:?}");
            file.unpack(&new_path).await.unwrap();
        } else {
            log::error!("Could not unpack file in archive...");
        }
    }

    Ok(())
}

async fn unpack_entry_tarball_async(
    repo: &LocalRepository,
    compressed_data: &[u8],
) -> Result<(), OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
    let version_store = repo.version_store()?;

    // Create async gzip decoder and tar archive
    let reader = Cursor::new(compressed_data);
    let buf_reader = BufReader::new(reader);
    let decoder = GzipDecoder::new(buf_reader);
    let mut archive = Archive::new(decoder);

    // Process entries asynchronously
    let mut entries = archive.entries()?;
    while let Some(entry) = entries.next().await {
        let mut file = entry?;
        let path = file
            .path()
            .map_err(|e| OxenError::basic_str(format!("Invalid path in archive: {e}")))?;

        if path.starts_with("versions") && path.to_string_lossy().contains("files") {
            // Handle version files with streaming
            let hash = extract_hash_from_path(&path)?;

            // Convert futures::io::AsyncRead to tokio::io::AsyncRead using compat
            // let mut tokio_reader = file.compat();

            // Use streaming storage - no memory buffering needed!
            version_store
                .store_version_from_reader(&hash, &mut file)
                .await?;
        } else {
            // For non-version files, unpack to hidden dir
            file.unpack_in(&hidden_dir)
                .await
                .map_err(|e| OxenError::basic_str(format!("Failed to unpack file: {e}")))?;
        }
    }

    log::debug!("Done decompressing with async streaming.");
    Ok(())
}

// Helper function to extract the content hash from a version file path
fn extract_hash_from_path(path: &Path) -> Result<String, OxenError> {
    // Path structure is: versions/files/XX/YYYYYYYY/data
    // where XXYYYYYYYY is the content hash

    // Split the path and look for the pattern
    let parts: Vec<_> = path.components().map(|comp| comp.as_os_str()).collect();
    if parts.len() >= 5 && parts[0] == "versions" && parts[1] == "files" {
        // The hash is composed of the directory names: XX/YYYYYYYY
        let top_dir = parts[2];
        let sub_dir = parts[3];

        // Ensure we have a valid hash structure
        if top_dir.len() == 2 && !sub_dir.is_empty() {
            return Ok(format!(
                "{}{}",
                top_dir.to_string_lossy(),
                sub_dir.to_string_lossy()
            ));
        }
    }

    Err(OxenError::basic_str(format!(
        "Could not get hash for file: {path:?}"
    )))
}
