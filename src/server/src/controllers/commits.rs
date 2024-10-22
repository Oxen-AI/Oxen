use liboxen::constants;
use liboxen::constants::COMMITS_DIR;
use liboxen::constants::DIRS_DIR;
use liboxen::constants::DIR_HASHES_DIR;
use liboxen::constants::HASH_FILE;
use liboxen::constants::HISTORY_DIR;
use liboxen::constants::OBJECTS_DIR;
use liboxen::constants::TREE_DIR;
use liboxen::constants::VERSION_FILE_NAME;

// TODO: Move all the v0.10.0 modules out of controllers so it is more abstracted
use liboxen::core::v0_10_0::cache::cacher_status::CacherStatusType;
use liboxen::core::v0_10_0::cache::cachers::content_validator;
use liboxen::core::v0_10_0::cache::commit_cacher;
use liboxen::core::v0_10_0::commits::create_commit_object;
use liboxen::core::v0_10_0::commits::create_commit_object_with_committers;
use liboxen::core::v0_10_0::commits::head_commits_have_conflicts;
use liboxen::core::v0_10_0::commits::list_with_missing_dbs;
use liboxen::core::v0_10_0::commits::merge_objects_dbs;
use liboxen::core::v0_10_0::index::CommitReader;
use liboxen::core::v0_10_0::index::CommitWriter;
use liboxen::core::versions::MinOxenVersion;

use liboxen::core::refs::RefWriter;
use liboxen::error::OxenError;
use liboxen::model::commit::CommitWithBranchName;
use liboxen::model::RepoNew;
use liboxen::model::{Commit, LocalRepository};
use liboxen::opts::PaginateOpts;
use liboxen::repositories;
use liboxen::util;
use liboxen::view::branch::BranchName;
use liboxen::view::commit::CommitSyncStatusResponse;
use liboxen::view::commit::CommitTreeValidationResponse;
use liboxen::view::commit::UploadCommitResponse;
use liboxen::view::http::MSG_CONTENT_IS_INVALID;
use liboxen::view::http::MSG_FAILED_PROCESS;
use liboxen::view::http::MSG_INTERNAL_SERVER_ERROR;
use liboxen::view::http::MSG_RESOURCE_IS_PROCESSING;
use liboxen::view::http::STATUS_ERROR;
use liboxen::view::http::{MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::tree::merkle_hashes::MerkleHashes;
use liboxen::view::{
    CommitResponse, IsValidStatusMessage, ListCommitResponse, PaginatedCommits, Pagination,
    RootCommitResponse, StatusMessage,
};
use os_path::OsPath;

use crate::app_data::OxenAppData;
use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::parse_resource;
use crate::params::PageNumQuery;
use crate::params::{app_data, path_param};
use crate::tasks;
use crate::tasks::post_push_complete::PostPushComplete;

use actix_web::{web, Error, HttpRequest, HttpResponse};
use bytesize::ByteSize;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::stream::StreamExt as _;
use serde::Deserialize;
use std::collections::HashMap;
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

pub async fn commit_history(
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
            Ok(HttpResponse::Ok().json(commits))
        }
        _ => {
            // Handling the case where resource is None or its path is empty
            log::debug!("commit_history revision: {:?}", revision);
            let revision_id = revision.as_ref().or_else(|| commit.as_ref().map(|c| &c.id));
            if let Some(revision_id) = revision_id {
                let commits =
                    repositories::commits::list_from_paginated(&repo, revision_id, pagination)?;
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
        return Ok(HttpResponse::BadRequest().json(StatusMessage::error("Invalid JSON")));
    };

    let missing_commits =
        repositories::tree::list_missing_node_hashes(&repo, &merkle_hashes.hashes)?;
    Ok(HttpResponse::Ok().json(missing_commits))
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

/// TODO: Depreciate this API - not good to send the full commit list separately from objects in the tree
///       We should just have commits be an object, and send them all last
pub async fn commits_db_status(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let commit_id = path_param(&req, "commit_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let commits_to_sync = list_with_missing_dbs(&repo, &commit_id)?;

    log::debug!(
        "About to respond with {} commits to sync",
        commits_to_sync.len()
    );

    Ok(HttpResponse::Ok().json(ListCommitResponse {
        status: StatusMessage::resource_found(),
        commits: commits_to_sync,
    }))
}

/// TODO: Depreciate this after v0.19.0
pub async fn entries_status(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let commit_id = path_param(&req, "commit_id")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let commits_to_sync = repositories::commits::list_with_missing_entries(&repo, &commit_id)?;

    Ok(HttpResponse::Ok().json(ListCommitResponse {
        status: StatusMessage::resource_found(),
        commits: commits_to_sync,
    }))
}

pub async fn latest_synced(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;
    let commit_id = path_param(&req, "commit_id")?;

    let commits = repositories::commits::list_from(&repository, &commit_id)?;
    log::debug!("latest_synced has commits {}", commits.len());
    // for commit in commits.iter() {
    //     log::debug!("latest_synced has commit.... {}", commit);
    // }

    // If the repo is v0.19.0 we don't use this API anymore outside of tests,
    // so we can just assume everything is synced
    if repository.min_version() == MinOxenVersion::V0_19_0 {
        return Ok(HttpResponse::Ok().json(CommitSyncStatusResponse {
            status: StatusMessage::resource_found(),
            latest_synced: commits.last().cloned(),
            num_unsynced: 0,
        }));
    }

    let mut latest_synced: Option<Commit> = None;
    let mut commits_to_sync: Vec<Commit> = Vec::new();

    // Iterate old to new over commits
    for commit in commits {
        // log::debug!("latest_synced checking commit {}", commit);
        // Include "None" and "Pending" in n_unsynced. Success, Failure, and Errors are "finished" processing
        match commit_cacher::get_status(&repository, &commit) {
            Ok(Some(CacherStatusType::Success)) => {
                match content_validator::is_valid(&repository, &commit) {
                    Ok(true) => {
                        // Iterating backwards, so this is the latest synced commit
                        // For this to work, we need to maintain relative order of commits in redis queue push // one worker, for now.
                        // TODO: If we want to move to multiple workers or break this order,
                        // we can make this more robust (but slower) by checking the full commit history
                        log::debug!("latest_synced commit is valid: {}", commit);
                        latest_synced = Some(commit);
                        // break;
                    }
                    Ok(false) => {
                        // Invalid, but processed - don't include in n_unsynced
                        log::debug!("latest_synced commit is invalid: {:?}", commit.id);
                    }
                    err => {
                        log::error!("latest_synced content_validator::is_valid error {err:?}");
                        return Ok(HttpResponse::InternalServerError().json(
                            IsValidStatusMessage {
                                status: String::from(STATUS_ERROR),
                                status_message: String::from(MSG_INTERNAL_SERVER_ERROR),
                                status_description: format!("Err: {err:?}"),
                                is_processing: false,
                                is_valid: false,
                            },
                        ));
                    }
                }
            }
            Ok(Some(CacherStatusType::Pending)) => {
                log::debug!("latest_synced commit is pending {}", commit);
                commits_to_sync.push(commit);
            }
            Ok(Some(CacherStatusType::Failed)) => {
                let errors = commit_cacher::get_failures(&repository, &commit).unwrap();
                let error_str = errors
                    .into_iter()
                    .map(|e| e.status_message)
                    .collect::<Vec<String>>()
                    .join(", ");
                log::error!("latest_synced CacherStatusType::Failed for commit {error_str}");
            }
            Ok(None) => {
                // log::debug!("latest_synced commit not yet processing: {}", commit.id);
                commits_to_sync.push(commit);
            }

            err => {
                log::error!("latest_synced {:?}", err);
                return Ok(
                    HttpResponse::InternalServerError().json(IsValidStatusMessage {
                        status: String::from(STATUS_ERROR),
                        status_message: String::from(MSG_INTERNAL_SERVER_ERROR),
                        status_description: format!("Err: {err:?}"),
                        is_processing: false,
                        is_valid: false,
                    }),
                );
            }
        };
    }

    Ok(HttpResponse::Ok().json(CommitSyncStatusResponse {
        status: StatusMessage::resource_found(),
        latest_synced,
        num_unsynced: commits_to_sync.len(),
    }))
}

// TODO: Deprecate this after v0.19.0
pub async fn is_synced(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let commit_or_branch = path_param(&req, "commit_or_branch")?;
    let repository = get_repo(&app_data.path, namespace, &repo_name)?;

    let commit = repositories::revisions::get(&repository, &commit_or_branch)?.ok_or(
        OxenError::revision_not_found(commit_or_branch.clone().into()),
    )?;

    let response = match repositories::commits::get_commit_status_tmp(&repository, &commit) {
        Ok(Some(CacherStatusType::Success)) => {
            match repositories::commits::is_commit_valid_tmp(&repository, &commit) {
                Ok(true) => HttpResponse::Ok().json(IsValidStatusMessage {
                    status: String::from(STATUS_SUCCESS),
                    status_message: String::from(MSG_RESOURCE_FOUND),
                    status_description: String::from(""),
                    is_processing: false,
                    is_valid: true,
                }),
                Ok(false) => {
                    log::error!("content_validator::is_valid false");

                    HttpResponse::Ok().json(IsValidStatusMessage {
                        status: String::from(STATUS_ERROR),
                        status_message: String::from(MSG_CONTENT_IS_INVALID),
                        status_description: "Content is not valid".to_string(),
                        is_processing: false,
                        is_valid: false,
                    })
                }
                err => {
                    log::error!("content_validator::is_valid error {err:?}");

                    HttpResponse::InternalServerError().json(IsValidStatusMessage {
                        status: String::from(STATUS_ERROR),
                        status_message: String::from(MSG_INTERNAL_SERVER_ERROR),
                        status_description: format!("Err: {err:?}"),
                        is_processing: false,
                        is_valid: false,
                    })
                }
            }
        }
        Ok(Some(CacherStatusType::Pending)) => HttpResponse::Ok().json(IsValidStatusMessage {
            status: String::from(STATUS_SUCCESS),
            status_message: String::from(MSG_RESOURCE_IS_PROCESSING),
            status_description: String::from("Commit is still processing"),
            is_processing: true,
            is_valid: false,
        }),
        Ok(Some(CacherStatusType::Failed)) => {
            let errors = commit_cacher::get_failures(&repository, &commit).unwrap();
            let error_str = errors
                .into_iter()
                .map(|e| e.status_message)
                .collect::<Vec<String>>()
                .join(", ");
            log::error!("CacherStatusType::Failed for commit {error_str}");
            HttpResponse::InternalServerError().json(IsValidStatusMessage {
                status: String::from(STATUS_ERROR),
                status_message: String::from(MSG_FAILED_PROCESS),
                status_description: format!("Err: {error_str}"),
                is_processing: false,
                is_valid: false,
            })
        }
        Ok(None) => {
            // This means background status was never kicked off...
            log::debug!(
                "get_status commit {} no status kicked off for repo: {}",
                commit_or_branch,
                repo_name
            );
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        err => {
            log::error!("Error getting status... {:?}", err);
            HttpResponse::InternalServerError().json(IsValidStatusMessage {
                status: String::from(STATUS_ERROR),
                status_message: String::from(MSG_INTERNAL_SERVER_ERROR),
                status_description: format!("Err: {err:?}"),
                is_processing: false,
                is_valid: false,
            })
        }
    };

    Ok(response)
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

fn compress_objects_db(repository: &LocalRepository) -> Result<Vec<u8>, OxenError> {
    let object_dir = util::fs::oxen_hidden_dir(&repository.path).join(OBJECTS_DIR);

    let tar_subdir = Path::new(OBJECTS_DIR);

    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    tar.append_dir_all(tar_subdir, object_dir)?;

    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;

    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!("Compressed objects dir size is {}", ByteSize::b(total_size));
    Ok(buffer)
}

pub async fn download_objects_db(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;
    let buffer = compress_objects_db(&repository)?;
    Ok(HttpResponse::Ok().body(buffer))
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

        repositories::commits::list_between(&repository, &base_commit, &head_commit)?
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

/// TODO: Depreciate this (should send the commit as part of the tree)
pub async fn create(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("Got commit data: {}", body);

    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

    let commit: Commit = match serde_json::from_str(&body) {
        Ok(commit) => commit,
        Err(_) => return Err(OxenHttpError::BadRequest("Invalid commit data".into())),
    };

    let bn: BranchName =
        match serde_json::from_str(&body) {
            Ok(name) => name,
            Err(_) => return Err(OxenHttpError::BadRequest(
                "Must supply `branch_name` in body. Upgrade CLI to greater than v0.6.1 if failing."
                    .into(),
            )),
        };

    // Create Commit from uri params
    match create_commit_object(&repository.path, bn.branch_name, &commit) {
        Ok(_) => Ok(HttpResponse::Ok().json(CommitResponse {
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

/// TODO: Depreciate this (should send the commits as part of the tree)
pub async fn create_bulk(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

    let commits: Vec<CommitWithBranchName> = match serde_json::from_str(&body) {
        Ok(commits) => commits,
        Err(_) => return Err(OxenHttpError::BadRequest("Invalid commit data".into())),
    };

    let mut result_commits: Vec<Commit> = Vec::new();

    let commit_reader = CommitReader::new(&repository)?;
    let commit_writer = CommitWriter::new(&repository)?;

    let ref_writer = RefWriter::new(&repository)?;

    for commit_with_branch in &commits {
        // get branch name from this commit and raise error if it's not there
        let bn = &commit_with_branch.branch_name;

        // Get commit from commit_with_branch
        let commit = Commit::from_with_branch_name(commit_with_branch);

        if let Err(err) = create_commit_object_with_committers(
            &repository.path,
            bn,
            &commit,
            &commit_reader,
            &commit_writer,
            &ref_writer,
        ) {
            log::error!("Err create_commit: {}", err);
            match err {
                OxenError::RootCommitDoesNotMatch(commit_id) => {
                    log::error!("Err create_commit: RootCommitDoesNotMatch {}", commit_id);
                    return Err(OxenHttpError::BadRequest("Remote commit history does not match local commit history. Make sure you are pushing to the correct remote.".into()));
                }
                _ => {
                    return Err(OxenHttpError::InternalServerError);
                }
            }
        }

        result_commits.push(commit);
    }
    Ok(HttpResponse::Ok().json(ListCommitResponse {
        status: StatusMessage::resource_created(),
        commits: result_commits.to_owned(),
    }))
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
    let commit_id = path_param(&req, "commit_id")?;
    let repo = get_repo(&app_data.path, namespace, name)?;

    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
    let id = query.hash.clone();
    let size = query.total_size;
    let chunk_num = query.chunk_num;
    let total_chunks = query.total_chunks;

    log::debug!(
        "upload_chunk {commit_id} got chunk {chunk_num}/{total_chunks} of upload {id} of total size {size}"
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

pub async fn can_push(
    req: HttpRequest,
    query: web::Query<HashMap<String, String>>,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let client_head_id = path_param(&req, "commit_id")?;
    let repo = get_repo(&app_data.path, namespace, name)?;
    let server_head_id = query.get("remote_head").unwrap();
    let lca_id = query.get("lca").unwrap();

    log::debug!("in the new_can_push endpoint");

    // Ensuring these commits exist on server
    let _server_head_commit = repositories::commits::get_by_id(&repo, server_head_id)?.ok_or(
        OxenError::revision_not_found(server_head_id.to_owned().into()),
    )?;
    let _lca_commit = repositories::commits::get_by_id(&repo, lca_id)?
        .ok_or(OxenError::revision_not_found(lca_id.to_owned().into()))?;

    let can_merge = !head_commits_have_conflicts(&repo, &client_head_id, server_head_id, lca_id)?;

    // Clean up tmp tree files from client head commit
    let tmp_tree_dir = util::fs::oxen_hidden_dir(&repo.path)
        .join("tmp")
        .join(client_head_id)
        .join(TREE_DIR);

    if tmp_tree_dir.exists() {
        std::fs::remove_dir_all(tmp_tree_dir).unwrap();
    }

    if can_merge {
        Ok(HttpResponse::Ok().json(CommitTreeValidationResponse {
            status: StatusMessage::resource_found(),
            can_merge: true,
        }))
    } else {
        Ok(HttpResponse::Ok().json(CommitTreeValidationResponse {
            status: StatusMessage::resource_found(),
            can_merge: false,
        }))
    }
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
    let commit_id = path_param(&req, "commit_id")?;
    let repo = get_repo(&app_data.path, namespace, name)?;

    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);

    // Read bytes from body
    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.unwrap());
    }

    // Compute total size as u64
    let total_size: u64 = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    log::debug!(
        "Got compressed data for commit {} -> {}",
        commit_id,
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

    let commit = repositories::commits::get_by_id(&repo, &commit_id)?;

    Ok(HttpResponse::Ok().json(UploadCommitResponse {
        status: StatusMessage::resource_created(),
        commit,
    }))
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
                    // Kick off processing in background thread because could take awhile
                    std::thread::spawn(move || {
                        log::debug!("Processing commit {:?} on repo {:?}", commit, repo.path);
                        let force = false;
                        match commit_cacher::run_all(&repo, &commit, force) {
                            Ok(_) => {
                                log::debug!(
                                    "Success processing commit {:?} on repo {:?}",
                                    commit,
                                    repo.path
                                );
                            }
                            Err(err) => {
                                log::error!(
                                    "Could not process commit {:?} on repo {:?}: {}",
                                    commit,
                                    repo.path,
                                    err
                                );
                            }
                        }
                    });

                    Ok(HttpResponse::Ok().json(StatusMessage::resource_created()))
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

// Bulk complete
pub async fn complete_bulk(req: HttpRequest, body: String) -> Result<HttpResponse, OxenHttpError> {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let mut queue = app_data.queue.clone();

    // name to the repo, should be in url path so okay to unwrap
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let _repo = get_repo(&app_data.path, namespace, repo_name)?;
    // Deserialize the "commits" param into Vec<Commit> with serde
    let commits: Vec<Commit> = match serde_json::from_str(&body) {
        Ok(commits) => commits,
        Err(_) => return Err(OxenHttpError::BadRequest("Invalid commit data".into())),
    };

    // Get repo by name
    let repo = repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)?
        .ok_or(OxenError::repo_not_found(RepoNew::from_namespace_name(
            namespace, repo_name,
        )))?;

    // List commits for this repo
    let all_commits = repositories::commits::list(&repo)?;

    // Read through existing commits and find any with pending status stuck from previous pushes.
    // This shouldn't be a super common case, but can freeze the repo on commits from old versions

    for commit in all_commits {
        log::debug!("Checking commit {:?}", commit.id);
        if commit_cacher::get_status(&repo, &commit)? == Some(CacherStatusType::Pending) {
            // Need to force remove errantly left locks
            commit_cacher::force_remove_lock(&repo, &commit)?;
            let task = PostPushComplete {
                commit: commit.clone(),
                repo: repo.clone(),
            };
            // Append a task to the queue
            log::debug!(
                "complete_bulk found stuck pending commit {:?}, adding to queue",
                commit.clone()
            );

            queue.push(tasks::Task::PostPushComplete(task))
        }
    }

    let commit_reader = CommitReader::new(&repo)?;

    for req_commit in commits {
        let commit_id = req_commit.id;
        let commit = commit_reader
            .get_commit_by_id(&commit_id)?
            .ok_or(OxenError::revision_not_found(commit_id.clone().into()))?;

        // Append a task to the queue
        let task = PostPushComplete {
            commit: commit.clone(),
            repo: repo.clone(),
        };

        queue.push(tasks::Task::PostPushComplete(task))
    }
    Ok(HttpResponse::Ok().json(StatusMessage::resource_created()))
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
    log::debug!("unpack_entry_tarball hidden_dir {:?}", hidden_dir);

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
                        log::debug!("unpack_entry_tarball path {:?}", path);

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
    let tmp_objects_dir = hidden_dir.join("tmp").join(OBJECTS_DIR);

    // If this dir exists:
    if tmp_objects_dir.exists() {
        log::debug!("tmp objects dir exists, let's do some stuff");

        // merge_objects_dbs(hidden_dir.to_path_buf()).unwrap();
        merge_objects_dbs(&hidden_dir.join(OBJECTS_DIR), &tmp_objects_dir).unwrap();

        std::fs::remove_dir_all(tmp_objects_dir.clone()).unwrap();
    }

    log::debug!("Done decompressing.");
}

#[cfg(test)]
mod tests {

    use actix_web::body::to_bytes;
    use actix_web::{web, App};
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use liboxen::view::commit::UploadCommitResponse;
    use std::path::Path;
    use std::thread;

    use liboxen::constants::OXEN_HIDDEN_DIR;
    use liboxen::error::OxenError;
    use liboxen::repositories;
    use liboxen::util;
    use liboxen::view::ListCommitResponse;

    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::params::PageNumQuery;
    use crate::test::{self, init_test_env};

    #[actix_web::test]
    async fn test_controllers_commits_index_empty() -> Result<(), OxenError> {
        init_test_env();
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();
        let namespace = "Testing-Namespace";
        let name = "Testing-Name";
        test::create_local_repo(&sync_dir, namespace, name)?;

        let uri = format!("/oxen/{namespace}/{name}/commits");
        let req = test::repo_request(&sync_dir, queue, &uri, namespace, name);

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
        let queue = test::init_queue();
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
        let req = test::repo_request(&sync_dir, queue, &uri, namespace, name);

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
        let queue = test::init_queue();
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
            queue,
            &uri,
            namespace,
            repo_name,
            "resource",
            branch_name,
        );

        let query: web::Query<PageNumQuery> =
            web::Query::from_query("page=1&page_size=10").unwrap();
        let resp = controllers::commits::commit_history(req, query)
            .await
            .unwrap();
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
        let queue = test::init_queue();
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
            queue,
            &uri,
            namespace,
            repo_name,
            "resource",
            og_branch.name,
        );

        let query: web::Query<PageNumQuery> =
            web::Query::from_query("page=1&page_size=10").unwrap();
        let resp = controllers::commits::commit_history(req, query)
            .await
            .unwrap();
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
        let queue = test::init_queue();
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

        let uri = format!("/oxen/{}/{}/commits/{}", namespace, repo_name, commit.id);
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData::new(sync_dir.clone(), queue))
                .route(
                    "/oxen/{namespace}/{repo_name}/commits/{commit_id}",
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
        let resp: UploadCommitResponse = serde_json::from_str(body)?;

        let Some(commit) = resp.commit else {
            return Err(OxenError::basic_str("Commit not found"));
        };

        // Make sure commit gets populated
        assert_eq!(commit.id, commit.id);
        assert_eq!(commit.message, commit.message);
        assert_eq!(commit.author, commit.author);
        assert_eq!(commit.parent_ids.len(), commit.parent_ids.len());

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
