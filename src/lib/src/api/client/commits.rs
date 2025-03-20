use crate::api::client;
use crate::constants::{DEFAULT_PAGE_NUM, DIRS_DIR, DIR_HASHES_DIR, HISTORY_DIR};

use crate::error::OxenError;
use crate::model::commit::CommitWithBranchName;
use crate::model::entry::unsynced_commit_entry::UnsyncedCommitEntries;
use crate::model::{Branch, Commit, LocalRepository, MerkleHash, RemoteRepository};
use crate::opts::PaginateOpts;
use crate::util::hasher::hash_buffer;
use crate::util::progress_bar::{oxify_bar, ProgressBarType};
use crate::view::tree::merkle_hashes::MerkleHashes;
use crate::{api, constants, repositories};
use crate::{current_function, util};
// use crate::util::ReadProgress;
use crate::view::{
    CommitResponse, ListCommitResponse, MerkleHashesResponse, PaginatedCommits, RootCommitResponse,
    StatusMessage,
};

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::str;
use std::sync::Arc;

use async_compression::futures::bufread::GzipDecoder;
use async_tar::Archive;
use bytesize::ByteSize;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::TryStreamExt;
use http::header::CONTENT_LENGTH;
use indicatif::{ProgressBar, ProgressStyle};

pub struct ChunkParams {
    pub chunk_num: usize,
    pub total_chunks: usize,
    pub total_size: usize,
}

pub async fn get_by_id(
    repository: &RemoteRepository,
    commit_id: impl AsRef<str>,
) -> Result<Option<Commit>, OxenError> {
    let commit_id = commit_id.as_ref();
    let uri = format!("/commits/{commit_id}");
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("remote::commits::get_by_id {}", url);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        if res.status() == 404 {
            return Ok(None);
        }

        let body = client::parse_json_body(&url, res).await?;
        log::debug!("api::client::commits::get_by_id Got response {}", body);
        let response: Result<CommitResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(Some(j_res.commit)),
            Err(err) => Err(OxenError::basic_str(format!(
                "get_commit_by_id() Could not deserialize response [{err}]\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str("get_commit_by_id() Request failed"))
    }
}

/// List commits for a file
pub async fn list_commits_for_path(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
    page_opts: &PaginateOpts,
) -> Result<PaginatedCommits, OxenError> {
    let revision = revision.as_ref();
    let path = path.as_ref();
    let path_str = path.to_string_lossy();
    let uri = format!(
        "/commits/history/{revision}/{path_str}?page={}&page_size={}",
        page_opts.page_num, page_opts.page_size
    );
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<PaginatedCommits, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(j_res) => Ok(j_res),
        Err(err) => Err(OxenError::basic_str(format!(
            "list_commits_for_file() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

pub async fn list_all(remote_repo: &RemoteRepository) -> Result<Vec<Commit>, OxenError> {
    let mut all_commits: Vec<Commit> = Vec::new();
    let mut page_num = DEFAULT_PAGE_NUM;
    let page_size = 100;

    let bar = Arc::new(ProgressBar::new_spinner());
    bar.set_style(ProgressStyle::default_spinner());

    loop {
        let page_opts = PaginateOpts {
            page_num,
            page_size,
        };
        match list_all_commits_paginated(remote_repo, &page_opts).await {
            Ok(paginated_commits) => {
                if page_num == DEFAULT_PAGE_NUM {
                    let bar = oxify_bar(bar.clone(), ProgressBarType::Counter);
                    bar.set_length(paginated_commits.pagination.total_entries as u64);
                }
                let n_commits = paginated_commits.commits.len();
                all_commits.extend(paginated_commits.commits);
                bar.inc(n_commits as u64);
                if page_num < paginated_commits.pagination.total_pages {
                    page_num += 1;
                } else {
                    break;
                }
            }
            Err(err) => {
                return Err(err);
            }
        }
    }
    bar.finish_and_clear();

    Ok(all_commits)
}

pub async fn list_missing_hashes(
    remote_repo: &RemoteRepository,
    commit_hashes: HashSet<MerkleHash>,
) -> Result<HashSet<MerkleHash>, OxenError> {
    let uri = "/commits/missing".to_string();
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let client = client::new_for_url(&url)?;
    let res = client
        .post(&url)
        .json(&MerkleHashes {
            hashes: commit_hashes,
        })
        .send()
        .await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<MerkleHashesResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(response) => Ok(response.hashes),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::client::tree::list_missing_hashes() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

pub async fn list_commit_history(
    remote_repo: &RemoteRepository,
    revision: &str,
) -> Result<Vec<Commit>, OxenError> {
    let mut all_commits: Vec<Commit> = Vec::new();
    let mut page_num = DEFAULT_PAGE_NUM;
    let page_size = 100;

    let bar = Arc::new(ProgressBar::new_spinner());
    bar.set_style(ProgressStyle::default_spinner());

    loop {
        let page_opts = PaginateOpts {
            page_num,
            page_size,
        };
        match list_commit_history_paginated(remote_repo, revision, &page_opts).await {
            Ok(paginated_commits) => {
                if page_num == DEFAULT_PAGE_NUM {
                    let bar = oxify_bar(bar.clone(), ProgressBarType::Counter);
                    bar.set_length(paginated_commits.pagination.total_entries as u64);
                }
                let n_commits = paginated_commits.commits.len();
                all_commits.extend(paginated_commits.commits);
                bar.inc(n_commits as u64);
                if page_num < paginated_commits.pagination.total_pages {
                    page_num += 1;
                } else {
                    break;
                }
            }
            Err(err) => {
                return Err(err);
            }
        }
    }
    bar.finish_and_clear();

    Ok(all_commits)
}

async fn list_commit_history_paginated(
    remote_repo: &RemoteRepository,
    revision: &str,
    page_opts: &PaginateOpts,
) -> Result<PaginatedCommits, OxenError> {
    let page_num = page_opts.page_num;
    let page_size = page_opts.page_size;
    let uri = format!("/commits/history/{revision}?page={page_num}&page_size={page_size}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<PaginatedCommits, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(j_res) => Ok(j_res),
                Err(err) => Err(OxenError::basic_str(format!(
                    "list_commit_history() Could not deserialize response [{err}]\n{body}"
                ))),
            }
        }
        Err(err) => Err(OxenError::basic_str(format!(
            "list_commit_history() Request failed: {err}"
        ))),
    }
}

async fn list_all_commits_paginated(
    remote_repo: &RemoteRepository,
    page_opts: &PaginateOpts,
) -> Result<PaginatedCommits, OxenError> {
    let page_num = page_opts.page_num;
    let page_size = page_opts.page_size;
    let uri = format!("/commits/all?page={page_num}&page_size={page_size}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            let response: Result<PaginatedCommits, serde_json::Error> = serde_json::from_str(&body);
            match response {
                Ok(j_res) => Ok(j_res),
                Err(err) => Err(OxenError::basic_str(format!(
                    "list_commit_history() Could not deserialize response [{err}]\n{body}"
                ))),
            }
        }
        Err(err) => Err(OxenError::basic_str(format!(
            "list_commit_history() Request failed: {err}"
        ))),
    }
}

pub async fn root_commit_maybe(
    remote_repo: &RemoteRepository,
) -> Result<Option<Commit>, OxenError> {
    let uri = "/commits/root".to_string();
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("remote::commits::root_commit {}", url);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        let body = client::parse_json_body(&url, res).await?;
        log::debug!("api::client::commits::root_commit Got response {}", body);
        let response: Result<RootCommitResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(j_res.commit),
            Err(err) => Err(OxenError::basic_str(format!(
                "root_commit() Could not deserialize response [{err}]\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str("root_commit() Request failed"))
    }
}

pub async fn download_dir_hashes_from_commit(
    remote_repo: &RemoteRepository,
    commit_id: &str,
    path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let uri = format!("/commits/{commit_id}/download_dir_hashes_db");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!(
        "calling download_dir_hashes_from_commit for commit {}",
        commit_id
    );
    download_dir_hashes_from_url(url, path).await
}

pub async fn download_base_head_dir_hashes(
    remote_repo: &RemoteRepository,
    base_commit_id: &str,
    head_commit_id: &str,
    path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let uri = format!("/commits/{base_commit_id}..{head_commit_id}/download_dir_hashes_db");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!(
        "calling download_base_head_dir_hashes for commits {}..{}",
        base_commit_id,
        head_commit_id
    );
    download_dir_hashes_from_url(url, path).await
}

pub async fn download_dir_hashes_from_url(
    url: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let url = url.as_ref();
    log::debug!("{} downloading from {}", current_function!(), url);
    let client = client::new_for_url(url)?;
    match client.get(url).send().await {
        Ok(res) => {
            let path = path.as_ref();
            let reader = res
                .bytes_stream()
                .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                .into_async_read();
            let decoder = GzipDecoder::new(futures::io::BufReader::new(reader));
            let archive = Archive::new(decoder);

            let full_unpacked_path = path;

            // // TODO: This is to avoid a race condition caused by another process initializing the
            // // dirs db while the tarball is being unpacked, leading to an error.

            // // Find out what is causing this, then revert this to unpack directly in the final path
            // let tmp_path = path.join("tmp").join("commits_db");

            // // create the temp path if it doesn't exist
            // if !tmp_path.exists() {
            //     util::fs::create_dir_all(&tmp_path)?;
            // }

            log::debug!("unpacking to {:?}", full_unpacked_path);
            let archive_result = archive.unpack(&full_unpacked_path).await;
            log::debug!("archive_result for url {} is {:?}", url, archive_result);
            archive_result?;

            // if !full_unpacked_path.exists() {
            //     log::debug!("{} creating {:?}", current_function!(), full_unpacked_path);
            //     if let Some(parent) = full_unpacked_path.parent() {
            //         util::fs::create_dir_all(parent)?;
            //     } else {
            //         log::error!(
            //             "{} no parent found for {:?}",
            //             current_function!(),
            //             full_unpacked_path
            //         );
            //     }
            // }

            // // Move the tmp path to the full path
            // let tmp_path = tmp_path.join(HISTORY_DIR);
            // log::debug!("copying all tmp {:?} to {:?}", tmp_path, full_unpacked_path);

            // for entry in std::fs::read_dir(&tmp_path)? {
            //     let entry = entry?;
            //     let target = full_unpacked_path.join(HISTORY_DIR).join(entry.file_name());
            //     if !target.exists() {
            //         log::debug!("copying {:?} to {:?}", entry.path(), target);
            //         if let Some(parent) = target.parent() {
            //             if !parent.exists() {
            //                 util::fs::create_dir_all(parent)?;
            //             }
            //         }
            //         util::fs::rename(entry.path(), &target)?;
            //     } else {
            //         log::debug!("skipping copying {:?} to {:?}", entry.path(), target);
            //     }
            // }

            // log::debug!("{} writing to {:?}", current_function!(), path);

            Ok(path.to_path_buf())
        }
        Err(err) => {
            let error = format!("Error fetching commit db: {}", err);
            Err(OxenError::basic_str(error))
        }
    }
}

pub async fn download_dir_hashes_db_to_path(
    remote_repo: &RemoteRepository,
    commit_id: &str,
    path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let uri = format!("/commits/{commit_id}/commit_db");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!(
        "calling download_dir_hashes_db_to_path for commit {}",
        commit_id
    );
    log::debug!("{} downloading from {}", current_function!(), url);
    let client = client::new_for_url(&url)?;
    match client.get(url).send().await {
        Ok(res) => {
            let path = path.as_ref();
            let reader = res
                .bytes_stream()
                .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
                .into_async_read();
            let decoder = GzipDecoder::new(futures::io::BufReader::new(reader));
            let archive = Archive::new(decoder);

            let full_unpacked_path = path.join(HISTORY_DIR).join(commit_id);

            // TODO: This is to avoid a race condition caused by another process initializing the
            // dirs db while the tarball is being unpacked, leading to an error.

            // Find out what is causing this, then revert this to unpack directly in the final path
            let tmp_path = path.join("tmp").join(commit_id).join("commits_db");

            // create the temp path if it doesn't exist
            if !tmp_path.exists() {
                std::fs::create_dir_all(&tmp_path)?;
            }

            let archive_result = archive.unpack(&tmp_path).await;
            log::debug!(
                "archive_result for commit {:?} is {:?}",
                commit_id,
                archive_result
            );
            archive_result?;

            if full_unpacked_path.exists() {
                log::debug!(
                    "{} removing existing {:?}",
                    current_function!(),
                    full_unpacked_path
                );
                util::fs::remove_dir_all(&full_unpacked_path)?;
            } else {
                log::debug!("{} creating {:?}", current_function!(), full_unpacked_path);
                if let Some(parent) = full_unpacked_path.parent() {
                    std::fs::create_dir_all(parent)?;
                } else {
                    log::error!(
                        "{} no parent found for {:?}",
                        current_function!(),
                        full_unpacked_path
                    );
                }
            }

            // Move the tmp path to the full path
            log::debug!("renaming {:?} to {:?}", tmp_path, full_unpacked_path);

            util::fs::rename(
                tmp_path.join(HISTORY_DIR).join(commit_id),
                &full_unpacked_path,
            )?;

            log::debug!("{} writing to {:?}", current_function!(), path);

            Ok(path.to_path_buf())
        }
        Err(err) => {
            let error = format!("Error fetching commit db: {}", err);
            Err(OxenError::basic_str(error))
        }
    }
}

pub async fn get_remote_parent(
    remote_repo: &RemoteRepository,
    commit_id: &str,
) -> Result<Vec<Commit>, OxenError> {
    let uri = format!("/commits/{commit_id}/parents");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<ListCommitResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(j_res) => Ok(j_res.commits),
            Err(err) => Err(OxenError::basic_str(format!(
                "get_remote_parent() Could not deserialize response [{err}]\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str("get_remote_parent() Request failed"))
    }
}

pub async fn post_push_complete(
    remote_repo: &RemoteRepository,
    branch: &Branch,
    // we need to pass in the commit id because we might be pushing multiple commits from the same branch
    commit_id: impl AsRef<str>,
) -> Result<(), OxenError> {
    use serde_json::json;
    let commit_id = commit_id.as_ref();
    let uri = format!("/commits/{}/complete", commit_id);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("post_push_complete: {}", url);
    let body = serde_json::to_string(&json!({
        "branch": {
            "name": branch.name,
            "commit_id": commit_id,
        }
    }))
    .unwrap();

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.post(&url).body(body).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(_) => Ok(()),
            Err(err) => Err(OxenError::basic_str(format!(
                "post_push_complete() Could not deserialize response [{err}]\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str("post_push_complete() Request failed"))
    }
}

// Commits must be in oldest-to-newest-order
pub async fn bulk_post_push_complete(
    remote_repo: &RemoteRepository,
    commits: &Vec<Commit>,
) -> Result<(), OxenError> {
    use serde_json::json;

    let uri = "/commits/complete".to_string();
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("bulk_post_push_complete: {}", url);
    let body = serde_json::to_string(&json!(commits)).unwrap();

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.post(&url).body(body).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(_) => Ok(()),
            Err(err) => Err(OxenError::basic_str(format!(
                "bulk_post_push_complete() Could not deserialize response [{err}]\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "bulk_post_push_complete() Request failed",
        ))
    }
}

pub async fn get_commits_with_unsynced_dbs(
    remote_repo: &RemoteRepository,
    branch: &Branch,
) -> Result<Vec<Commit>, OxenError> {
    let revision = branch.commit_id.clone();

    let uri = format!("/commits/{revision}/db_status");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<ListCommitResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(commit_response) => Ok(commit_response.commits),
            Err(err) => Err(OxenError::basic_str(format!(
                "get_commits_with_unsynced_dbs() Could not deserialize response [{err}]\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "get_commits_with_unsynced_dbs() Request failed",
        ))
    }
}

pub async fn get_commits_with_unsynced_entries(
    remote_repo: &RemoteRepository,
    branch: &Branch,
) -> Result<Vec<Commit>, OxenError> {
    let commit_id = branch.commit_id.clone();

    let uri = format!("/commits/{commit_id}/entries_status");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<ListCommitResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(commit_response) => Ok(commit_response.commits),
            Err(err) => Err(OxenError::basic_str(format!(
                "get_commits_with_unsynced_entries() Could not deserialize response [{err}]\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "get_commits_with_unsynced_entries() Request failed",
        ))
    }
}

pub async fn post_commits_to_server(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commits: &Vec<UnsyncedCommitEntries>,
    branch_name: String,
) -> Result<(), OxenError> {
    let mut commits_with_size: Vec<CommitWithBranchName> = Vec::new();
    for commit_with_entries in commits {
        let commit_history_dir = util::fs::oxen_hidden_dir(&local_repo.path)
            .join(HISTORY_DIR)
            .join(&commit_with_entries.commit.id);
        let entries_size =
            repositories::entries::compute_generic_entries_size(&commit_with_entries.entries)?;

        let size = match fs_extra::dir::get_size(&commit_history_dir) {
            Ok(size) => size + entries_size,
            Err(err) => {
                log::warn!("Err {}: {:?}", err, commit_history_dir);
                entries_size
            }
        };

        let commit_with_size = CommitWithBranchName::from_commit(
            &commit_with_entries.commit,
            size,
            branch_name.clone(),
        );

        commits_with_size.push(commit_with_size);
    }

    bulk_create_commit_obj_on_server(remote_repo, &commits_with_size).await?;
    Ok(())
}

pub async fn post_commit_dir_hashes_to_server(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commit: &Commit,
) -> Result<(), OxenError> {
    let commit_dir = util::fs::oxen_hidden_dir(&local_repo.path)
        .join(HISTORY_DIR)
        .join(commit.id.clone());

    // This will be the subdir within the tarball
    let tar_subdir = Path::new(HISTORY_DIR).join(commit.id.clone());

    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    // Don't send any errantly downloaded local cache files (from old versions of oxen clone)
    let dirs_to_compress = vec![DIRS_DIR, DIR_HASHES_DIR];

    for dir in &dirs_to_compress {
        let full_path = commit_dir.join(dir);
        let tar_path = tar_subdir.join(dir);
        if full_path.exists() {
            tar.append_dir_all(&tar_path, full_path)?;
        }
    }

    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;

    let is_compressed = true;
    let filename = None;

    let quiet_bar = Arc::new(ProgressBar::hidden());

    let client = client::new_for_remote_repo(remote_repo)?;
    post_data_to_server_with_client(
        &client,
        remote_repo,
        buffer,
        is_compressed,
        &filename,
        quiet_bar,
    )
    .await
}

pub async fn post_commits_dir_hashes_to_server(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    commits: &Vec<Commit>,
) -> Result<(), OxenError> {
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    for commit in commits {
        let commit_dir = util::fs::oxen_hidden_dir(&local_repo.path)
            .join(HISTORY_DIR)
            .join(commit.id.clone());

        // This will be the subdir within the tarball
        let tar_subdir = Path::new(HISTORY_DIR).join(commit.id.clone());

        // Don't send any errantly downloaded local cache files (from old versions of oxen clone)
        let dirs_to_compress = vec![DIRS_DIR, DIR_HASHES_DIR];

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

    let is_compressed = true;
    let filename = None;

    let quiet_bar = Arc::new(ProgressBar::hidden());

    let client = client::new_for_remote_repo(remote_repo)?;
    post_data_to_server_with_client(
        &client,
        remote_repo,
        buffer,
        is_compressed,
        &filename,
        quiet_bar,
    )
    .await
}

pub async fn bulk_create_commit_obj_on_server(
    remote_repo: &RemoteRepository,
    commits: &Vec<CommitWithBranchName>,
) -> Result<ListCommitResponse, OxenError> {
    let url = api::endpoint::url_from_repo(remote_repo, "/commits/bulk")?;
    log::debug!("bulk_create_commit_obj_on_server {}\n{:?}", url, commits);

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.post(&url).json(commits).send().await {
        let body = client::parse_json_body(&url, res).await?;
        log::debug!("bulk_create_commit_obj_on_server got response {}", body);
        let response: Result<ListCommitResponse, serde_json::Error> = serde_json::from_str(&body);
        match response {
            Ok(response) => Ok(response),
            Err(_) => Err(OxenError::basic_str(format!(
                "bulk_create_commit_obj_on_server Err deserializing \n\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str(
            "bulk_create_commit_obj_on_server error sending data from file",
        ))
    }
}

pub async fn post_data_to_server_with_client(
    client: &reqwest::Client,
    remote_repo: &RemoteRepository,
    buffer: Vec<u8>,
    is_compressed: bool,
    filename: &Option<String>,
    bar: Arc<ProgressBar>,
) -> Result<(), OxenError> {
    let chunk_size: usize = constants::AVG_CHUNK_SIZE as usize;

    if buffer.len() > chunk_size {
        upload_data_to_server_in_chunks_with_client(
            client,
            remote_repo,
            &buffer,
            chunk_size,
            is_compressed,
            filename,
        )
        .await?;
    } else {
        upload_single_tarball_to_server_with_client_with_retry(client, remote_repo, &buffer, bar)
            .await?;
    }
    Ok(())
}

pub async fn upload_single_tarball_to_server_with_client_with_retry(
    client: &reqwest::Client,
    remote_repo: &RemoteRepository,
    buffer: &[u8],
    bar: Arc<ProgressBar>,
) -> Result<(), OxenError> {
    let mut total_tries = 0;

    while total_tries < constants::NUM_HTTP_RETRIES {
        match upload_single_tarball_to_server_with_client(
            client,
            remote_repo,
            buffer,
            bar.to_owned(),
        )
        .await
        {
            Ok(_) => {
                return Ok(());
            }
            Err(err) => {
                total_tries += 1;
                // Exponentially back off
                let sleep_time = total_tries * total_tries;
                log::debug!(
                    "upload_single_tarball_to_server_with_retry upload failed sleeping {}: {:?}",
                    sleep_time,
                    err
                );
                std::thread::sleep(std::time::Duration::from_secs(sleep_time));
            }
        }
    }

    Err(OxenError::basic_str("Upload retry failed."))
}

async fn upload_single_tarball_to_server_with_client(
    client: &reqwest::Client,
    remote_repo: &RemoteRepository,
    buffer: &[u8],
    bar: Arc<ProgressBar>,
) -> Result<StatusMessage, OxenError> {
    let uri = "/commits/upload".to_string();
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let size = buffer.len() as u64;
    let res = client.post(&url).body(buffer.to_owned()).send().await?;
    let body = client::parse_json_body(&url, res).await?;

    let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(response) => {
            bar.inc(size);
            Ok(response)
        }
        Err(_) => Err(OxenError::basic_str(format!(
            "upload_single_tarball_to_server Err deserializing \n\n{body}"
        ))),
    }
}

async fn upload_data_to_server_in_chunks_with_client(
    client: &reqwest::Client,
    remote_repo: &RemoteRepository,
    buffer: &[u8],
    chunk_size: usize,
    is_compressed: bool,
    filename: &Option<String>,
) -> Result<(), OxenError> {
    let total_size = buffer.len();
    log::debug!(
        "upload_data_to_server_in_chunks chunking data {} ...",
        total_size
    );
    let chunks: Vec<&[u8]> = buffer.chunks(chunk_size).collect();
    let hash = hash_buffer(buffer);
    log::debug!(
        "upload_data_to_server_in_chunks got {} chunks from {}",
        chunks.len(),
        ByteSize::b(total_size as u64)
    );

    for (i, chunk) in chunks.iter().enumerate() {
        log::debug!(
            "upload_data_to_server_in_chunks uploading chunk {} of size {}",
            i,
            ByteSize::b(chunks.len() as u64)
        );

        let params = ChunkParams {
            chunk_num: i,
            total_chunks: chunks.len(),
            total_size,
        };
        match upload_data_chunk_to_server_with_retry(
            client,
            remote_repo,
            chunk,
            &hash,
            &params,
            is_compressed,
            filename,
        )
        .await
        {
            Ok(_) => {
                log::debug!("Success uploading chunk!")
            }
            Err(err) => {
                log::error!("Err uploading chunk: {}", err)
            }
        }
    }
    Ok(())
}

pub async fn upload_data_chunk_to_server_with_retry(
    client: &reqwest::Client,
    remote_repo: &RemoteRepository,
    chunk: &[u8],
    hash: &str,
    params: &ChunkParams,
    is_compressed: bool,
    filename: &Option<String>,
) -> Result<(), OxenError> {
    let mut total_tries = 0;
    let mut last_error = String::from("");
    while total_tries < constants::NUM_HTTP_RETRIES {
        match upload_data_chunk_to_server(
            client,
            remote_repo,
            chunk,
            hash,
            params,
            is_compressed,
            filename,
        )
        .await
        {
            Ok(_) => {
                return Ok(());
            }
            Err(err) => {
                total_tries += 1;
                // Exponentially back off
                let sleep_time = total_tries * total_tries;
                log::debug!(
                    "upload_data_chunk_to_server_with_retry upload failed sleeping {}: {}",
                    sleep_time,
                    err
                );
                last_error = format!("{}", err);
                std::thread::sleep(std::time::Duration::from_secs(sleep_time));
            }
        }
    }

    Err(OxenError::basic_str(format!(
        "Upload chunk retry failed. {}",
        last_error
    )))
}

async fn upload_data_chunk_to_server(
    client: &reqwest::Client,
    remote_repo: &RemoteRepository,
    chunk: &[u8],
    hash: &str,
    params: &ChunkParams,
    is_compressed: bool,
    filename: &Option<String>,
) -> Result<StatusMessage, OxenError> {
    let maybe_filename = if !is_compressed {
        format!(
            "&filename={}",
            urlencoding::encode(
                filename
                    .as_ref()
                    .expect("Must provide filename if !compressed")
            )
        )
    } else {
        String::from("")
    };

    let uri = format!(
        "/commits/upload_chunk?chunk_num={}&total_size={}&hash={}&total_chunks={}&is_compressed={}{}",
        params.chunk_num, params.total_size, hash, params.total_chunks, is_compressed, maybe_filename);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let total_size = chunk.len() as u64;
    log::debug!(
        "upload_data_chunk_to_server posting {} to url {}",
        ByteSize::b(total_size),
        url
    );

    let res = client
        .post(&url)
        .header(CONTENT_LENGTH, total_size.to_string())
        .body(chunk.to_owned())
        .send()
        .await?;
    let body = client::parse_json_body(&url, res).await?;

    log::debug!("upload_data_chunk_to_server got response {}", body);
    let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(response) => Ok(response),
        Err(err) => Err(OxenError::basic_str(format!(
            "upload_data_chunk_to_server Err deserializing: {err}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;

    use crate::model::MerkleHash;
    use crate::repositories;
    use crate::test;

    use std::str::FromStr;

    #[tokio::test]
    async fn test_list_remote_commits_all() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|local_repo| async move {
            let mut local_repo = local_repo;
            let commit_history = repositories::commits::list(&local_repo)?;
            let num_local_commits = commit_history.len();

            // Set the proper remote
            let name = local_repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Push it
            repositories::push(&local_repo).await?;

            // List the remote commits
            let remote_commits =
                api::client::commits::list_commit_history(&remote_repo, DEFAULT_BRANCH_NAME)
                    .await?;
            assert_eq!(remote_commits.len(), num_local_commits);

            api::client::repositories::delete(&remote_repo).await?;

            Ok(())
        })
        .await
    }

    /* Commented out because it's expensive to find the initial commit id
    #[tokio::test]
    async fn test_list_commit_history_for_path() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|local_repo| async move {
            let mut local_repo = local_repo;
            // Set the proper remote
            let name = local_repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Write, add, and commit file_1
            let file_1 = local_repo.path.join("file_1.txt");
            util::fs::write_to_path(&file_1, "file_1")?;
            repositories::add(&local_repo, &file_1)?;
            let commit_1_file_1 = repositories::commit(&local_repo, "Adding file_1")?;

            // Add a new commit to file_1
            util::fs::write_to_path(&file_1, "file_1_2")?;
            repositories::add(&local_repo, &file_1)?;
            let commit_2_file_1 = repositories::commit(&local_repo, "Adding file_1_2")?;

            // Add a new file_2 and a single commit
            let file_2 = local_repo.path.join("file_2.txt");
            util::fs::write_to_path(&file_2, "file_2")?;
            repositories::add(&local_repo, &file_2)?;
            let _commit_1_file_2 = repositories::commit(&local_repo, "Adding file_2")?;

            // Push it
            repositories::push(&local_repo).await?;

            // List the remote commits
            let remote_commits = api::client::commits::list_commits_for_path(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                "file_1.txt",
                &PaginateOpts::default(),
            )
            .await?;

            // Make sure there are only two for file_1
            assert_eq!(remote_commits.commits.len(), 2);
            assert_eq!(remote_commits.pagination.total_entries, 2);
            assert_eq!(remote_commits.pagination.total_pages, 1);
            assert_eq!(remote_commits.pagination.page_number, 1);
            assert_eq!(remote_commits.pagination.page_size, DEFAULT_PAGE_SIZE);

            // Ensure they come in reverse chronological order
            assert_eq!(remote_commits.commits[0].id, commit_2_file_1.id);
            assert_eq!(remote_commits.commits[1].id, commit_1_file_1.id);

            api::client::repositories::delete(&remote_repo).await?;

            Ok(())
        })
        .await
    }
    */

    /* Commented out because it's expensive to find the initial commit id
    #[tokio::test]
    async fn test_list_commit_history_for_dir() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|local_repo| async move {
            let mut local_repo = local_repo;
            // Set the proper remote
            let name = local_repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Write, add, and commit file_1
            // create the file within a subdirectory
            let sub_dir_name = Path::new("sub_dir");
            let sub_dir = local_repo.path.join(sub_dir_name);

            // create subdir
            util::fs::create_dir_all(&sub_dir)?;

            let file_1 = sub_dir.join("file_1.txt");
            util::fs::write_to_path(&file_1, "file_1")?;
            repositories::add(&local_repo, &file_1)?;
            let commit_1_file_1 = repositories::commit(&local_repo, "Adding file_1")?;

            // Add a new commit to file_1
            util::fs::write_to_path(&file_1, "file_1_2")?;
            repositories::add(&local_repo, &file_1)?;
            let commit_2_file_1 = repositories::commit(&local_repo, "Adding file_1_2")?;

            // Add a new file_2 and a single commit
            let file_2 = sub_dir.join("file_2.txt");
            util::fs::write_to_path(&file_2, "file_2")?;
            repositories::add(&local_repo, &file_2)?;
            let commit_1_file_2 = repositories::commit(&local_repo, "Adding file_2")?;

            // Push it
            repositories::push(&local_repo).await?;

            // List the remote commits
            let remote_commits = api::client::commits::list_commits_for_path(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                &sub_dir_name,
                &PaginateOpts::default(),
            )
            .await?;

            // Make sure there are 3 for the dir
            assert_eq!(remote_commits.commits.len(), 3);
            assert_eq!(remote_commits.pagination.total_entries, 3);
            assert_eq!(remote_commits.pagination.total_pages, 1);
            assert_eq!(remote_commits.pagination.page_number, 1);
            assert_eq!(remote_commits.pagination.page_size, DEFAULT_PAGE_SIZE);

            // Ensure they come in reverse chronological order

            assert_eq!(remote_commits.commits[0].id, commit_1_file_2.id);
            assert_eq!(remote_commits.commits[1].id, commit_2_file_1.id);
            assert_eq!(remote_commits.commits[2].id, commit_1_file_1.id);

            api::client::repositories::delete(&remote_repo).await?;

            Ok(())
        })
        .await
    }
     */

    #[tokio::test]
    async fn test_list_remote_commits_base_head() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|local_repo, remote_repo| async move {
            // There should be >= 6 commits here
            let commit_history = repositories::commits::list(&local_repo)?;
            assert!(commit_history.len() >= 6);

            // Log comes out in reverse order, so we want the 5th commit as the base,
            // and will end up with the 1st,2nd,3rd,4th commits (4 commits total inclusive)
            let base_commit = &commit_history[1];
            let head_commit = &commit_history[4];

            println!("base_commit: {}\nhead_commit: {}", base_commit, head_commit);

            let revision = format!("{}..{}", base_commit.id, head_commit.id);

            // List the remote commits
            let remote_commits =
                api::client::commits::list_commit_history(&remote_repo, &revision).await?;

            for commit in remote_commits.iter() {
                println!("got commit: {} -> {}", commit.id, commit.message);
            }

            assert_eq!(remote_commits.len(), 4);

            api::client::repositories::delete(&remote_repo).await?;

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_list_missing_commit_hashes() -> Result<(), OxenError> {
        test::run_one_commit_sync_repo_test(|local_repo, remote_repo| async move {
            let commit = repositories::commits::head_commit(&local_repo)?;
            let commit_hash = MerkleHash::from_str(&commit.id)?;

            println!("first commit_hash: {}", commit_hash);

            let missing_commit_hashes = api::client::commits::list_missing_hashes(
                &remote_repo,
                HashSet::from([commit_hash]),
            )
            .await?;

            for hash in missing_commit_hashes.iter() {
                println!("missing commit hash: {}", hash);
            }

            assert_eq!(missing_commit_hashes.len(), 0);

            // Add and commit a new file
            let file_path = local_repo.path.join("test.txt");
            let file_path = test::write_txt_file_to_path(file_path, "image,label\n1,2\n3,4\n5,6")?;
            repositories::add(&local_repo, &file_path)?;
            let commit = repositories::commit(&local_repo, "test")?;
            let commit_hash = MerkleHash::from_str(&commit.id)?;

            println!("second commit_hash: {}", commit_hash);

            let missing_node_hashes = api::client::commits::list_missing_hashes(
                &remote_repo,
                HashSet::from([commit_hash]),
            )
            .await?;

            for hash in missing_node_hashes.iter() {
                println!("missing commit hash: {}", hash);
            }

            assert_eq!(missing_node_hashes.len(), 1);
            assert!(missing_node_hashes.contains(&commit_hash));

            Ok(remote_repo)
        })
        .await
    }
}
