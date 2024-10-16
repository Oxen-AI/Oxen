use crate::api::client;
use crate::config::UserConfig;
use crate::constants::{AVG_CHUNK_SIZE, DEFAULT_BRANCH_NAME, OBJECTS_DIR, OXEN_HIDDEN_DIR};
use crate::core::v0_10_0::commits::merge_objects_dbs;
use crate::core::v0_10_0::index::{puller, CommitEntryReader, ObjectDBReader};
use crate::core::v0_19_0::structs::PullProgress;
use crate::error::OxenError;
use crate::model::entry::commit_entry::Entry;
use crate::model::{MetadataEntry, NewCommitBody, RemoteRepository};
use crate::opts::UploadOpts;
use crate::repositories;
use crate::{api, constants};
use crate::{current_function, util};

use async_compression::futures::bufread::GzipDecoder;
use async_tar::Archive;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures_util::TryStreamExt;
use std::fs::{self};
use std::io::prelude::*;
use std::io::Cursor;
use std::path::{Path, PathBuf};

/// Returns the metadata given a file path
pub async fn get_entry(
    remote_repo: &RemoteRepository,
    remote_path: impl AsRef<Path>,
    revision: impl AsRef<str>,
) -> Result<MetadataEntry, OxenError> {
    let remote_path = remote_path.as_ref();

    let response = api::client::metadata::get_file(remote_repo, &revision, &remote_path).await?;
    let entry = response.entry;

    Ok(entry)
}

pub async fn upload_entries(
    remote_repo: &RemoteRepository,
    opts: &UploadOpts,
) -> Result<(), OxenError> {
    if opts.paths.is_empty() {
        return Err(OxenError::basic_str("No files to upload"));
    }

    // Filter down to only files
    let mut file_paths: Vec<PathBuf> = Vec::new();
    for path in &opts.paths {
        if path.is_dir() {
            eprintln!("Directory upload not yet supported: {:?}", path);
            continue;
        }

        file_paths.push(path.to_owned());
    }

    let branch_name = if let Some(branch) = &opts.branch {
        api::client::branches::create_from_branch(remote_repo, branch, DEFAULT_BRANCH_NAME).await?;
        branch.to_owned()
    } else {
        DEFAULT_BRANCH_NAME.to_string()
    };

    log::debug!("Uploading to {}", branch_name);

    // Create uniq workspace id
    let workspace_id = uuid::Uuid::new_v4().to_string();
    let workspace =
        api::client::workspaces::create(remote_repo, &branch_name, &workspace_id).await?;
    assert_eq!(workspace.id, workspace_id);

    api::client::workspaces::files::add_many(
        remote_repo,
        &workspace_id,
        &opts.dst.to_string_lossy(),
        file_paths,
    )
    .await?;

    log::debug!("Committing on {}", branch_name);

    // Commit the data
    let user = UserConfig::get()?.to_user();
    let commit = NewCommitBody {
        message: opts.message.clone(),
        author: user.name,
        email: user.email,
    };
    let commit =
        api::client::workspaces::commit(remote_repo, &branch_name, &workspace_id, &commit).await?;

    println!("Commit {} done.", commit.id);

    Ok(())
}

/// Pings the remote server first to see if the entry exists
/// and get the size before downloading
pub async fn download_entry(
    remote_repo: &RemoteRepository,
    remote_path: impl AsRef<Path>,
    local_path: impl AsRef<Path>,
    revision: impl AsRef<str>,
) -> Result<(), OxenError> {
    let remote_path = remote_path.as_ref();
    let entry = get_entry(remote_repo, remote_path, &revision).await?;
    let remote_file_name = remote_path.file_name();
    let mut local_path = local_path.as_ref().to_path_buf();

    // Following the similar logic as cp or scp

    // * if the dst parent is a file, we error because cannot copy to a file subdirectory
    if let Some(parent) = local_path.parent() {
        if parent.is_file() {
            return Err(OxenError::basic_str(format!(
                "{:?} is not a directory",
                parent
            )));
        }

        // * if the dst parent does not exist, we error because cannot copy a directory to a non-existent location
        if !parent.exists() && parent != Path::new("") {
            return Err(OxenError::basic_str(format!("{:?} does not exist", parent)));
        }
    }

    // * if the dst is a directory, and it exists, then we download the file to the dst
    // given by the dst + the file name
    if local_path.is_dir() && local_path.exists() {
        if let Some(file_name) = &remote_file_name {
            // Only append if the remote entry is a file
            if !entry.is_dir {
                local_path = local_path.join(file_name);
            }
        }
    }

    if entry.is_dir {
        repositories::download::download_dir(remote_repo, &entry, &local_path).await
    } else {
        download_file(remote_repo, &entry, remote_path, local_path, revision).await
    }
}

pub async fn download_dir(
    remote_repo: &RemoteRepository,
    entry: &MetadataEntry,
    local_path: &Path,
) -> Result<(), OxenError> {
    // Download the commit db for the given commit id or branch
    let commit_id = &entry.resource.as_ref().unwrap().commit.as_ref().unwrap().id;
    let home_dir = util::fs::oxen_tmp_dir()?;
    let repo_dir = home_dir
        .join(&remote_repo.namespace)
        .join(&remote_repo.name);
    let repo_cache_dir = repo_dir.join(OXEN_HIDDEN_DIR);
    api::client::commits::download_dir_hashes_db_to_path(remote_repo, commit_id, &repo_cache_dir)
        .await?;

    let local_objects_dir = repo_cache_dir.join(OBJECTS_DIR);
    let tmp_objects_dir =
        api::client::commits::download_objects_db_to_path(remote_repo, &repo_dir).await?;
    log::debug!(
        "trying to merge tmp_objects_dir {:?} into local objects dir {:?}",
        tmp_objects_dir,
        local_objects_dir
    );

    merge_objects_dbs(&local_objects_dir, &tmp_objects_dir)?;

    // Merge it in with the (probably not already extant) local objects db

    let object_reader = ObjectDBReader::new_from_path(repo_dir.clone(), commit_id)?;

    let commit_reader = CommitEntryReader::new_from_path(&repo_dir, commit_id, object_reader)?;
    let entries =
        commit_reader.list_directory(Path::new(&entry.resource.as_ref().unwrap().path))?;

    // Convert entries to [Entry]
    let entries: Vec<Entry> = entries.into_iter().map(Entry::from).collect();

    // Pull all the entries
    let pull_progress = PullProgress::new();
    puller::pull_entries_to_working_dir(remote_repo, &entries, local_path, &pull_progress).await?;

    Ok(())
}

pub async fn download_file(
    remote_repo: &RemoteRepository,
    entry: &MetadataEntry,
    remote_path: impl AsRef<Path>,
    local_path: impl AsRef<Path>,
    revision: impl AsRef<str>,
) -> Result<(), OxenError> {
    if entry.size > AVG_CHUNK_SIZE {
        download_large_entry(
            remote_repo,
            &remote_path,
            &local_path,
            &revision,
            entry.size,
        )
        .await
    } else {
        download_small_entry(remote_repo, remote_path, local_path, revision).await
    }
}

pub async fn download_small_entry(
    remote_repo: &RemoteRepository,
    remote_path: impl AsRef<Path>,
    dest: impl AsRef<Path>,
    revision: impl AsRef<str>,
) -> Result<(), OxenError> {
    let path = remote_path.as_ref().to_string_lossy();
    let revision = revision.as_ref();
    let uri = format!("/file/{}/{}", revision, path);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|_| OxenError::resource_not_found(&url))?;

    let status = response.status();
    if reqwest::StatusCode::OK == status {
        // Copy to file
        let dest = dest.as_ref();
        // Create parent directories if they don't exist
        if let Some(parent) = dest.parent() {
            if !parent.exists() {
                util::fs::create_dir_all(parent)?;
            }
        }

        let mut dest_file = { util::fs::file_create(dest)? };
        let mut content = Cursor::new(response.bytes().await?);

        std::io::copy(&mut content, &mut dest_file)?;
        Ok(())
    } else {
        let err = format!("Could not download entry status: {status}");
        Err(OxenError::basic_str(err))
    }
}

/// Download a file from the remote repository in parallel chunks
pub async fn download_large_entry(
    remote_repo: &RemoteRepository,
    remote_path: impl AsRef<Path>,
    local_path: impl AsRef<Path>,
    revision: impl AsRef<str>,
    num_bytes: u64,
) -> Result<(), OxenError> {
    // Read chunks
    let chunk_size = AVG_CHUNK_SIZE;
    let total_size = num_bytes;
    let num_chunks = ((total_size / chunk_size) + 1) as usize;
    let mut chunk_size = chunk_size;

    // Write files to ~/.oxen/tmp/HASH/chunk_0..N
    let remote_path = remote_path.as_ref();
    let local_path = local_path.as_ref();
    let hash = util::hasher::hash_str(format!("{:?}_{:?}", remote_path, local_path));

    let home_dir = util::fs::oxen_tmp_dir()?;

    let tmp_dir = home_dir.join("tmp").join(&hash);
    if !tmp_dir.exists() {
        util::fs::create_dir_all(&tmp_dir)?;
    }

    log::debug!(
        "Trying to download file {:?} to dir {:?}",
        remote_path,
        tmp_dir
    );

    // Download chunks in parallel
    type PieceOfWork = (
        RemoteRepository,
        PathBuf, // remote_path
        PathBuf, // local_path
        String,  // revision
        u64,     // chunk_start
        u64,     // chunk_size
    );
    let mut tasks: Vec<PieceOfWork> = Vec::new();
    for i in 0..num_chunks {
        // Make sure we read the last size correctly
        let chunk_start = (i as u64) * chunk_size;
        if (chunk_start + chunk_size) > total_size {
            chunk_size = total_size % chunk_size;
        }

        let filename = format!("chunk_{i}");
        let tmp_file = tmp_dir.join(filename);

        tasks.push((
            remote_repo.clone(),
            remote_path.to_path_buf(),
            tmp_file,
            revision.as_ref().to_string(),
            chunk_start,
            chunk_size,
        ));
    }

    use futures::prelude::*;
    let num_workers = constants::DEFAULT_NUM_WORKERS;
    let bodies = stream::iter(tasks)
        .map(|item| async move {
            // log::debug!("Downloading chunk {:?} -> {:?}", remote_path, tmp_file);
            let (remote_repo, remote_path, tmp_file, revision, chunk_start, chunk_size) = item;

            match try_download_entry_chunk(
                &remote_repo,
                &remote_path,
                &tmp_file, // local_path
                &revision,
                chunk_start,
                chunk_size,
            )
            .await
            {
                Ok(_) => Ok(chunk_size),
                Err(err) => Err(err),
            }
        })
        .buffer_unordered(num_workers);

    // Wait for all requests to finish
    bodies
        .for_each(|b| async {
            match b {
                Ok(s) => {
                    log::debug!("Downloaded chunk {:?}", s);
                }
                Err(err) => {
                    log::error!("Error downloading chunk: {:?}", err)
                }
            }
        })
        .await;

    // Once all downloaded, recombine file and delete temp dir
    log::debug!("Unpack to {:?}", local_path);

    // Create parent dir if it doesn't exist
    if let Some(parent) = local_path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let mut combined_file = util::fs::file_create(local_path)?;

    let mut should_cleanup = false;
    for i in 0..num_chunks {
        let filename = format!("chunk_{i}");
        let tmp_file = tmp_dir.join(filename);

        log::debug!("Reading file bytes {:?}", tmp_file);
        match std::fs::File::open(&tmp_file) {
            Ok(mut chunk_file) => {
                let mut buffer: Vec<u8> = Vec::new();
                chunk_file
                    .read_to_end(&mut buffer)
                    .expect("Could not read tmp file to end...");

                match combined_file.write_all(&buffer) {
                    Ok(_) => {
                        log::debug!("Unpack successful! {:?}", local_path);
                        util::fs::remove_file(tmp_file)?;
                    }
                    Err(err) => {
                        log::error!("Could not write all data to disk {:?}", err);
                        should_cleanup = true;
                    }
                }
            }
            Err(err) => {
                log::error!("Could not read chunk file {tmp_file:?}: {err}");
                should_cleanup = true;
            }
        }
    }

    if should_cleanup {
        log::error!("Cleaning up tmp dir {:?}", tmp_dir);
        util::fs::remove_dir_all(tmp_dir)?;
        return Err(OxenError::basic_str("Could not write all data to disk"));
    }

    Ok(())
}

async fn try_download_entry_chunk(
    remote_repo: &RemoteRepository,
    remote_path: impl AsRef<Path>,
    local_path: impl AsRef<Path>,
    revision: impl AsRef<str>,
    chunk_start: u64,
    chunk_size: u64,
) -> Result<u64, OxenError> {
    let mut try_num = 0;
    while try_num < constants::NUM_HTTP_RETRIES {
        match download_entry_chunk(
            remote_repo,
            &remote_path,
            &local_path,
            &revision,
            chunk_start,
            chunk_size,
        )
        .await
        {
            Ok(_) => {
                log::debug!("Downloaded chunk {:?}", local_path.as_ref());
                return Ok(chunk_size);
            }
            Err(err) => {
                log::error!("Error trying to download chunk: {}", err);
                try_num += 1;
                let sleep_time = try_num * try_num;
                std::thread::sleep(std::time::Duration::from_secs(sleep_time));
            }
        }
    }
    Err(OxenError::basic_str("Retry download chunk failed"))
}

/// Downloads a chunk of a file
async fn download_entry_chunk(
    remote_repo: &RemoteRepository,
    remote_path: impl AsRef<Path>,
    local_path: impl AsRef<Path>,
    revision: impl AsRef<str>,
    chunk_start: u64,
    chunk_size: u64,
) -> Result<(), OxenError> {
    let remote_path = remote_path.as_ref();
    let local_path = local_path.as_ref();
    log::debug!(
        "{} {:?} -> {:?}",
        current_function!(),
        remote_path,
        local_path
    );

    let uri = format!(
        "/chunk/{}/{}?chunk_start={}&chunk_size={}",
        revision.as_ref(),
        remote_path.to_string_lossy(),
        chunk_start,
        chunk_size
    );

    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    log::debug!("download_entry_chunk {}", url);

    let client = client::new_for_url(&url)?;
    let response = client.get(&url).send().await?;

    if let Some(parent) = local_path.parent() {
        if !parent.exists() {
            log::debug!("Create parent dir {:?}", parent);
            std::fs::create_dir_all(parent)?;
        }
    }

    let status = response.status();
    if reqwest::StatusCode::OK == status {
        // TODO: replace these with util::fs:: file functions for better error messages
        // Copy to file
        let mut dest = { fs::File::create(local_path)? };
        let mut content = Cursor::new(response.bytes().await?);
        std::io::copy(&mut content, &mut dest)?;
        Ok(())
    } else {
        let err = format!("Could not download entry status: {status}");
        Err(OxenError::basic_str(err))
    }
}

pub async fn download_data_from_version_paths(
    remote_repo: &RemoteRepository,
    content_ids: &[(String, PathBuf)], // tuple of content id and entry path
    dst: &Path,
) -> Result<u64, OxenError> {
    let total_retries = constants::NUM_HTTP_RETRIES;
    let mut num_retries = 0;

    while num_retries < total_retries {
        match try_download_data_from_version_paths(remote_repo, content_ids, &dst).await {
            Ok(val) => return Ok(val),
            Err(OxenError::Authentication(val)) => return Err(OxenError::Authentication(val)),
            Err(err) => {
                num_retries += 1;
                // Exponentially back off
                let sleep_time = num_retries * num_retries;
                log::warn!(
                    "Could not download content {:?} sleeping {}",
                    err,
                    sleep_time
                );
                std::thread::sleep(std::time::Duration::from_secs(sleep_time));
            }
        }
    }

    let err = format!(
        "Err: Failed to download {} files after {} retries",
        content_ids.len(),
        total_retries
    );
    Err(OxenError::basic_str(err))
}

pub async fn try_download_data_from_version_paths(
    remote_repo: &RemoteRepository,
    content_ids: &[(String, PathBuf)], // tuple of content id and entry path
    dst: impl AsRef<Path>,
) -> Result<u64, OxenError> {
    use async_std::prelude::*;

    let dst = dst.as_ref();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    for (content_id, _path) in content_ids.iter() {
        let line = format!("{content_id}\n");
        // log::debug!("download_data_from_version_paths encoding line: {} path: {:?}", line, path);
        encoder.write_all(line.as_bytes())?;
    }
    let body = encoder.finish()?;
    log::debug!("download_data_from_version_paths body len: {}", body.len());
    let url = api::endpoint::url_from_repo(remote_repo, "/versions")?;

    let client = client::new_for_url(&url)?;
    if let Ok(res) = client.get(&url).body(body).send().await {
        if reqwest::StatusCode::UNAUTHORIZED == res.status() {
            let err = "Err: unauthorized request to download data".to_string();
            log::error!("{}", err);
            return Err(OxenError::authentication(err));
        }

        let reader = res
            .bytes_stream()
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
            .into_async_read();
        let decoder = GzipDecoder::new(futures::io::BufReader::new(reader));
        let archive = Archive::new(decoder);

        let mut size: u64 = 0;
        let mut idx: usize = 0;
        // Iterate over archive entries and unpack them to their entry paths
        let mut entries = archive.entries()?;
        while let Some(file) = entries.next().await {
            let entry_path = &content_ids[idx].1;
            // let version = &content_ids[idx];
            // log::debug!(
            //     "download_data_from_version_paths Unpacking {:?} -> {:?}",
            //     version,
            //     entry_path
            // );

            let full_path = dst.join(entry_path);

            let mut file = match file {
                Ok(file) => file,
                Err(err) => {
                    let err = format!("Could not unwrap file {:?} -> {:?}", entry_path, err);
                    return Err(OxenError::basic_str(err));
                }
            };

            if let Some(parent) = full_path.parent() {
                if !parent.exists() {
                    util::fs::create_dir_all(parent)?;
                }
            }

            log::debug!("Unpacking {:?} into path {:?}", entry_path, full_path);
            match file.unpack(&full_path).await {
                Ok(_) => {
                    log::debug!("Successfully unpacked {:?} into dst {:?}", entry_path, dst);
                }
                Err(err) => {
                    let err = format!("Could not unpack file {:?} -> {:?}", entry_path, err);
                    return Err(OxenError::basic_str(err));
                }
            }

            let metadata = util::fs::metadata(&full_path)?;
            size += metadata.len();
            idx += 1;
            log::debug!("Unpacked {} bytes {:?}", metadata.len(), entry_path);
        }

        Ok(size)
    } else {
        let err =
            format!("api::entries::download_data_from_version_paths Err request failed: {url}");
        Err(OxenError::basic_str(err))
    }
}

#[cfg(test)]
mod tests {

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::test;
    use crate::{api, util};

    use std::path::Path;

    #[tokio::test]
    async fn test_download_file_large() -> Result<(), OxenError> {
        test::run_select_data_sync_remote("large_files", |local_repo, remote_repo| async move {
            let remote_path = Path::new("large_files").join("test.csv");
            let local_path = local_repo.path.join("data.csv");
            let revision = DEFAULT_BRANCH_NAME;
            api::client::entries::download_entry(&remote_repo, &remote_path, &local_path, revision)
                .await?;

            assert!(local_path.exists());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_download_file_large_to_dir() -> Result<(), OxenError> {
        test::run_select_data_sync_remote("large_files", |local_repo, remote_repo| async move {
            let remote_path = Path::new("large_files").join("test.csv");
            let local_path = local_repo.path.join("train_data");
            let revision = DEFAULT_BRANCH_NAME;
            // mkdir train_data
            util::fs::create_dir_all(&local_path)?;
            api::client::entries::download_entry(&remote_repo, &remote_path, &local_path, revision)
                .await?;

            assert!(local_path.join("test.csv").exists());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_download_file_large_to_dir_does_not_exist() -> Result<(), OxenError> {
        test::run_select_data_sync_remote("large_files", |local_repo, remote_repo| async move {
            let remote_path = Path::new("large_files").join("test.csv");
            let local_path = local_repo.path.join("I_DO_NOT_EXIST").join("put_it_here");
            let revision = DEFAULT_BRANCH_NAME;
            let result = api::client::entries::download_entry(
                &remote_repo,
                &remote_path,
                &local_path,
                revision,
            )
            .await;

            assert!(result.is_err());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_download_file_large_to_dir_does_exist() -> Result<(), OxenError> {
        test::run_select_data_sync_remote("large_files", |local_repo, remote_repo| async move {
            let remote_path = Path::new("large_files").join("test.csv");
            let local_path = local_repo.path.join("I_DO_EXIST");
            util::fs::create_dir_all(&local_path)?;
            let revision = DEFAULT_BRANCH_NAME;
            let result = api::client::entries::download_entry(
                &remote_repo,
                &remote_path,
                &local_path,
                revision,
            )
            .await;

            assert!(result.is_ok());
            assert!(local_path.join("test.csv").exists());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_download_small_file_to_dir_does_exist() -> Result<(), OxenError> {
        test::run_select_data_sync_remote("annotations", |local_repo, remote_repo| async move {
            let remote_path = Path::new("annotations").join("README.md");
            let local_path = local_repo.path.join("I_DO_EXIST");
            util::fs::create_dir_all(&local_path)?;
            let revision = DEFAULT_BRANCH_NAME;
            let result = api::client::entries::download_entry(
                &remote_repo,
                &remote_path,
                &local_path,
                revision,
            )
            .await;

            assert!(result.is_ok());
            assert!(local_path.join("README.md").exists());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_download_different_dir() -> Result<(), OxenError> {
        test::run_select_data_sync_remote("annotations", |local_repo, remote_repo| async move {
            let remote_path = Path::new("annotations");
            let local_path = local_repo.path.join("data");
            let revision = DEFAULT_BRANCH_NAME;
            api::client::entries::download_entry(&remote_repo, &remote_path, &local_path, revision)
                .await?;

            assert!(local_path.exists());
            assert!(local_path.join("annotations").join("README.md").exists());
            assert!(local_path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv")
                .exists());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_root_entry_metadata() -> Result<(), OxenError> {
        test::run_one_commit_sync_repo_test(|_local_repo, remote_repo| async move {
            let entry =
                api::client::entries::get_entry(&remote_repo, Path::new(""), DEFAULT_BRANCH_NAME)
                    .await;
            println!("entry: {:?}", entry);
            assert!(entry.is_ok());

            Ok(remote_repo)
        })
        .await
    }
}
