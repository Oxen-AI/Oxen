use std::path::Path;

use crate::api;
use crate::api::client;
use crate::constants;
use crate::error::OxenError;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::metadata::MetadataDir;
use crate::model::RemoteRepository;
use crate::view::entries::EMetadataEntry;
use crate::view::{PaginatedDirEntries, PaginatedDirEntriesResponse};

pub async fn list_root(remote_repo: &RemoteRepository) -> Result<PaginatedDirEntries, OxenError> {
    list(
        remote_repo,
        constants::DEFAULT_BRANCH_NAME,
        Path::new(""),
        1,
        1,
    )
    .await
}

pub async fn list(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
    page: usize,
    page_size: usize,
) -> Result<PaginatedDirEntries, OxenError> {
    let revision = revision.as_ref();
    let path = path.as_ref().to_string_lossy();
    let uri = format!("/dir/{revision}/{path}?page={page}&page_size={page_size}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<PaginatedDirEntries, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::dir::list_dir error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn file_counts(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<MetadataDir, OxenError> {
    let path_str = path.as_ref().to_string_lossy();
    let response = list(remote_repo, revision, &path, 1, 1).await?;
    match response.dir {
        Some(dir_entry) => match dir_entry {
            EMetadataEntry::MetadataEntry(metadata_entry) => match metadata_entry.metadata {
                Some(GenericMetadata::MetadataDir(metadata)) => Ok(metadata),
                _ => Err(OxenError::basic_str(format!(
                    "No metadata on directory found at {path_str}"
                ))),
            },
            EMetadataEntry::WorkspaceMetadataEntry(_) => Err(OxenError::basic_str(
                "Workspace metadata entry is not implemented",
            )),
        },
        None => Err(OxenError::basic_str(format!(
            "No directory found at {path_str}"
        ))),
    }
}

pub async fn get_dir(
    remote_repo: &RemoteRepository,
    revision: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<PaginatedDirEntriesResponse, OxenError> {
    let path_str = path.as_ref().to_string_lossy();
    let revision = revision.as_ref();
    let uri = format!("/dir/{revision}/{path_str}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<PaginatedDirEntriesResponse, serde_json::Error> =
        serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::dir::get_dir error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;

    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::StagedEntryStatus;
    use crate::repositories;
    use crate::test;
    use crate::util;
    use crate::view::entries::EMetadataEntry;

    use std::path::Path;

    #[tokio::test]
    async fn test_list_dir_has_correct_commits() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|local_repo| async move {
            let mut local_repo = local_repo;

            // Set the proper remote
            let name = local_repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::config::set_remote(&mut local_repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Push it
            repositories::push(&local_repo).await?;

            // Make sure we have one entry
            let root_path = Path::new("");
            let root_entries =
                api::client::dir::list(&remote_repo, DEFAULT_BRANCH_NAME, root_path, 1, 10).await?;
            assert_eq!(root_entries.entries.len(), 1);

            // Add a file
            let file_name = Path::new("file.txt");
            let file_path = local_repo.path.join(file_name);
            let file_content = "Hello, World!";
            util::fs::write_to_path(&file_path, file_content)?;
            repositories::add(&local_repo, file_path).await?;

            // Commit it
            let first_commit = repositories::commit(&local_repo, "Add file.txt")?;

            // Push it
            repositories::push(&local_repo).await?;

            // Make sure we have two entries
            let root_entries =
                api::client::dir::list(&remote_repo, DEFAULT_BRANCH_NAME, root_path, 1, 10).await?;
            assert_eq!(root_entries.entries.len(), 2);

            // Add a dir
            let dir_name = Path::new("data");
            let dir_path = local_repo.path.join(dir_name);
            util::fs::create_dir_all(&dir_path)?;

            // Write some files to the dir
            let file1_path = dir_path.join("file1.txt");
            let file2_path = dir_path.join("file2.txt");
            let file1_content = "Hello, World 1!";
            let file2_content = "Hello, World 2!";
            util::fs::write_to_path(&file1_path, file1_content)?;
            util::fs::write_to_path(&file2_path, file2_content)?;
            repositories::add(&local_repo, &dir_path).await?;

            // Commit it
            let second_commit = repositories::commit(&local_repo, "Add data dir")?;

            // Push it
            repositories::push(&local_repo).await?;

            // Make sure we have three entries
            let root_entries =
                api::client::dir::list(&remote_repo, DEFAULT_BRANCH_NAME, root_path, 1, 10).await?;
            assert_eq!(root_entries.entries.len(), 3);

            for entry in &root_entries.entries {
                println!("entry: {:?}", entry);
            }
            println!("----------------------");

            // Make sure the commit hashes are correct for "data"
            let data_entry = root_entries
                .entries
                .iter()
                .find(|entry| match entry {
                    EMetadataEntry::MetadataEntry(meta) => meta.filename == "data",
                    EMetadataEntry::WorkspaceMetadataEntry(ws) => ws.filename == "data",
                })
                .expect("data entry not found");
            if let EMetadataEntry::MetadataEntry(data) = data_entry {
                assert_eq!(
                    data.latest_commit.as_ref().unwrap().id,
                    second_commit.id,
                    "data commit id mismatch"
                );
            } else {
                panic!("Expected 'data' entry to be a MetadataEntry");
            }

            // Make sure the commit hashes are correct for "file.txt"
            let file_entry = root_entries
                .entries
                .iter()
                .find(|entry| match entry {
                    EMetadataEntry::MetadataEntry(meta) => meta.filename == "file.txt",
                    EMetadataEntry::WorkspaceMetadataEntry(ws) => ws.filename == "file.txt",
                })
                .expect("file.txt entry not found");
            if let EMetadataEntry::MetadataEntry(file) = file_entry {
                assert_eq!(
                    file.latest_commit.as_ref().unwrap().id,
                    first_commit.id,
                    "file.txt commit id mismatch"
                );
            } else {
                panic!("Expected 'file.txt' entry to be a MetadataEntry");
            }

            // Add a second dir
            let dir2_name = Path::new("a_data");
            let dir2_path = local_repo.path.join(dir2_name);
            util::fs::create_dir_all(&dir2_path)?;

            // Write some files to the dir
            let file3_path = dir2_path.join("file3.txt");
            let file4_path = dir2_path.join("file4.txt");
            let file3_content = "Hello, World 3!";
            let file4_content = "Hello, World 4!";
            util::fs::write_to_path(&file3_path, file3_content)?;
            util::fs::write_to_path(&file4_path, file4_content)?;
            repositories::add(&local_repo, &dir2_path).await?;

            // Commit it
            let third_commit = repositories::commit(&local_repo, "Add a_data dir")?;

            // Push it
            repositories::push(&local_repo).await?;

            // Make sure we have four entries
            let root_entries =
                api::client::dir::list(&remote_repo, DEFAULT_BRANCH_NAME, root_path, 1, 10).await?;
            assert_eq!(root_entries.entries.len(), 4);

            for entry in &root_entries.entries {
                println!("entry: {:?}", entry);
            }

            // Make sure the commit hash for "a_data" is correct.
            let a_data_entry = root_entries
                .entries
                .iter()
                .find(|entry| match entry {
                    EMetadataEntry::MetadataEntry(meta) => meta.filename == "a_data",
                    EMetadataEntry::WorkspaceMetadataEntry(ws) => ws.filename == "a_data",
                })
                .expect("a_data entry not found");
            if let EMetadataEntry::MetadataEntry(a_data) = a_data_entry {
                assert_eq!(
                    a_data.latest_commit.as_ref().unwrap().id,
                    third_commit.id,
                    "a_data commit id mismatch"
                );
            } else {
                panic!("Expected 'a_data' entry to be a MetadataEntry");
            }

            // Add a sub directory to the second dir
            let dir3_name = Path::new("sub_data");
            let dir3_path = dir2_path.join(dir3_name);
            util::fs::create_dir_all(&dir3_path)?;

            // Write some files to the dir
            let file5_path = dir3_path.join("file5.txt");
            let file6_path = dir3_path.join("file6.txt");
            let file5_content = "Hello, World 5!";
            let file6_content = "Hello, World 6!";
            util::fs::write_to_path(&file5_path, file5_content)?;
            util::fs::write_to_path(&file6_path, file6_content)?;
            repositories::add(&local_repo, &dir3_path).await?;

            // Commit it
            let fourth_commit = repositories::commit(&local_repo, "Add sub_data dir")?;

            // Push it
            repositories::push(&local_repo).await?;

            // Make sure the sub directory has the correct commit id
            let sub_entries =
                api::client::dir::list(&remote_repo, DEFAULT_BRANCH_NAME, dir2_name, 1, 10).await?;
            println!("sub_entries: {:?}", sub_entries.entries.len());
            for entry in &sub_entries.entries {
                match entry {
                    EMetadataEntry::MetadataEntry(meta) => println!("entry: {:?}", meta.filename),
                    EMetadataEntry::WorkspaceMetadataEntry(ws) => {
                        println!("entry: {:?}", ws.filename)
                    }
                }
            }
            let sub_data_entry = sub_entries
                .entries
                .iter()
                .find(|entry| match entry {
                    EMetadataEntry::MetadataEntry(meta) => meta.filename == "sub_data",
                    EMetadataEntry::WorkspaceMetadataEntry(ws) => ws.filename == "sub_data",
                })
                .expect("sub_data entry not found");
            if let EMetadataEntry::MetadataEntry(sub_data) = sub_data_entry {
                assert_eq!(
                    sub_data.latest_commit.as_ref().unwrap().id,
                    fourth_commit.id,
                    "sub_data commit id mismatch"
                );
            } else {
                panic!("Expected 'sub_data' entry to be a MetadataEntry");
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_dir_has_populates_resource_path() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|local_repo, remote_repo| async move {
            let first_commit = repositories::commits::head_commit(&local_repo)?;

            // Make sure we have one entry
            let root_path = Path::new("");
            let root_entries =
                api::client::dir::list(&remote_repo, DEFAULT_BRANCH_NAME, root_path, 1, 10).await?;
            assert_eq!(root_entries.entries.len(), 1);

            for entry in &root_entries.entries {
                println!("entry: {:?}", entry);
            }

            // Find the README.md entry among the metadata entries.
            let readme_entry = root_entries
                .entries
                .iter()
                .find(|entry| {
                    if let EMetadataEntry::MetadataEntry(meta) = entry {
                        meta.filename == "README.md"
                    } else {
                        false
                    }
                })
                .expect("README.md entry not found");

            if let EMetadataEntry::MetadataEntry(entry) = readme_entry {
                assert_eq!(entry.latest_commit.as_ref().unwrap().id, first_commit.id);
                assert!(entry.resource.as_ref().unwrap().branch.is_some());
                assert_eq!(
                    entry.resource.as_ref().unwrap().path,
                    Path::new("README.md")
                );
            } else {
                panic!("README.md entry is not a MetadataEntry");
            }

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_dir_encoding() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|local_repo, remote_repo| async move {
            let mut local_repo = local_repo;

            command::config::set_remote(
                &mut local_repo,
                constants::DEFAULT_REMOTE_NAME,
                &remote_repo.remote.url,
            )?;
            let repo_path = local_repo.path.join("dir=dir");
            util::fs::create_dir_all(&repo_path)?;
            let file_path = repo_path.join("file example.txt");
            util::fs::write_to_path(&file_path, "Hello World")?;
            repositories::add(&local_repo, &file_path).await?;
            repositories::commit(&local_repo, "Adding README")?;
            repositories::push(&local_repo).await?;

            let dir_response =
                api::client::dir::get_dir(&remote_repo, DEFAULT_BRANCH_NAME, "dir=dir").await?;
            assert_eq!(dir_response.status.status, "success");

            // Assert the directory is present and named "dir=dir"
            if let Some(EMetadataEntry::MetadataEntry(ref dir)) = dir_response.entries.dir {
                assert_eq!(dir.filename, "dir=dir");
                assert!(dir.is_dir);
            } else {
                panic!("Directory 'dir=dir' not found, or is not a MetadataEntry");
            }

            // Assert the file "file example.txt" is present in the entries
            let file_entry = dir_response.entries.entries.iter().find(|entry| {
                if let EMetadataEntry::MetadataEntry(meta) = entry {
                    meta.filename == "file example.txt"
                } else {
                    false
                }
            });
            match file_entry {
                Some(EMetadataEntry::MetadataEntry(file)) => {
                    assert_eq!(file.filename, "file example.txt");
                    assert!(!file.is_dir);
                }
                _ => panic!("File 'file example.txt' not found, or is not a MetadataEntry"),
            }
            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_dir_with_workspace() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|local_repo, remote_repo| async move {
            let file_path = "annotations/train/file.txt";
            let workspace_id = "test_workspace_id";
            let directory_name = "annotations/train";

            let workspace =
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;
            assert_eq!(workspace.id, workspace_id);

            let full_path = local_repo.path.join(file_path);
            util::fs::file_create(&full_path)?;
            util::fs::write(&full_path, b"test content")?;

            let _result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                &full_path,
            )
            .await;

            let file_path = test::test_bounding_box_csv();
            let full_path = local_repo.path.join(file_path);
            util::fs::write(&full_path, "name,age\nAlice,30\nBob,25\n")?;

            let _result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_id,
                directory_name,
                &full_path,
            )
            .await;

            let response =
                api::client::dir::get_dir(&remote_repo, workspace_id, "annotations/train").await?;

            for entry in response.entries.entries.iter() {
                if let EMetadataEntry::WorkspaceMetadataEntry(ws_entry) = entry {
                    match ws_entry.filename.as_str() {
                        "bounding_box.csv" => {
                            assert_eq!(
                                ws_entry.changes.as_ref().unwrap().status,
                                StagedEntryStatus::Modified,
                                "Expected bounding_box.csv to be Modified"
                            );
                        }
                        "file.txt" => {
                            assert_eq!(
                                ws_entry.changes.as_ref().unwrap().status,
                                StagedEntryStatus::Added,
                                "Expected file.txt to be Added"
                            );
                        }
                        _ => {}
                    }
                }
            }
            Ok(remote_repo)
        })
        .await
    }
}
