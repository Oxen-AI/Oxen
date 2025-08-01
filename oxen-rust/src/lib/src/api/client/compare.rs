use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::{Commit, MerkleHash, RemoteRepository};
use crate::view::compare::{CompareCommitsResponse, CompareEntries, CompareTabularResponse};
use crate::view::compare::{
    TabularCompareBody, TabularCompareFieldBody, TabularCompareResourceBody,
    TabularCompareTargetBody,
};
use crate::view::diff::{DirDiffTreeSummary, DirTreeDiffResponse};
use crate::view::{compare::CompareTabular, JsonDataFrameView};
use crate::view::{CompareEntriesResponse, JsonDataFrameViewResponse};

use serde_json::json;

// TODO this should probably be cpath
#[allow(clippy::too_many_arguments)]
pub async fn create_compare(
    remote_repo: &RemoteRepository,
    compare_id: &str,
    left_path: &str,
    left_revision: &str,
    right_path: &str,
    right_revision: &str,
    keys: Vec<TabularCompareFieldBody>,
    compare: Vec<TabularCompareTargetBody>,
    display: Vec<TabularCompareTargetBody>,
) -> Result<CompareTabular, OxenError> {
    log::debug!("🔄 create_compare() Starting with parameters:");
    log::debug!("  - compare_id: {}", compare_id);
    log::debug!("  - left_path: {}", left_path);
    log::debug!("  - left_revision: {}", left_revision);
    log::debug!("  - right_path: {}", right_path);
    log::debug!("  - right_revision: {}", right_revision);
    log::debug!("  - keys.len(): {}", keys.len());
    log::debug!("  - compare.len(): {}", compare.len());
    log::debug!("  - display.len(): {}", display.len());
    log::debug!("  - remote_repo.remote.url: {}", remote_repo.remote.url);

    log::debug!("🔧 Building request body...");
    let req_body = TabularCompareBody {
        compare_id: compare_id.to_string(),
        left: TabularCompareResourceBody {
            path: left_path.to_string(),
            version: left_revision.to_string(),
        },
        right: TabularCompareResourceBody {
            path: right_path.to_string(),
            version: right_revision.to_string(),
        },
        keys,
        compare,
        display,
    };
    log::debug!("✅ Request body built successfully");

    log::debug!("🌐 Constructing URL...");
    let uri = "/compare/data_frames".to_string();
    log::debug!("  - URI: {}", uri);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("  - Full URL: {}", url);

    log::debug!("📡 Creating HTTP client...");
    let client = client::new_for_url(&url)?;
    log::debug!("✅ HTTP client created successfully");

    log::debug!("📤 Sending POST request...");
    log::debug!("  - URL: {}", url);
    log::debug!("  - Method: POST");
    log::debug!("  - Content-Type: application/json");
    
    match client.post(&url).json(&json!(req_body)).send().await {
        Ok(res) => {
            log::debug!("✅ POST request successful");
            log::debug!("  - Response status: {}", res.status());
            log::debug!("  - Response headers: {:?}", res.headers());
            
            log::debug!("📥 Parsing response body...");
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("✅ Response body parsed successfully");
            log::debug!("  - Body length: {} bytes", body.len());
            log::debug!("  - Body preview: {}", 
                if body.len() > 200 { 
                    format!("{}...", &body[..200]) 
                } else { 
                    body.clone() 
                });
            
            log::debug!("🔄 Deserializing response to CompareTabularResponse...");
            let response: Result<CompareTabularResponse, serde_json::Error> = serde_json::from_str(&body);
            
            match response {
                Ok(tabular_compare) => {
                    log::debug!("✅ Successfully deserialized CompareTabularResponse");
                    log::debug!("🎉 create_compare() completed successfully");
                    Ok(tabular_compare.dfs)
                },
                Err(err) => {
                    log::error!("❌ Failed to deserialize CompareTabularResponse");
                    log::error!("  - Error: {}", err);
                    log::error!("  - Response body: {}", body);
                    Err(OxenError::basic_str(format!(
                        "create_compare() Could not deserialize response [{err}]\n{body}"
                    )))
                }
            }
        }
        Err(err) => {
            log::error!("❌ POST request failed");
            log::error!("  - Error: {}", err);
            log::error!("  - URL: {}", url);
            Err(OxenError::basic_str(format!(
                "create_compare() Request failed: {}", err
            )))
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn update_compare(
    remote_repo: &RemoteRepository,
    compare_id: &str,
    left_path: &str,
    left_revision: &str,
    right_path: &str,
    right_revision: &str,
    keys: Vec<TabularCompareFieldBody>,
    compare: Vec<TabularCompareTargetBody>,
    display: Vec<TabularCompareTargetBody>,
) -> Result<CompareTabular, OxenError> {
    log::debug!("🔄 update_compare() Starting with parameters:");
    log::debug!("  - compare_id: {}", compare_id);
    log::debug!("  - left_path: {}", left_path);
    log::debug!("  - left_revision: {}", left_revision);
    log::debug!("  - right_path: {}", right_path);
    log::debug!("  - right_revision: {}", right_revision);
    log::debug!("  - keys.len(): {}", keys.len());
    log::debug!("  - compare.len(): {}", compare.len());
    log::debug!("  - display.len(): {}", display.len());
    log::debug!("  - remote_repo.remote.url: {}", remote_repo.remote.url);

    log::debug!("🔧 Building request body...");
    let req_body = TabularCompareBody {
        compare_id: compare_id.to_string(),
        left: TabularCompareResourceBody {
            path: left_path.to_string(),
            version: left_revision.to_string(),
        },
        right: TabularCompareResourceBody {
            path: right_path.to_string(),
            version: right_revision.to_string(),
        },
        keys,
        compare,
        display,
    };
    log::debug!("✅ Request body built successfully");

    log::debug!("🌐 Constructing URL...");
    let uri = format!("/compare/data_frames/{compare_id}");
    log::debug!("  - URI: {}", uri);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    log::debug!("  - Full URL: {}", url);

    log::debug!("📡 Creating HTTP client...");
    let client = client::new_for_url(&url)?;
    log::debug!("✅ HTTP client created successfully");

    log::debug!("📤 Sending PUT request...");
    log::debug!("  - URL: {}", url);
    log::debug!("  - Method: PUT");
    log::debug!("  - Content-Type: application/json");
    
    match client.put(&url).json(&json!(req_body)).send().await {
        Ok(res) => {
            log::debug!("✅ PUT request successful");
            log::debug!("  - Response status: {}", res.status());
            log::debug!("  - Response headers: {:?}", res.headers());
            
            log::debug!("📥 Parsing response body...");
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("✅ Response body parsed successfully");
            log::debug!("  - Body length: {} bytes", body.len());
            log::debug!("  - Body preview: {}", 
                if body.len() > 200 { 
                    format!("{}...", &body[..200]) 
                } else { 
                    body.clone() 
                });
            
            log::debug!("🔄 Deserializing response to CompareTabularResponse...");
            let response: Result<CompareTabularResponse, serde_json::Error> =
                serde_json::from_str(&body);
            
            match response {
                Ok(tabular_compare) => {
                    log::debug!("✅ Successfully deserialized CompareTabularResponse");
                    // log::debug!("  - Compare data available: {}", tabular_compare.dfs.to_string().len() > 0);
                    log::debug!("🎉 update_compare() completed successfully");
                    Ok(tabular_compare.dfs)
                },
                Err(err) => {
                    log::error!("❌ Failed to deserialize CompareTabularResponse");
                    log::error!("  - Error: {}", err);
                    log::error!("  - Response body: {}", body);
                    Err(OxenError::basic_str(format!(
                        "update_compare() Could not deserialize response [{err}]\n{body}"
                    )))
                }
            }
        }
        Err(err) => {
            log::error!("❌ PUT request failed");
            log::error!("  - Error: {}", err);
            log::error!("  - URL: {}", url);
            Err(OxenError::basic_str(format!(
                "update_compare() Request failed: {}", err
            )))
        }
    }
}

pub async fn get_derived_compare_df(
    remote_repo: &RemoteRepository,
    compare_id: &str,
) -> Result<JsonDataFrameView, OxenError> {
    // TODO: Factor out this basehead - not actually using it but needs to sync w/ routes on server
    let uri = format!("/compare/data_frames/{}/diff/main..main", compare_id);
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;

    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<JsonDataFrameViewResponse, serde_json::Error> =
        serde_json::from_str(&body);
    match response {
        Ok(df) => Ok(df.data_frame.view),
        Err(err) => Err(OxenError::basic_str(format!(
            "get_derived_compare_df() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

pub async fn commits(
    remote_repo: &RemoteRepository,
    base_commit_id: &MerkleHash,
    head_commit_id: &MerkleHash,
) -> Result<Vec<Commit>, OxenError> {
    let base_commit_id = base_commit_id.to_string();
    let head_commit_id = head_commit_id.to_string();
    let uri = format!("/compare/commits/{base_commit_id}..{head_commit_id}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<CompareCommitsResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(commits) => Ok(commits.compare.commits),
        Err(err) => Err(OxenError::basic_str(format!(
            "commits() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

pub async fn dir_tree(
    remote_repo: &RemoteRepository,
    base_commit_id: &MerkleHash,
    head_commit_id: &MerkleHash,
) -> Result<Vec<DirDiffTreeSummary>, OxenError> {
    let base_commit_id = base_commit_id.to_string();
    let head_commit_id = head_commit_id.to_string();
    let uri = format!("/compare/dir_tree/{base_commit_id}..{head_commit_id}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<DirTreeDiffResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(dir_tree) => Ok(dir_tree.dirs),
        Err(err) => Err(OxenError::basic_str(format!(
            "commits() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

pub async fn entries(
    remote_repo: &RemoteRepository,
    base_commit_id: &MerkleHash,
    head_commit_id: &MerkleHash,
) -> Result<CompareEntries, OxenError> {
    let base_commit_id = base_commit_id.to_string();
    let head_commit_id = head_commit_id.to_string();
    let uri = format!("/compare/entries/{base_commit_id}..{head_commit_id}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<CompareEntriesResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(entries) => Ok(entries.compare),
        Err(err) => Err(OxenError::basic_str(format!(
            "commits() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::constants::DIFF_STATUS_COL;
    use crate::error::OxenError;
    use crate::model::diff::diff_entry_status::DiffEntryStatus;
    use crate::model::MerkleHash;
    use crate::repositories;
    use crate::test;
    use crate::util;
    use crate::view::compare::{TabularCompareFieldBody, TabularCompareTargetBody};
    use polars::lazy::dsl::col;
    use polars::lazy::dsl::lit;
    use polars::lazy::frame::IntoLazy;

    use std::path::PathBuf;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_compare_commits() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut local_repo, remote_repo| async move {
            // Keep track of the commit ids
            let mut commit_ids = Vec::new();

            // Create 5 commits
            for i in 0..5 {
                // Write a file
                let file_path = format!("file_{i}.txt");
                test::write_txt_file_to_path(
                    local_repo.path.join(file_path),
                    format!("File content {}", i),
                )?;
                repositories::add(&local_repo, &local_repo.path).await?;

                let commit_message = format!("Commit {}", i);
                let commit = repositories::commit(&local_repo, &commit_message)?;
                commit_ids.push(commit.id);
            }

            // Set remote
            command::config::set_remote(
                &mut local_repo,
                constants::DEFAULT_REMOTE_NAME,
                &remote_repo.remote.url,
            )?;

            // Push the commits to the remote
            repositories::push(&local_repo).await?;

            let base_commit_id = MerkleHash::from_str(&commit_ids[3])?;
            let head_commit_id = MerkleHash::from_str(&commit_ids[1])?;
            let commits =
                api::client::compare::commits(&remote_repo, &base_commit_id, &head_commit_id)
                    .await?;
            assert_eq!(commits.len(), 2);
            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_compare_change_at_root_dir_tree() -> Result<(), OxenError> {
        test::run_one_commit_sync_repo_test(|mut local_repo, remote_repo| async move {
            let og_commit = repositories::commits::head_commit(&local_repo)?;

            // Write a file
            let file_path = PathBuf::from("test_me_out.txt");
            let full_path = &local_repo.path.join(file_path);

            test::write_txt_file_to_path(full_path, "i am the contents of test_me_out.txt")?;
            repositories::add(&local_repo, &local_repo.path).await?;

            let commit_message = "add test_me_out.txt";
            let new_commit = repositories::commit(&local_repo, commit_message)?;

            // Set remote
            command::config::set_remote(
                &mut local_repo,
                constants::DEFAULT_REMOTE_NAME,
                &remote_repo.remote.url,
            )?;

            // Push the commits to the remote
            repositories::push(&local_repo).await?;

            let base_commit_id = MerkleHash::from_str(&og_commit.id)?;
            let head_commit_id = MerkleHash::from_str(&new_commit.id)?;
            let results =
                api::client::compare::dir_tree(&remote_repo, &base_commit_id, &head_commit_id)
                    .await?;
            println!("results: {:?}", results);
            assert_eq!(results.len(), 1);
            let first = results.first().unwrap();
            assert_eq!(first.name, PathBuf::from(""));
            assert_eq!(first.status, DiffEntryStatus::Modified);
            assert_eq!(first.children.len(), 0);

            let results =
                api::client::compare::entries(&remote_repo, &base_commit_id, &head_commit_id)
                    .await?;
            println!("results: {:?}", results);
            assert_eq!(results.entries.len(), 1);
            let first = results.entries.first().unwrap();
            assert_eq!(first.filename, "test_me_out.txt");
            assert_eq!(first.status, "added");
            assert!(first.base_resource.is_none());
            assert!(first.head_resource.is_some());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_compare_change_tabular_at_root_dir_tree() -> Result<(), OxenError> {
        test::run_one_commit_sync_repo_test(|mut local_repo, remote_repo| async move {
            // Write a file
            let file_path = PathBuf::from("test_me_out.csv");
            let full_path = &local_repo.path.join(file_path);

            test::write_txt_file_to_path(full_path, "image,label\n1,2\n3,4\n5,6")?;
            repositories::add(&local_repo, &local_repo.path).await?;

            let commit_message = "add test_me_out.csv";
            let og_commit = repositories::commit(&local_repo, commit_message)?;

            // Add another row
            test::write_txt_file_to_path(full_path, "image,label\n1,2\n3,4\n5,6\n7,8")?;
            repositories::add(&local_repo, &local_repo.path).await?;
            let new_commit = repositories::commit(&local_repo, commit_message)?;

            // Set remote
            command::config::set_remote(
                &mut local_repo,
                constants::DEFAULT_REMOTE_NAME,
                &remote_repo.remote.url,
            )?;

            // Push the commits to the remote
            repositories::push(&local_repo).await?;

            let base_commit_id = MerkleHash::from_str(&og_commit.id)?;
            let head_commit_id = MerkleHash::from_str(&new_commit.id)?;
            let results =
                api::client::compare::dir_tree(&remote_repo, &base_commit_id, &head_commit_id)
                    .await?;
            println!("results: {:?}", results);
            assert_eq!(results.len(), 1);
            let first = results.first().unwrap();
            assert_eq!(first.name, PathBuf::from(""));
            assert_eq!(first.status, DiffEntryStatus::Modified);
            assert_eq!(first.children.len(), 0);

            let results =
                api::client::compare::entries(&remote_repo, &base_commit_id, &head_commit_id)
                    .await?;
            println!("results: {:?}", results);
            assert_eq!(results.entries.len(), 1);
            let first = results.entries.first().unwrap();
            assert_eq!(first.filename, "test_me_out.csv");
            assert_eq!(first.status, "modified");
            assert!(first.base_resource.is_some());
            assert!(first.head_resource.is_some());

            // The resource version should be different
            assert_ne!(
                first.base_resource.as_ref().unwrap().version,
                first.head_resource.as_ref().unwrap().version
            );

            assert!(first.base_entry.is_some());
            assert!(first.head_entry.is_some());
            println!("\n\nbase_entry: {:?}\n\n", first.base_entry);
            println!("\n\nhead_entry: {:?}\n\n", first.head_entry);
            // The entry resource version should be different
            assert_ne!(
                first
                    .base_entry
                    .as_ref()
                    .unwrap()
                    .resource
                    .as_ref()
                    .unwrap()
                    .version,
                first
                    .head_entry
                    .as_ref()
                    .unwrap()
                    .resource
                    .as_ref()
                    .unwrap()
                    .version
            );

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_compare_nested_dir_tree() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut local_repo, remote_repo| async move {
            // Keep track of the commit ids
            let mut commit_ids = Vec::new();

            // Create 5 commits with 5 new directories
            let total_dirs = 5;
            for i in 0..total_dirs {
                // Write a file
                let dir_path = format!("dir_{i}");
                let file_path = PathBuf::from(dir_path).join(format!("file_{i}.txt"));
                let full_path = local_repo.path.join(file_path);

                // Create the directory
                util::fs::create_dir_all(full_path.parent().unwrap())?;

                test::write_txt_file_to_path(full_path, format!("File content {}", i))?;
                repositories::add(&local_repo, &local_repo.path).await?;

                let commit_message = format!("Commit {}", i);
                let commit = repositories::commit(&local_repo, &commit_message)?;
                commit_ids.push(commit.id);
            }

            // Set remote
            command::config::set_remote(
                &mut local_repo,
                constants::DEFAULT_REMOTE_NAME,
                &remote_repo.remote.url,
            )?;

            // Push the commits to the remote
            repositories::push(&local_repo).await?;

            let base_commit_id = MerkleHash::from_str(&commit_ids[1])?;
            let head_commit_id = MerkleHash::from_str(&commit_ids[3])?;
            let results =
                api::client::compare::dir_tree(&remote_repo, &base_commit_id, &head_commit_id)
                    .await?;
            println!("results: {:?}", results);
            assert_eq!(results.len(), 1);
            let first = results.first().unwrap();
            assert_eq!(first.name, PathBuf::from(""));
            assert_eq!(first.status, DiffEntryStatus::Modified);
            assert_eq!(first.children.len(), 2);
            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_create_compare_get_derived() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut local_repo, remote_repo| async move {
            // Keying on first 3, targeting on d - should be:
            // 1 modified, 1 added, 1 removed?
            let csv1 = "a,b,c,d\n1,2,3,4\n4,5,6,7\n9,0,1,2";
            let csv2 = "a,b,c,d\n1,2,3,4\n4,5,6,8\n0,1,9,2";

            let left_path = "left.csv";
            let right_path = "right.csv";

            test::write_txt_file_to_path(local_repo.path.join(left_path), csv1)?;
            test::write_txt_file_to_path(local_repo.path.join(right_path), csv2)?;

            repositories::add(&local_repo, &local_repo.path).await?;

            repositories::commit(&local_repo, "committing files")?;

            // set remote

            command::config::set_remote(
                &mut local_repo,
                constants::DEFAULT_REMOTE_NAME,
                &remote_repo.remote.url,
            )?;
            repositories::push(&local_repo).await?;

            let compare_id = "abcdefgh";

            api::client::compare::create_compare(
                &remote_repo,
                compare_id,
                left_path,
                constants::DEFAULT_BRANCH_NAME,
                right_path,
                constants::DEFAULT_BRANCH_NAME,
                vec![
                    TabularCompareFieldBody {
                        left: "a".to_string(),
                        right: "a".to_string(),
                        alias_as: None,
                        compare_method: None,
                    },
                    TabularCompareFieldBody {
                        left: "b".to_string(),
                        right: "b".to_string(),
                        alias_as: None,
                        compare_method: None,
                    },
                    TabularCompareFieldBody {
                        left: "c".to_string(),
                        right: "c".to_string(),
                        alias_as: None,
                        compare_method: None,
                    },
                ],
                vec![TabularCompareTargetBody {
                    left: Some("d".to_string()),
                    right: Some("d".to_string()),
                    compare_method: None,
                }],
                vec![],
            )
            .await?;

            // Now get the derived df
            let derived_df =
                api::client::compare::get_derived_compare_df(&remote_repo, compare_id).await?;

            let df = derived_df.to_df();
            println!("df: {:?}", df);

            assert_eq!(df.height(), 3);

            let added_df = df
                .clone()
                .lazy()
                .filter(col(DIFF_STATUS_COL).eq(lit("added")))
                .collect()?;
            assert_eq!(added_df.height(), 1);

            let modified_df = df
                .clone()
                .lazy()
                .filter(col(DIFF_STATUS_COL).eq(lit("modified")))
                .collect()?;
            assert_eq!(modified_df.height(), 1);

            let removed_df = df
                .clone()
                .lazy()
                .filter(col(DIFF_STATUS_COL).eq(lit("removed")))
                .collect()?;

            assert_eq!(removed_df.height(), 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_compare_does_not_update_automatically() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut local_repo, remote_repo| async move {
            println!("🚀 Starting test_remote_compare_does_not_update_automatically");
            
            // Keying on first 3, targeting on d - should be:
            // 1 modified, 1 added, 1 removed?
            let csv1 = "a,b,c,d\n1,2,3,4\n4,5,6,7\n9,0,1,2";
            let csv2 = "a,b,c,d\n1,2,3,4\n4,5,6,8\n0,1,9,2";
            println!("!!!!!!!!!! remote url: {}", remote_repo.remote.url);

            let left_path = "left.csv";
            let right_path = "right.csv";

            println!("📝 Writing initial CSV files:");
            println!("  left.csv content: {}", csv1);
            println!("  right.csv content: {}", csv2);

            test::write_txt_file_to_path(local_repo.path.join(left_path), csv1)?;
            test::write_txt_file_to_path(local_repo.path.join(right_path), csv2)?;
            
            println!("✅ CSV files written successfully");

            println!("➕ Adding files to repository...");
            repositories::add(&local_repo, &local_repo.path).await?;
            println!("✅ Files added to repository");
            
            println!("💾 Committing files...");
            repositories::commit(&local_repo, "committing files")?;
            println!("✅ Files committed");

            // set remote

            println!("🌐 Setting remote configuration...");
            command::config::set_remote(
                &mut local_repo,
                constants::DEFAULT_REMOTE_NAME,
                &remote_repo.remote.url,
            )?;
            println!("✅ Remote configuration set");
            
            println!("📤 Pushing to remote...");
            repositories::push(&local_repo).await?;
            println!("✅ Push completed");

            let compare_id = "abcdefgh";
            println!("🔍 Creating compare with ID: {}", compare_id);

            api::client::compare::create_compare(
                &remote_repo,
                compare_id,
                left_path,
                constants::DEFAULT_BRANCH_NAME,
                right_path,
                constants::DEFAULT_BRANCH_NAME,
                vec![
                    TabularCompareFieldBody {
                        left: "a".to_string(),
                        right: "a".to_string(),
                        alias_as: None,
                        compare_method: None,
                    },
                    TabularCompareFieldBody {
                        left: "b".to_string(),
                        right: "b".to_string(),
                        alias_as: None,
                        compare_method: None,
                    },
                    TabularCompareFieldBody {
                        left: "c".to_string(),
                        right: "c".to_string(),
                        alias_as: None,
                        compare_method: None,
                    },
                ],
                vec![TabularCompareTargetBody {
                    left: Some("d".to_string()),
                    right: Some("d".to_string()),
                    compare_method: None,
                }],
                vec![],
            )
            .await?;
            println!("✅ Compare created successfully");

            // Now get the derived df
            println!("📊 Getting initial derived dataframe...");
            let derived_df =
                api::client::compare::get_derived_compare_df(&remote_repo, compare_id).await?;

            let df = derived_df.to_df();
            println!("📊 Initial dataframe height: {}", df.height());
            println!("📊 Initial dataframe schema: {:?}", df.schema());

            assert_eq!(df.height(), 3);

            println!("🔍 Analyzing initial dataframe by status...");
            let added_df = df
                .clone()
                .lazy()
                .filter(col(DIFF_STATUS_COL).eq(lit("added")))
                .collect()?;
            println!("➕ Added rows: {}", added_df.height());
            assert_eq!(added_df.height(), 1);

            let modified_df = df
                .clone()
                .lazy()
                .filter(col(DIFF_STATUS_COL).eq(lit("modified")))
                .collect()?;
            println!("✏️ Modified rows: {}", modified_df.height());
            assert_eq!(modified_df.height(), 1);

            let removed_df = df
                .clone()
                .lazy()
                .filter(col(DIFF_STATUS_COL).eq(lit("removed")))
                .collect()?;
            println!("➖ Removed rows: {}", removed_df.height());

            assert_eq!(removed_df.height(), 1);

            // Advance the data and don't change the compare definition. New will just take away the removed observation
            let csv1 = "a,b,c,d\n1,2,3,4\n4,5,6,7";
            // let csv2 = "a,b,c,d\n1,2,3,4\n4,5,6,8\n0,1,9,2";

            println!("🔄 Updating left.csv with new content (removing last row):");
            println!("  New left.csv content: {}", csv1);
            test::write_txt_file_to_path(local_repo.path.join(left_path), csv1)?;
            
            println!("➕ Adding updated file to repository...");
            repositories::add(&local_repo, &local_repo.path).await?;
            println!("✅ Updated file added to repository");
            
            println!("💾 Committing updated file...");
            repositories::commit(&local_repo, "committing files")?;
            println!("✅ Updated file committed");
            
            println!("📤 Pushing updated file to remote...");
            repositories::push(&local_repo).await?;
            println!("✅ Updated file pushed to remote");

            // Now get the derived df
            println!("📊 Getting derived dataframe after data update (should be unchanged)...");
            let derived_df =
                api::client::compare::get_derived_compare_df(&remote_repo, compare_id).await?;

            let new_df = derived_df.to_df();
            println!("📊 New dataframe height: {}", new_df.height());
            println!("📊 Original dataframe height: {}", df.height());

            // Nothing should've changed! Compare wasn't updated.
            println!("🔍 Checking if dataframes are equal (should be equal)...");
            let are_equal = new_df == df;
            println!("  Are dataframes equal? {}", are_equal);
            assert_eq!(new_df, df);

            // Now, update the compare - using the exact same body as before, only the commits have changed
            // (is now MAIN)
            println!("🔄 Now updating the compare definition to use latest commits...");
            api::client::compare::update_compare(
                &remote_repo,
                compare_id,
                left_path,
                constants::DEFAULT_BRANCH_NAME,
                right_path,
                constants::DEFAULT_BRANCH_NAME,
                vec![
                    TabularCompareFieldBody {
                        left: "a".to_string(),
                        right: "a".to_string(),
                        alias_as: None,
                        compare_method: None,
                    },
                    TabularCompareFieldBody {
                        left: "b".to_string(),
                        right: "b".to_string(),
                        alias_as: None,
                        compare_method: None,
                    },
                    TabularCompareFieldBody {
                        left: "c".to_string(),
                        right: "c".to_string(),
                        alias_as: None,
                        compare_method: None,
                    },
                ],
                vec![TabularCompareTargetBody {
                    left: Some("d".to_string()),
                    right: Some("d".to_string()),
                    compare_method: None,
                }],
                vec![],
            )
            .await?;
            println!("✅ Compare updated successfully");

            // Get derived df again
            println!("📊 Getting derived dataframe after compare update...");
            let derived_df =
                api::client::compare::get_derived_compare_df(&remote_repo, compare_id).await?;

            let new_df = derived_df.to_df();
            println!("📊 Updated dataframe height: {}", new_df.height());
            println!("📊 Expected height: 2");

            println!("🔍 Checking if dataframes are different (should be different)...");
            let are_different = new_df != df;
            println!("  Are dataframes different? {}", are_different);

            assert_ne!(new_df, df);
            assert_eq!(new_df.height(), 2);

            println!("✅ Test completed successfully!");
            println!("🎉 test_remote_compare_does_not_update_automatically passed all assertions");

            Ok(remote_repo)
        })
        .await
    }
}
