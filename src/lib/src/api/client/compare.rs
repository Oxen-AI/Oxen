use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::{Commit, MerkleHash, RemoteRepository};
use crate::view::compare::{CompareCommitsResponse, CompareTabularResponse};
use crate::view::compare::{
    TabularCompareBody, TabularCompareFieldBody, TabularCompareResourceBody,
    TabularCompareTargetBody,
};
use crate::view::JsonDataFrameViewResponse;
use crate::view::{compare::CompareTabular, JsonDataFrameView};

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

    let uri = "/compare/data_frames".to_string();
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;

    let res = client.post(&url).json(&json!(req_body)).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<CompareTabularResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(tabular_compare) => Ok(tabular_compare.dfs),
        Err(err) => Err(OxenError::basic_str(format!(
            "create_compare() Could not deserialize response [{err}]\n{body}"
        ))),
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

    let uri = format!("/compare/data_frames/{compare_id}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;

    // let params =

    if let Ok(res) = client.put(&url).json(&json!(req_body)).send().await {
        let body = client::parse_json_body(&url, res).await?;
        let response: Result<CompareTabularResponse, serde_json::Error> =
            serde_json::from_str(&body);
        match response {
            Ok(tabular_compare) => Ok(tabular_compare.dfs),
            Err(err) => Err(OxenError::basic_str(format!(
                "create_compare() Could not deserialize response [{err}]\n{body}"
            ))),
        }
    } else {
        Err(OxenError::basic_str("create_compare() Request failed"))
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

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::constants::DIFF_STATUS_COL;
    use crate::error::OxenError;
    use crate::model::MerkleHash;
    use crate::repositories;
    use crate::test;
    use crate::view::compare::{TabularCompareFieldBody, TabularCompareTargetBody};
    use polars::lazy::dsl::col;
    use polars::lazy::dsl::lit;
    use polars::lazy::frame::IntoLazy;

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
                repositories::add(&local_repo, &local_repo.path)?;

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
            let commits =
                api::client::compare::commits(&remote_repo, &base_commit_id, &head_commit_id)
                    .await?;
            assert_eq!(commits.len(), 2);
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

            repositories::add(&local_repo, &local_repo.path)?;

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
            // Keying on first 3, targeting on d - should be:
            // 1 modified, 1 added, 1 removed?
            let csv1 = "a,b,c,d\n1,2,3,4\n4,5,6,7\n9,0,1,2";
            let csv2 = "a,b,c,d\n1,2,3,4\n4,5,6,8\n0,1,9,2";

            let left_path = "left.csv";
            let right_path = "right.csv";

            test::write_txt_file_to_path(local_repo.path.join(left_path), csv1)?;
            test::write_txt_file_to_path(local_repo.path.join(right_path), csv2)?;

            repositories::add(&local_repo, &local_repo.path)?;
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

            // Advance the data and don't change the compare definition. New will just take away the removed observation
            let csv1 = "a,b,c,d\n1,2,3,4\n4,5,6,7";
            // let csv2 = "a,b,c,d\n1,2,3,4\n4,5,6,8\n0,1,9,2";

            test::write_txt_file_to_path(local_repo.path.join(left_path), csv1)?;
            repositories::add(&local_repo, &local_repo.path)?;
            repositories::commit(&local_repo, "committing files")?;
            repositories::push(&local_repo).await?;

            // Now get the derived df
            let derived_df =
                api::client::compare::get_derived_compare_df(&remote_repo, compare_id).await?;

            let new_df = derived_df.to_df();

            // Nothing should've changed! Compare wasn't updated.
            assert_eq!(new_df, df);

            // Now, update the compare - using the exact same body as before, only the commits have changed
            // (is now MAIN)
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

            // Get derived df again
            let derived_df =
                api::client::compare::get_derived_compare_df(&remote_repo, compare_id).await?;

            let new_df = derived_df.to_df();

            assert_ne!(new_df, df);
            assert_eq!(new_df.height(), 2);

            Ok(remote_repo)
        })
        .await
    }
}
