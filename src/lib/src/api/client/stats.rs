//! # Remote Stats
//!
//! Get high level stats about a repository.
//!

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::repository::{RepositoryStatsResponse, RepositoryStatsView};

pub async fn get(remote_repo: &RemoteRepository) -> Result<RepositoryStatsView, OxenError> {
    let uri = "/stats".to_string();
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    match client.get(&url).send().await {
        Ok(res) => {
            let body = client::parse_json_body(&url, res).await?;
            log::debug!("got body: {}", body);
            let response: Result<RepositoryStatsResponse, serde_json::Error> =
                serde_json::from_str(&body);
            match response {
                Ok(val) => {
                    log::debug!("got RepositoryStatsResponse: {:?}", val);
                    Ok(val.repository)
                }
                Err(err) => Err(OxenError::basic_str(format!(
                    "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
                ))),
            }
        }
        Err(err) => {
            let err = format!("Request failed: {url}\nErr {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::api;
    use crate::command;
    use crate::constants::DEFAULT_REMOTE_NAME;
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;
    use crate::util;

    // NOTE: Keep this test to ensure we compute sizes on the backend
    #[tokio::test]
    async fn test_remote_get_stats() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|mut local_repo| async move {
            let repo_dir = &local_repo.path;
            let large_dir = repo_dir.join("csvs");
            std::fs::create_dir_all(&large_dir)?;
            let csv_file = large_dir.join("test.csv");
            let from_file = test::test_csv_file_with_name("mixed_data_types.csv");
            util::fs::copy(from_file, &csv_file)?;

            repositories::add(&local_repo, &csv_file)?;
            repositories::commit(&local_repo, "add test.csv")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&local_repo.dirname());
            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, &remote)?;

            // Create the repo
            let remote_repo = test::create_remote_repo(&local_repo).await?;

            // Push the repo
            repositories::push(&local_repo).await?;

            // List the stats
            let stats = api::client::stats::get(&remote_repo).await?;

            // Smol but mighty repo
            assert!(stats.data_size == 183 || stats.data_size == 187); // windows vs unix

            Ok(())
        })
        .await
    }
}
