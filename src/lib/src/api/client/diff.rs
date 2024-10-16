use std::path::Path;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::{DiffEntry, RemoteRepository};
use crate::view::compare::{CompareEntries, CompareEntryResponse};
use crate::view::CompareEntriesResponse;

pub async fn list_diff_entries(
    remote_repo: &RemoteRepository,
    base: impl AsRef<str>,
    head: impl AsRef<str>,
    page: usize,
    page_size: usize,
) -> Result<CompareEntries, OxenError> {
    let base = base.as_ref();
    let head = head.as_ref();
    let uri = format!("/compare/entries/{base}..{head}?page={page}&page_size={page_size}");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    log::debug!("list_page got body: {}", body);
    let response: Result<CompareEntriesResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val.compare),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::client::diff::list_diff_entries error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn diff_entries(
    remote_repo: &RemoteRepository,
    base: impl AsRef<str>,
    head: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<DiffEntry, OxenError> {
    let base = base.as_ref();
    let head = head.as_ref();
    let path = path.as_ref();
    let uri = format!("/compare/file/{base}..{head}/{}", path.to_string_lossy());
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    log::debug!("list_page got body: {}", body);
    let response: Result<CompareEntryResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val.compare),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::client::diff::diff_entries error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

#[cfg(test)]
mod tests {

    use std::path::Path;
    use std::path::PathBuf;

    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::error::OxenError;
    use crate::model::diff::generic_diff::GenericDiff;
    use crate::model::diff::generic_diff_summary::GenericDiffSummary;
    use crate::model::metadata::generic_metadata::GenericMetadata;
    use crate::model::EntryDataType;
    use crate::opts::RmOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    // Test diff add image
    #[tokio::test]
    async fn test_diff_entries_add_image() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images");
            util::fs::create_dir_all(&images_dir)?;

            // Add and commit the first cat
            let test_file = test::test_img_file_with_name("cat_1.jpg");
            let repo_filepath = images_dir.join(test_file.file_name().unwrap());
            util::fs::copy(&test_file, &repo_filepath)?;

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial cat image")?;

            // Get the current branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Create branch
            let branch_name = "feat/collect-another-cat";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Add and commit the second cat
            let test_file = test::test_img_file_with_name("cat_2.jpg");
            let repo_filepath = images_dir.join(test_file.file_name().unwrap());
            util::fs::copy(&test_file, &repo_filepath)?;

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding a second cat")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                branch_name,
            )
            .await?;

            let compare = api::client::diff::diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                &PathBuf::from("images").join("cat_2.jpg"),
            )
            .await?;

            println!("compare: {:#?}", compare);

            // Make sure base entry is empty
            assert!(compare.base_entry.is_none());
            let entry = compare.head_entry.as_ref().unwrap();

            assert_eq!(entry.filename, "cat_2.jpg");
            assert_eq!(
                PathBuf::from(&entry.resource.as_ref().unwrap().path),
                PathBuf::from("images").join("cat_2.jpg")
            );
            assert_eq!(compare.status, "added");
            assert_eq!(entry.data_type, EntryDataType::Image);

            Ok(())
        })
        .await
    }

    // Test diff modify image
    #[tokio::test]
    async fn test_diff_entries_modify_image_resize() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images");
            util::fs::create_dir_all(&images_dir)?;

            // Add and commit the first cat
            let test_file = test::test_img_file_with_name("cat_1.jpg");
            let repo_filepath = images_dir.join(test_file.file_name().unwrap());
            util::fs::copy(&test_file, &repo_filepath)?;

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial cat image")?;

            // Get the current branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Create branch
            let branch_name = "feat/modify-dat-cat";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Modify and commit the first cat
            let repo_filepath = images_dir.join("cat_1.jpg");

            let dims = 96;
            util::image::resize_and_save(&repo_filepath, &repo_filepath, dims)?;

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Modifying the cat")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                branch_name,
            )
            .await?;

            let compare = api::client::diff::diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                &PathBuf::from("images").join("cat_1.jpg"),
            )
            .await?;

            println!("compare: {:#?}", compare);

            // Make sure base entry is empty
            assert!(compare.base_entry.is_some());
            assert!(compare.head_entry.is_some());
            let entry = compare.head_entry.as_ref().unwrap();

            assert_eq!(entry.filename, "cat_1.jpg");
            assert_eq!(
                PathBuf::from(&entry.resource.as_ref().unwrap().path),
                PathBuf::from("images").join("cat_1.jpg")
            );
            assert_eq!(compare.status, "modified");
            assert_eq!(entry.data_type, EntryDataType::Image);

            let metadata = entry.metadata.as_ref().unwrap();
            match metadata {
                GenericMetadata::MetadataImage(metadata) => {
                    assert_eq!(metadata.image.width, dims);
                    assert_eq!(metadata.image.height, dims);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    // Test diff add rows to a csv
    #[tokio::test]
    async fn test_diff_entries_modify_add_rows_csv() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // Add and commit the initial data
            let test_file = test::test_csv_file_with_name("llm_fine_tune.csv");
            let repo_filepath = repo.path.join(test_file.file_name().unwrap());
            util::fs::copy(&test_file, &repo_filepath)?;

            repositories::add(&repo, &repo_filepath)?;
            repositories::commit(&repo, "Adding initial csv")?;

            // Get the current branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Create branch
            let branch_name = "feat/add-some-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Modify and commit the dataframe
            let repo_filepath = test::append_line_txt_file(repo_filepath, "answer the question,what is the color of the sky?,blue,trivia")?;
            let repo_filepath = test::append_line_txt_file(repo_filepath, "answer the question,what is the color of the ocean?,blue-ish green sometimes,trivia")?;

            repositories::add(&repo, &repo_filepath)?;
            repositories::commit(&repo, "Modifying the csv")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            repositories::push::push_remote_branch(&repo, constants::DEFAULT_REMOTE_NAME, branch_name).await?;

            let compare = api::client::diff::diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                &PathBuf::from("llm_fine_tune.csv"),
            )
            .await?;

            println!("compare: {:#?}", compare);

            // Make sure base entry is empty
            assert!(compare.base_entry.is_some());
            assert!(compare.head_entry.is_some());
            assert_eq!(compare.status, "modified");

            let head_entry = compare.head_entry.as_ref().unwrap();
            assert_eq!(head_entry.filename, "llm_fine_tune.csv");
            assert_eq!(head_entry.resource.as_ref().unwrap().path, Path::new("llm_fine_tune.csv"));
            assert_eq!(head_entry.data_type, EntryDataType::Tabular);

            let metadata = head_entry.metadata.as_ref().unwrap();
            match metadata {
                GenericMetadata::MetadataTabular(metadata) => {
                    assert_eq!(metadata.tabular.height, 8);
                    assert_eq!(metadata.tabular.width, 4);
                }
                _ => panic!("Wrong summary type"),
            }

            let base_entry = compare.base_entry.as_ref().unwrap();
            assert_eq!(base_entry.filename, "llm_fine_tune.csv");
            assert_eq!(base_entry.resource.as_ref().unwrap().path, Path::new("llm_fine_tune.csv"));
            assert_eq!(base_entry.data_type, EntryDataType::Tabular);

            let metadata = base_entry.metadata.as_ref().unwrap();
            match metadata {
                GenericMetadata::MetadataTabular(metadata) => {
                    assert_eq!(metadata.tabular.height, 6);
                    assert_eq!(metadata.tabular.width, 4);
                }
                _ => panic!("Wrong summary type"),
            }

            let diff_summary = compare.diff_summary.as_ref().unwrap();
            match diff_summary {
                GenericDiffSummary::TabularDiffWrapper(diff_summary) => {
                    assert_eq!(diff_summary.tabular.num_added_rows, 2);
                    assert_eq!(diff_summary.tabular.num_removed_rows, 0);
                    assert_eq!(diff_summary.tabular.num_added_cols, 0);
                    assert_eq!(diff_summary.tabular.num_removed_cols, 0);
                }
                _ => panic!("Wrong summary type"),
            }

            let diff = compare.diff.as_ref().unwrap();
            match diff {
                GenericDiff::TabularDiff(diff) => {
                    assert_eq!(diff.tabular.added_rows.as_ref().unwrap().view_size.height, 2);
                    assert!(diff.tabular.added_cols.as_ref().is_none());
                    assert!(diff.tabular.removed_cols.as_ref().is_none());
                    assert!(diff.tabular.removed_rows.as_ref().is_none());
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    // Test diff add rows to a csv
    #[tokio::test]
    async fn test_diff_entries_modify_add_and_remove_rows_csv() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // Add and commit the initial data
            let test_file = test::test_csv_file_with_name("llm_fine_tune.csv");
            let repo_filepath = repo.path.join(test_file.file_name().unwrap());
            util::fs::copy(&test_file, &repo_filepath)?;

            repositories::add(&repo, &repo_filepath)?;
            repositories::commit(&repo, "Adding initial csv")?;

            // Get the current branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Create branch
            let branch_name = "feat/add-some-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Modify and commit the dataframe
            let repo_filepath = test::write_txt_file_to_path(
                repo_filepath,
                r"instruction,context,response,category
answer the question,what is the capital of france?,paris,geography
answer the question,who was the 44th president of the united states?,barack obama,politics
who won the game,,I don't know what game you are referring to,sports
who won the game?,The packers beat up on the bears,packers,sports
define the word,what does the word 'the' mean?,it is a stopword.,language
",
            )?;

            repositories::add(&repo, &repo_filepath)?;
            repositories::commit(&repo, "Modifying the csv")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                branch_name,
            )
            .await?;

            let compare = api::client::diff::diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                &PathBuf::from("llm_fine_tune.csv"),
            )
            .await?;

            println!("compare: {:#?}", compare);

            // Make sure base entry is empty
            assert!(compare.base_entry.is_some());
            assert!(compare.head_entry.is_some());
            assert_eq!(compare.status, "modified");

            let head_entry = compare.head_entry.as_ref().unwrap();
            assert_eq!(head_entry.filename, "llm_fine_tune.csv");
            assert_eq!(
                head_entry.resource.as_ref().unwrap().path,
                Path::new("llm_fine_tune.csv")
            );
            assert_eq!(head_entry.data_type, EntryDataType::Tabular);

            let metadata = head_entry.metadata.as_ref().unwrap();
            match metadata {
                GenericMetadata::MetadataTabular(metadata) => {
                    assert_eq!(metadata.tabular.height, 5);
                    assert_eq!(metadata.tabular.width, 4);
                }
                _ => panic!("Wrong summary type"),
            }

            let base_entry = compare.base_entry.as_ref().unwrap();
            assert_eq!(base_entry.filename, "llm_fine_tune.csv");
            assert_eq!(
                base_entry.resource.as_ref().unwrap().path,
                Path::new("llm_fine_tune.csv")
            );
            assert_eq!(base_entry.data_type, EntryDataType::Tabular);

            let metadata = base_entry.metadata.as_ref().unwrap();
            match metadata {
                GenericMetadata::MetadataTabular(metadata) => {
                    assert_eq!(metadata.tabular.height, 6);
                    assert_eq!(metadata.tabular.width, 4);
                }
                _ => panic!("Wrong summary type"),
            }

            let diff_summary = compare.diff_summary.as_ref().unwrap();
            match diff_summary {
                GenericDiffSummary::TabularDiffWrapper(diff_summary) => {
                    assert_eq!(diff_summary.tabular.num_added_rows, 1);
                    assert_eq!(diff_summary.tabular.num_removed_rows, 2);
                    assert_eq!(diff_summary.tabular.num_added_cols, 0);
                    assert_eq!(diff_summary.tabular.num_removed_cols, 0);
                }
                _ => panic!("Wrong summary type"),
            }

            let diff = compare.diff.as_ref().unwrap();
            match diff {
                GenericDiff::TabularDiff(diff) => {
                    assert_eq!(
                        diff.tabular.added_rows.as_ref().unwrap().view_size.height,
                        1
                    );
                    assert_eq!(
                        diff.tabular.removed_rows.as_ref().unwrap().view_size.height,
                        2
                    );
                    assert!(diff.tabular.added_cols.as_ref().is_none());
                    assert!(diff.tabular.removed_cols.as_ref().is_none());
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    // Test diff add cols to a csv
    #[tokio::test]
    async fn test_diff_entries_modify_remove_columns_csv() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // Add and commit the initial data
            let test_file = test::test_csv_file_with_name("llm_fine_tune.csv");
            let repo_filepath = repo.path.join(test_file.file_name().unwrap());
            util::fs::copy(&test_file, &repo_filepath)?;

            repositories::add(&repo, &repo_filepath)?;
            repositories::commit(&repo, "Adding initial csv")?;

            // Get the current branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Create branch
            let branch_name = "feat/add-some-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Modify and commit the dataframe
            let repo_filepath = test::write_txt_file_to_path(
                repo_filepath,
                r#"instruction,context,response
answer the question,what is the capital of france?,paris
answer the question,who was the 44th president of the united states?,barack obama
turn xml to json,<body><name>Bessie</name></body>,{"body": {"name": "bessie"}}
who won the game,,I don't know what game you are referring to
who won the game,broncos 23 v chargers 17,broncos
who won the game?,The packers beat up on the bears,packers
"#,
            )?;

            repositories::add(&repo, &repo_filepath)?;
            repositories::commit(&repo, "Modifying the csv")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                branch_name,
            )
            .await?;

            let compare = api::client::diff::diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                &PathBuf::from("llm_fine_tune.csv"),
            )
            .await?;

            println!("compare: {:#?}", compare);

            // Make sure base entry is empty
            assert!(compare.base_entry.is_some());
            assert!(compare.head_entry.is_some());
            assert_eq!(compare.status, "modified");

            let head_entry = compare.head_entry.as_ref().unwrap();
            assert_eq!(head_entry.filename, "llm_fine_tune.csv");
            assert_eq!(
                head_entry.resource.as_ref().unwrap().path,
                Path::new("llm_fine_tune.csv")
            );
            assert_eq!(head_entry.data_type, EntryDataType::Tabular);

            let metadata = head_entry.metadata.as_ref().unwrap();
            match metadata {
                GenericMetadata::MetadataTabular(metadata) => {
                    assert_eq!(metadata.tabular.height, 6);
                    assert_eq!(metadata.tabular.width, 3);
                }
                _ => panic!("Wrong summary type"),
            }

            let base_entry = compare.base_entry.as_ref().unwrap();
            assert_eq!(base_entry.filename, "llm_fine_tune.csv");
            assert_eq!(
                base_entry.resource.as_ref().unwrap().path,
                Path::new("llm_fine_tune.csv")
            );
            assert_eq!(base_entry.data_type, EntryDataType::Tabular);

            let metadata = base_entry.metadata.as_ref().unwrap();
            match metadata {
                GenericMetadata::MetadataTabular(metadata) => {
                    assert_eq!(metadata.tabular.height, 6);
                    assert_eq!(metadata.tabular.width, 4);
                }
                _ => panic!("Wrong summary type"),
            }

            let diff_summary = compare.diff_summary.as_ref().unwrap();
            match diff_summary {
                GenericDiffSummary::TabularDiffWrapper(diff_summary) => {
                    assert_eq!(diff_summary.tabular.num_added_rows, 0);
                    assert_eq!(diff_summary.tabular.num_removed_rows, 0);
                    assert_eq!(diff_summary.tabular.num_added_cols, 0);
                    assert_eq!(diff_summary.tabular.num_removed_cols, 1);
                }
                _ => panic!("Wrong summary type"),
            }

            let diff = compare.diff.as_ref().unwrap();
            match diff {
                GenericDiff::TabularDiff(diff) => {
                    assert_eq!(
                        diff.tabular.removed_cols.as_ref().unwrap().view_size.height,
                        6
                    );
                    assert_eq!(
                        diff.tabular.removed_cols.as_ref().unwrap().view_size.width,
                        1
                    );
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    // Test diff modify image passing the commit ids instead of the branch names
    #[tokio::test]
    async fn test_diff_entries_modify_image_pass_commit_ids() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images");
            util::fs::create_dir_all(&images_dir)?;

            // Add and commit the first cat
            let test_file = test::test_img_file_with_name("cat_1.jpg");
            let repo_filepath = images_dir.join(test_file.file_name().unwrap());
            util::fs::copy(&test_file, &repo_filepath)?;

            repositories::add(&repo, &images_dir)?;
            let og_commit = repositories::commit(&repo, "Adding initial cat image")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Create branch
            let branch_name = "feat/modify-dat-cat";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Modify and commit the first cat
            let repo_filepath = images_dir.join("cat_1.jpg");

            // Resize the image
            let dims = 96;
            util::image::resize_and_save(&repo_filepath, &repo_filepath, dims)?;

            repositories::add(&repo, &images_dir)?;
            let new_commit = repositories::commit(&repo, "Modifying the cat")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                branch_name,
            )
            .await?;

            let compare = api::client::diff::diff_entries(
                &remote_repo,
                &og_commit.id,
                &new_commit.id,
                &PathBuf::from("images").join("cat_1.jpg"),
            )
            .await?;

            println!("compare: {:#?}", compare);

            // Make sure base entry is empty
            assert!(compare.base_entry.is_some());
            assert!(compare.head_entry.is_some());
            let entry = compare.head_entry.as_ref().unwrap();

            assert_eq!(entry.filename, "cat_1.jpg");
            assert_eq!(
                PathBuf::from(&entry.resource.as_ref().unwrap().path),
                PathBuf::from("images").join("cat_1.jpg")
            );
            assert_eq!(compare.status, "modified");
            assert_eq!(entry.data_type, EntryDataType::Image);

            let metadata = entry.metadata.as_ref().unwrap();
            match metadata {
                GenericMetadata::MetadataImage(metadata) => {
                    assert_eq!(metadata.image.width, dims);
                    assert_eq!(metadata.image.height, dims);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_diff_entries_cifar_csvs() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // Track test.csv file
            let test_file = test::test_csv_file_with_name("test_cifar_2x9999.csv");
            let repo_filename = "test.csv";
            let repo_filepath = repo.path.join(repo_filename);
            util::fs::copy(&test_file, &repo_filepath)?;

            repositories::add(&repo, &repo_filepath)?;
            repositories::commit(&repo, "Adding test csv with two columns and 9999 rows")?;

            // Get the current branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Track train.csv file
            let test_file = test::test_csv_file_with_name("train_cifar_2x50000.csv");
            let repo_filename = "train.csv";
            let repo_filepath = repo.path.join(repo_filename);
            util::fs::copy(&test_file, &repo_filepath)?;

            repositories::add(&repo, &repo_filepath)?;
            repositories::commit(&repo, "Adding train csv with two columns and 50k rows")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Create branch
            let branch_name = "modify-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Modify test.csv file
            let test_file = test::test_csv_file_with_name("test_cifar_2x10000.csv");
            let repo_filename = "test.csv";
            let repo_filepath = repo.path.join(repo_filename);
            util::fs::copy(&test_file, &repo_filepath)?;

            repositories::add(&repo, &repo_filepath)?;
            repositories::commit(&repo, "Adding title row to csv")?;

            // Track train.csv file
            let test_file = test::test_csv_file_with_name("train_cifar_5x50000.csv");
            let repo_filename = "train.csv";
            let repo_filepath = repo.path.join(repo_filename);
            util::fs::copy(&test_file, &repo_filepath)?;

            repositories::add(&repo, &repo_filepath)?;
            repositories::commit(&repo, "Adding columns to train.csv")?;

            // Push it real good
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                branch_name,
            )
            .await?;

            let compare = api::client::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            assert_eq!(compare.entries.len(), 2);

            let test_csv = compare.entries.first().unwrap();
            assert_eq!(test_csv.filename, "test.csv");
            assert_eq!(test_csv.status, "modified");

            let summary = test_csv.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::TabularDiffWrapper(summary) => {
                    assert_eq!(summary.tabular.num_added_rows, 1);
                    assert_eq!(summary.tabular.num_removed_rows, 0);
                    assert_eq!(summary.tabular.num_added_cols, 0);
                    assert_eq!(summary.tabular.num_removed_cols, 0);
                    assert!(!summary.tabular.schema_has_changed);
                }
                _ => panic!("Wrong summary type"),
            }

            let test_csv = compare.entries.get(1).unwrap();
            assert_eq!(test_csv.filename, "train.csv");
            assert_eq!(test_csv.status, "modified");

            let summary = test_csv.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::TabularDiffWrapper(summary) => {
                    assert_eq!(summary.tabular.num_added_rows, 0);
                    assert_eq!(summary.tabular.num_removed_rows, 0);
                    assert_eq!(summary.tabular.num_added_cols, 3);
                    assert_eq!(summary.tabular.num_removed_cols, 0);
                    assert!(summary.tabular.schema_has_changed);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_diff_entries_added_images_in_dir() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images");
            util::fs::create_dir_all(&images_dir)?;

            // Add and commit the cats
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Get the current branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Create branch
            let branch_name = "add-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Add and commit the dogs
            for i in 1..=4 {
                let test_file = test::test_img_file_with_name(&format!("dog_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial dog images")?;

            // Set the proper remote
            // let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                branch_name,
            )
            .await?;

            let compare = api::client::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            println!("COMPARE: {:#?}", compare);

            // Added 4 dogs, one dir
            assert_eq!(compare.entries.len(), 5);

            let entry = compare.entries.first().unwrap();
            assert_eq!(entry.filename, "images");
            assert_eq!(entry.status, "modified");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();

            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 4);
                    assert_eq!(summary.dir.file_counts.removed, 0);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_diff_entries_added_images_in_subdirs() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images");
            let cats_dir = images_dir.join("cats");
            util::fs::create_dir_all(&cats_dir)?;

            // Add and commit the cats
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = cats_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &cats_dir)?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Get the current branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Create branch
            let branch_name = "add-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Add and commit the dogs
            let dogs_dir = images_dir.join("dogs");
            util::fs::create_dir_all(&dogs_dir)?;
            for i in 1..=4 {
                let test_file = test::test_img_file_with_name(&format!("dog_{i}.jpg"));
                let repo_filepath = dogs_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &dogs_dir)?;
            repositories::commit(&repo, "Adding initial dog images")?;

            // Modify a cat
            let test_file = test::test_img_file_with_name("cat_1.jpg");
            let repo_filepath = cats_dir.join(test_file.file_name().unwrap());

            // Resize the image
            let dims = 96;
            util::image::resize_and_save(&repo_filepath, &repo_filepath, dims)?;

            // Add the modification
            repositories::add(&repo, &repo_filepath)?;

            // Remove a cat
            let test_file = test::test_img_file_with_name("cat_2.jpg");
            let repo_filepath = cats_dir.join(test_file.file_name().unwrap());
            util::fs::remove_file(&repo_filepath)?;

            let rm_opts = RmOpts::from_path(Path::new("images").join("cats").join("cat_2.jpg"));
            repositories::rm(&repo, &rm_opts)?;
            repositories::commit(&repo, "Remove and modify some cats")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                branch_name,
            )
            .await?;

            let compare = api::client::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            println!("COMPARE: {:#?}", compare);

            // Added 4 dogs, modified 1 cat, removed 1 cat, three dirs
            assert_eq!(compare.entries.len(), 9);

            let entry = compare.entries.first().unwrap();
            assert_eq!(entry.filename, "images");
            assert_eq!(entry.status, "modified");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref();
            assert!(summary.is_some());
            // TODO: Fix this, should be modified 1
            // match summary {
            //     GenericDiffSummary::DirDiffSummary(summary) => {
            //         assert_eq!(summary.dir.file_counts.modified, 1);
            //         assert_eq!(summary.dir.file_counts.added, 4);
            //         assert_eq!(summary.dir.file_counts.removed, 1);
            //     }
            //     _ => panic!("Wrong summary type"),
            // }

            let entry = compare.entries.get(1).unwrap();
            assert_eq!(
                PathBuf::from(&entry.filename),
                PathBuf::from("images").join("cats")
            );
            assert_eq!(entry.status, "modified");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref();
            assert!(summary.is_some());
            // TODO: Fix this, should be modified 1
            // match summary {
            //     GenericDiffSummary::DirDiffSummary(summary) => {
            //         assert_eq!(summary.dir.file_counts.modified, 1);
            //         assert_eq!(summary.dir.file_counts.added, 0);
            //         assert_eq!(summary.dir.file_counts.removed, 1);
            //     }
            //     _ => panic!("Wrong summary type"),
            // }

            let entry = compare.entries.get(2).unwrap();
            assert_eq!(
                PathBuf::from(&entry.filename),
                PathBuf::from("images").join("dogs")
            );
            assert_eq!(entry.status, "added");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref();
            assert!(summary.is_some());
            // TODO: Fix this, should be modified 0
            // match summary {
            //     GenericDiffSummary::DirDiffSummary(summary) => {
            //         assert_eq!(summary.dir.file_counts.modified, 0);
            //         assert_eq!(summary.dir.file_counts.added, 4);
            //         assert_eq!(summary.dir.file_counts.removed, 0);
            //     }
            //     _ => panic!("Wrong summary type"),
            // }

            Ok(())
        })
        .await
    }

    /*
    #[tokio::test]
    async fn test_list_diff_entries_removing_images_in_subdir() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images").join("cats");
            util::fs::create_dir_all(&images_dir)?;

            // Add and commit the cats
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Get the current branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Create branch
            let branch_name = "remove-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Remove all the cat images
            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));
                util::fs::remove_file(&repo_filepath)?;
            }

            let mut rm_opts = RmOpts::from_path(Path::new("images").join("cats"));
            rm_opts.recursive = true;
            repositories::rm(&repo, &rm_opts)?;
            repositories::commit(&repo, "Removing cat images")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                branch_name,
            )
            .await?;

            let compare = api::client::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            println!("COMPARE: {:#?}", compare);

            // Removed 3 cats, two sub dirs
            assert_eq!(compare.entries.len(), 5);

            // images
            let entry = compare.entries.first().unwrap();
            assert_eq!(entry.filename, "images");
            assert_eq!(entry.status, "removed");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 0);
                    assert_eq!(summary.dir.file_counts.removed, 3);
                }
                _ => panic!("Wrong summary type"),
            }

            // images/cats
            let entry = compare.entries.get(1).unwrap();
            assert_eq!(
                PathBuf::from(&entry.filename),
                PathBuf::from("images").join("cats")
            );
            assert_eq!(entry.status, "removed");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 0);
                    assert_eq!(summary.dir.file_counts.removed, 3);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }
    */

    #[tokio::test]
    async fn test_list_diff_entries_removing_images_by_rming_parent_in_subdir(
    ) -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images").join("cats");
            util::fs::create_dir_all(&images_dir)?;

            // Add and commit the cats
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Get the current branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Create branch
            let branch_name = "remove-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Remove all the cat images
            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));
                util::fs::remove_file(&repo_filepath)?;
            }

            // THIS IS THE CRUX of this test, do not remove images/cats, just remove images/
            let mut rm_opts = RmOpts::from_path(Path::new("images"));
            rm_opts.recursive = true;
            repositories::rm(&repo, &rm_opts)?;
            repositories::commit(&repo, "Removing cat images")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                branch_name,
            )
            .await?;

            let compare = api::client::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            println!("COMPARE: {:#?}", compare);

            // Removed 3 cats, two parent dirs
            assert_eq!(compare.entries.len(), 5);

            // images
            let entry = compare.entries.first().unwrap();
            assert_eq!(entry.filename, "images");
            assert_eq!(entry.status, "removed");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 0);
                    assert_eq!(summary.dir.file_counts.removed, 3);
                }
                _ => panic!("Wrong summary type"),
            }

            // images/cats
            let entry = compare.entries.get(1).unwrap();
            assert_eq!(
                PathBuf::from(&entry.filename),
                PathBuf::from("images").join("cats")
            );
            assert_eq!(entry.status, "removed");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 0);
                    assert_eq!(summary.dir.file_counts.removed, 3);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_diff_entries_adding_images_in_one_subdir_two_levels() -> Result<(), OxenError>
    {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // create the images directory
            let cats_dir = repo.path.join("images").join("cats");
            util::fs::create_dir_all(&cats_dir)?;

            // Add and commit the cats
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = cats_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &cats_dir)?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Get the current branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Create branch
            let branch_name = "add-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Add some dog images
            let dogs_dir = repo.path.join("images").join("dogs").join("puppers");
            util::fs::create_dir_all(&dogs_dir)?;
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("dog_{i}.jpg"));
                let repo_filepath = dogs_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, dogs_dir)?;
            repositories::commit(&repo, "Adding dog images 🐕")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                branch_name,
            )
            .await?;

            let compare = api::client::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            println!("COMPARE: {:#?}", compare);

            // Added 3 dogs, 3 parent dirs
            assert_eq!(compare.entries.len(), 6);

            // images
            let entry = compare.entries.first().unwrap();
            assert_eq!(entry.filename, "images");
            assert_eq!(entry.status, "modified");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 3);
                    assert_eq!(summary.dir.file_counts.removed, 0);
                }
                _ => panic!("Wrong summary type"),
            }

            // images/dogs
            let entry = compare.entries.get(1).unwrap();
            assert_eq!(
                PathBuf::from(&entry.filename),
                PathBuf::from("images").join("dogs")
            );
            assert_eq!(entry.status, "added");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 3);
                    assert_eq!(summary.dir.file_counts.removed, 0);
                }
                _ => panic!("Wrong summary type"),
            }

            // images/dogs/puppers
            let entry = compare.entries.get(1).unwrap();
            assert_eq!(
                PathBuf::from(&entry.filename),
                PathBuf::from("images").join("dogs")
            );
            assert_eq!(entry.status, "added");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 3);
                    assert_eq!(summary.dir.file_counts.removed, 0);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_diff_entries_adding_images_in_subdirs() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // create the images directory
            let cats_dir = repo.path.join("images").join("cats");
            util::fs::create_dir_all(&cats_dir)?;

            // Add and commit the cats
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = cats_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &cats_dir)?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Get the current branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Create branch
            let branch_name = "add-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Add some dog images
            let dogs_dir = repo.path.join("images").join("dogs");
            util::fs::create_dir_all(&dogs_dir)?;
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("dog_{i}.jpg"));
                let repo_filepath = dogs_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, dogs_dir)?;
            repositories::commit(&repo, "Adding dog images")?;

            // Add dwight vince to the cats dir
            let test_file = test::test_img_file_with_name("dwight_vince.jpeg");
            let repo_filepath = cats_dir.join(test_file.file_name().unwrap());
            util::fs::copy(&test_file, &repo_filepath)?;

            repositories::add(&repo, cats_dir)?;
            repositories::commit(&repo, "Adding dwight/vince image to cats")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                branch_name,
            )
            .await?;

            let compare = api::client::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            println!("COMPARE: {:#?}", compare);

            // Added 3 dogs, added 1 dwight/vince, 3 parent dirs
            assert_eq!(compare.entries.len(), 7);

            // images
            let entry = compare.entries.first().unwrap();
            assert_eq!(entry.filename, "images");
            assert_eq!(entry.status, "modified");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 4);
                    assert_eq!(summary.dir.file_counts.removed, 0);
                }
                _ => panic!("Wrong summary type"),
            }

            // images/cats
            let entry = compare.entries.get(1).unwrap();
            assert_eq!(
                PathBuf::from(&entry.filename),
                PathBuf::from("images").join("cats")
            );
            assert_eq!(entry.status, "modified");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 1);
                    assert_eq!(summary.dir.file_counts.removed, 0);
                }
                _ => panic!("Wrong summary type"),
            }

            // images/cats
            let entry = compare.entries.get(2).unwrap();
            assert_eq!(
                PathBuf::from(&entry.filename),
                PathBuf::from("images").join("dogs")
            );
            assert_eq!(entry.status, "added");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 3);
                    assert_eq!(summary.dir.file_counts.removed, 0);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    /*
    #[tokio::test]
    async fn test_list_diff_entries_modifying_images_in_subdir() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images").join("cats");
            util::fs::create_dir_all(&images_dir)?;

            // Add and commit the cats
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Get the current branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Create branch
            let branch_name = "modify-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Remove all the cat images
            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));

                // Resize the image
                let dims = 96;
                util::image::resize_and_save(&repo_filepath, &repo_filepath, dims)?;
            }

            // THIS IS THE CRUX of this test, do not modify images/cats, just modify images/
            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Modify cat images")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                branch_name,
            )
            .await?;

            let compare = api::client::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            println!("COMPARE: {:#?}", compare);

            // Removed 3 cats, two parent dirs
            assert_eq!(compare.entries.len(), 5);

            // images
            let entry = compare.entries.first().unwrap();
            assert_eq!(entry.filename, "images");
            assert_eq!(entry.status, "modified");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 3);
                    assert_eq!(summary.dir.file_counts.added, 0);
                    assert_eq!(summary.dir.file_counts.removed, 0);
                }
                _ => panic!("Wrong summary type"),
            }

            // images/cats
            let entry = compare.entries.get(1).unwrap();
            assert_eq!(
                PathBuf::from(&entry.filename),
                PathBuf::from("images").join("cats")
            );
            assert_eq!(entry.status, "modified");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 3);
                    assert_eq!(summary.dir.file_counts.added, 0);
                    assert_eq!(summary.dir.file_counts.removed, 0);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }
    */

    #[tokio::test]
    async fn test_list_diff_entries_removing_images_in_dir() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images");
            util::fs::create_dir_all(&images_dir)?;

            // Add and commit the cats
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Get the current branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Create branch
            let branch_name = "remove-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Remove all the cat images
            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));
                util::fs::remove_file(&repo_filepath)?;
            }

            let mut rm_opts = RmOpts::from_path("images");
            rm_opts.recursive = true;
            repositories::rm(&repo, &rm_opts)?;
            repositories::commit(&repo, "Removing cat images")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Push new branch real good
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                branch_name,
            )
            .await?;

            let compare = api::client::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            // Removed 3 cats, one dir
            assert_eq!(compare.entries.len(), 4);

            let entry = compare.entries.first().unwrap();
            assert_eq!(entry.filename, "images");
            assert_eq!(entry.status, "removed");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    assert_eq!(summary.dir.file_counts.modified, 0);
                    assert_eq!(summary.dir.file_counts.added, 0);
                    assert_eq!(summary.dir.file_counts.removed, 3);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_diff_entries_changed_images_in_dir() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // create the images directory
            let images_dir = repo.path.join("images");
            util::fs::create_dir_all(&images_dir)?;

            // Add and commit the cats
            for i in 1..=3 {
                let test_file = test::test_img_file_with_name(&format!("cat_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial cat images")?;

            // Get the current branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Add and commit the dogs
            for i in 1..=4 {
                let test_file = test::test_img_file_with_name(&format!("dog_{i}.jpg"));
                let repo_filepath = images_dir.join(test_file.file_name().unwrap());
                util::fs::copy(&test_file, &repo_filepath)?;
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding initial dog images")?;

            // Set the proper remote
            let remote = test::repo_remote_url_from(&repo.dirname());
            command::config::set_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            // Create Remote
            let remote_repo = test::create_remote_repo(&repo).await?;

            // Push it real good
            repositories::push(&repo).await?;

            // Create branch
            let branch_name = "modify-data";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Resize all the cat images
            for i in 1..=3 {
                let repo_filepath = images_dir.join(format!("cat_{i}.jpg"));

                // Resize the image
                let dims = 96;
                util::image::resize_and_save(&repo_filepath, &repo_filepath, dims)?;
            }

            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Resized all the cats")?;

            // Remove one of the dogs
            let repo_filepath = PathBuf::from("images").join("dog_1.jpg");

            let rm_opts = RmOpts::from_path(repo_filepath);
            repositories::rm(&repo, &rm_opts)?;
            repositories::commit(&repo, "Removing dog")?;

            // Add dwight howard and vince carter
            let test_file = test::test_img_file_with_name("dwight_vince.jpeg");
            let repo_filepath = images_dir.join(test_file.file_name().unwrap());
            util::fs::copy(&test_file, &repo_filepath)?;
            repositories::add(&repo, &images_dir)?;
            repositories::commit(&repo, "Adding dwight and vince")?;

            // Push it real good
            repositories::push::push_remote_branch(
                &repo,
                constants::DEFAULT_REMOTE_NAME,
                branch_name,
            )
            .await?;

            let compare = api::client::diff::list_diff_entries(
                &remote_repo,
                &og_branch.name,
                &branch_name,
                0,
                100,
            )
            .await?;

            for entry in &compare.entries {
                println!(
                    "COMPARE ENTRY {:?} -> {} -> {}",
                    entry.filename, entry.data_type, entry.status
                );
                println!("Diff {:#?}", entry.diff_summary);
            }

            // removed 1 dog, add 1 dog, modified 3 cats, modified 1 directory
            assert_eq!(compare.entries.len(), 6);

            let entry = compare.entries.first().unwrap();
            assert_eq!(entry.filename, "images");
            assert_eq!(entry.status, "modified");
            assert_eq!(entry.data_type, EntryDataType::Dir);

            let summary = entry.diff_summary.as_ref().unwrap();
            match summary {
                GenericDiffSummary::DirDiffSummary(summary) => {
                    // TODO: Fix this, should be modified 3 and removed 1
                    // assert_eq!(summary.dir.file_counts.modified, 3);
                    assert_eq!(summary.dir.file_counts.added, 1);
                    // assert_eq!(summary.dir.file_counts.removed, 1);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }
}
