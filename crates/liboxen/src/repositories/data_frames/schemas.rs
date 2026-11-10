//! # oxen schemas
//!
//! Interact with schemas
//!

use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, Schema};
use crate::repositories;
use crate::tasks;

use std::path::Path;

pub use crate::core::v_latest::data_frames::schemas::list;

pub use crate::core::v_latest::data_frames::schemas::get_by_path;

/// [`get_by_path`], off the async worker.
pub async fn get_by_path_async(
    repo: &LocalRepository,
    commit: &Commit,
    path: &Path,
) -> Result<Option<Schema>, OxenError> {
    let repo = repo.clone();
    let commit = commit.clone();
    let path = path.to_path_buf();
    tasks::spawn_blocking(move || get_by_path(&repo, &commit, path)).await?
}

/// Get a staged schema
pub use crate::core::v_latest::data_frames::schemas::get_staged;

/// Get staged schema for workspace
pub use crate::core::v_latest::data_frames::schemas::get_staged_schema_with_staged_db_manager;

/// [`get_staged_schema_with_staged_db_manager`], off the async worker.
pub async fn get_staged_schema_with_staged_db_manager_async(
    repo: &LocalRepository,
    path: &Path,
) -> Result<Option<Schema>, OxenError> {
    let repo = repo.clone();
    let path = path.to_path_buf();
    tasks::spawn_blocking(move || get_staged_schema_with_staged_db_manager(&repo, path)).await?
}

/// List all the staged schemas
pub use crate::core::v_latest::data_frames::schemas::list_staged;

/// Get a string representation of the schema given a schema ref
pub fn show(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    staged: bool,
    verbose: bool,
) -> Result<String, OxenError> {
    let path = path.as_ref();
    let schema = if staged {
        get_staged(repo, path)?
    } else {
        match repositories::commits::head_commit_maybe(repo)? {
            Some(commit) => repositories::data_frames::schemas::get_by_path(repo, &commit, path)?,
            None => None,
        }
    };

    log::debug!("show: {schema:?}");
    let Some(schema) = schema else {
        return Err(OxenError::schema_does_not_exist(path));
    };

    let mut results = String::new();
    if verbose {
        let verbose_str = schema.verbose_str();
        results.push_str(&format!(
            "{} {}\n{}\n",
            path.to_string_lossy(),
            schema.hash,
            verbose_str
        ));
    } else {
        results.push_str(&format!(
            "{}\t{}\t{}",
            path.to_string_lossy(),
            schema.hash,
            schema
        ))
    }
    Ok(results)
}

/// Remove a schema override from the staging area, TODO: Currently undefined behavior for non-staged schemas
pub use crate::core::v_latest::data_frames::schemas::rm;

/// Add metadata to the schema
pub use crate::core::v_latest::data_frames::schemas::add_schema_metadata;

/// Add metadata to a specific column
pub use crate::core::v_latest::data_frames::schemas::add_column_metadata;

// unit tests
#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::model::Schema;
    use crate::test;
    use crate::util;
    use crate::{command, repositories};

    use serde_json::json;
    use std::path::{Path, PathBuf};
    use std::sync::Barrier;

    #[tokio::test]
    async fn test_list_staged_serves_simultaneous_callers() -> Result<(), OxenError> {
        // Listing staged schemas is a read. Concurrent readers all have to get an answer rather
        // than one of them failing on RocksDB's exclusive per-path lock.
        test::run_empty_local_repo_test_async(|repo| async move {
            const CALLERS: usize = 8;
            let barrier = Barrier::new(CALLERS);
            let results = std::thread::scope(|scope| {
                let threads: Vec<_> = (0..CALLERS)
                    .map(|_| {
                        scope.spawn(|| {
                            barrier.wait();
                            repositories::data_frames::schemas::list_staged(&repo)
                        })
                    })
                    .collect();
                threads
                    .into_iter()
                    .map(|thread| thread.join().expect("listing thread panicked"))
                    .collect::<Vec<_>>()
            });

            for result in results {
                assert!(result?.is_empty(), "nothing is staged in an empty repo");
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_schema_list() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let commit = repositories::commits::head_commit(&repo)?;
            let schemas = repositories::data_frames::schemas::list(&repo, &commit)?;
            assert_eq!(schemas.len(), 8);
            let path = PathBuf::from("annotations")
                .join("train")
                .join("bounding_box.csv");

            let schema = repositories::data_frames::schemas::get_by_path(&repo, &commit, &path)?
                .expect("bounding_box.csv carries a schema");
            assert_eq!(schema.hash, "b821946753334c083124fd563377d795");
            assert_bounding_box_fields(&schema);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_stage_and_commit_schema() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|repo| async move {
            // Make sure no schemas are staged
            let status = repositories::status(&repo).await?;
            assert_eq!(status.staged_schemas.len(), 0);

            // Schema should be staged when added
            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(&bbox_filename);
            repositories::add(&repo, bbox_file).await?;

            // Make sure it is staged
            let status = repositories::status(&repo).await?;
            assert_eq!(status.staged_schemas.len(), 1);

            // Schema should be committed after commit
            let commit = repositories::commit(&repo, "Adding bounding box schema")?;

            // Make sure no schemas are staged after commit
            let status = repositories::status(&repo).await?;
            assert_eq!(status.staged_schemas.len(), 0);

            // Fetch schema from HEAD commit
            let schema =
                repositories::data_frames::schemas::get_by_path(&repo, &commit, &bbox_filename)?
                    .expect("the committed csv carries a schema");
            assert_eq!(schema.hash, "b821946753334c083124fd563377d795");
            assert_bounding_box_fields(&schema);

            // Write a new commit that is modifies any file
            let readme_file = repo.path.join("README.md");
            util::fs::write(&readme_file, "Changing the README")?;
            repositories::add(&repo, readme_file).await?;
            let commit = repositories::commit(&repo, "Changing the README")?;

            // Fetch schema from HEAD commit, it should still be there in all it's glory
            let schema =
                repositories::data_frames::schemas::get_by_path(&repo, &commit, &bbox_filename)?
                    .expect("a commit that leaves the csv alone keeps its parent's schema");
            assert_eq!(schema.hash, "b821946753334c083124fd563377d795");
            assert_bounding_box_fields(&schema);

            Ok(())
        })
        .await
    }

    /// Asserts the columns of `annotations/train/bounding_box.csv`.
    fn assert_bounding_box_fields(schema: &Schema) {
        let fields: Vec<(&str, &str)> = schema
            .fields
            .iter()
            .map(|field| (field.name.as_str(), field.dtype.as_str()))
            .collect();
        assert_eq!(
            fields,
            [
                ("file", "str"),
                ("label", "str"),
                ("min_x", "f64"),
                ("min_y", "f64"),
                ("width", "i64"),
                ("height", "i64"),
            ]
        );
    }

    // Each step starts from the schema the step before it left, staged or committed.
    #[tokio::test]
    async fn test_schemas_column_and_schema_metadata() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            let bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_path = repo.path.join(&bbox_file);
            let get_staged = || -> Result<Schema, OxenError> {
                Ok(
                    repositories::data_frames::schemas::get_staged(&repo, &bbox_file)?
                        .expect("the csv has a staged schema"),
                )
            };

            // Stage the file
            repositories::add(&repo, &bbox_path).await?;
            assert_bounding_box_fields(&get_staged()?);

            let file_metadata = json!({
                "root": "images"
            });
            repositories::data_frames::schemas::add_column_metadata(
                &repo,
                &bbox_file,
                "file",
                &file_metadata,
            )?;
            let schema = get_staged()?;
            assert_bounding_box_fields(&schema);
            assert_eq!(schema.fields[0].metadata, Some(file_metadata.to_owned()));

            // Commit the schema
            let commit = repositories::commit(&repo, "Adding bounding box schema")?;
            let schemas = repositories::data_frames::schemas::list(&repo, &commit)?;
            assert_eq!(schemas.len(), 1);
            let schema = schemas.values().next().unwrap();
            assert_bounding_box_fields(schema);
            assert_eq!(schema.fields[0].metadata, Some(file_metadata.to_owned()));

            // Add schema metadata to the committed schema
            let schema_metadata = json!({
                "task": "bounding_box",
                "description": "detect some bounding boxes"
            });
            repositories::data_frames::schemas::add_schema_metadata(
                &repo,
                &bbox_file,
                &schema_metadata,
            )?;
            assert_eq!(get_staged()?.metadata, Some(schema_metadata.to_owned()));
            // Make sure to do this last for this test, because then we get str instead of path as the dtype_override
            repositories::data_frames::schemas::add_column_metadata(
                &repo,
                &bbox_file,
                "file",
                &file_metadata,
            )?;
            let schema = get_staged()?;
            assert_eq!(schema.metadata, Some(schema_metadata.to_owned()));
            assert_eq!(schema.fields[0].metadata, Some(file_metadata.to_owned()));

            // Adding the unchanged file again keeps its staged schema change
            repositories::add(&repo, &bbox_path).await?;
            assert_eq!(get_staged()?.metadata, Some(schema_metadata));

            // Update the schema
            let min_x_metadata = json!({
                "key": "val"
            });
            let updated_schemas = repositories::data_frames::schemas::add_column_metadata(
                &repo,
                &bbox_file,
                "min_x",
                &min_x_metadata,
            )?;
            let updated_schema = updated_schemas
                .get(&bbox_file)
                .expect("Expected to find updated schema");
            let schema = get_staged()?;
            assert_eq!(updated_schema.hash, schema.hash);
            assert_bounding_box_fields(&schema);
            // this was added in the previous commit, so it should still be there
            assert_eq!(schema.fields[0].metadata, Some(file_metadata.to_owned()));
            assert_eq!(schema.fields[2].metadata, Some(min_x_metadata.to_owned()));

            // Commit the schema again
            repositories::commit(&repo, "Updating the bounding box schema")?;

            // Update the schema
            let width_metadata = json!({
                "metric": "meters"
            });
            let updated_schemas = repositories::data_frames::schemas::add_column_metadata(
                &repo,
                &bbox_file,
                "width",
                &width_metadata,
            )?;
            let updated_schema = updated_schemas
                .get(&bbox_file)
                .expect("Expected to find updated schema");
            let schema = get_staged()?;
            assert_eq!(updated_schema.hash, schema.hash);
            assert_bounding_box_fields(&schema);
            // these were added in the previous commits, so they should still be there
            assert_eq!(schema.fields[0].metadata, Some(file_metadata.to_owned()));
            assert_eq!(schema.fields[2].metadata, Some(min_x_metadata));
            assert_eq!(schema.fields[4].metadata, Some(width_metadata));

            // Commit the schema, then add a new column to the data frame
            repositories::commit(&repo, "Adding metadata to width column")?;
            command::df::add_column(&bbox_path, "new_column:0:i32").await?;
            repositories::add(&repo, &bbox_path).await?;

            // Make sure the metadata persisted
            let schema = get_staged()?;
            assert_eq!(schema.fields.len(), 7);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[0].metadata, Some(file_metadata));

            // Remove the staged schema
            repositories::data_frames::schemas::rm(&repo, &bbox_file, true)?;
            assert!(
                repositories::data_frames::schemas::get_staged(&repo, &bbox_file)?.is_none(),
                "no staged schema is left after removing it"
            );

            Ok(())
        })
        .await
    }

    /// The staged db holds directory markers and non-tabular files next to data
    /// frames, so listing staged schemas has to skip them rather than fail.
    #[tokio::test]
    async fn test_schemas_list_staged_skips_entries_without_a_schema() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let csv_path = repo.path.join("nested").join("data.csv");
            util::fs::create_dir_all(csv_path.parent().unwrap())?;
            test::write_txt_file_to_path(&csv_path, "a,b\n1,2\n")?;
            let txt_path = repo.path.join("nested").join("readme.txt");
            test::write_txt_file_to_path(&txt_path, "not a data frame\n")?;

            // Staging the csv also stages `nested` and the repo root as directory markers.
            repositories::add(&repo, &csv_path).await?;
            repositories::add(&repo, &txt_path).await?;

            let staged = repositories::data_frames::schemas::list_staged(&repo)?;
            let paths: Vec<&PathBuf> = staged.keys().collect();
            assert_eq!(
                paths.len(),
                1,
                "only the data frame has a staged schema, got {paths:?}"
            );
            let schema = staged
                .get(&PathBuf::from("nested").join("data.csv"))
                .expect("the csv's schema is listed under its repo-relative path");
            let names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
            assert_eq!(names, vec!["a", "b"]);

            Ok(())
        })
        .await
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_schemas_merge_fast_forward_pull() -> Result<(), OxenError> {
        test::run_select_data_sync_remote("README.md", |_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            test::run_empty_dir_test_async(|repo_dir_a| async move {
                let repo_dir_a = repo_dir_a.join("repo_a");
                let cloned_repo_a =
                    repositories::clone_url(&remote_repo.remote.url, &repo_dir_a).await?;

                test::run_empty_dir_test_async(|repo_dir_b| async move {
                    println!("=== Starting test: Two users pushing to remote ===");
                    println!("User A: Will add schema metadata to dataframe");
                    println!("User B: Will edit README");

                    // Write a data frame to data.csv
                    let data_filename = Path::new("data.csv");
                    let data_path = cloned_repo_a.path.join(data_filename);
                    let column_name = "name";
                    let data_content = "name,age\nJohn,30\nJane,25";
                    util::fs::write_to_path(&data_path, data_content)?;
                    repositories::add(&cloned_repo_a, &data_path).await?;
                    repositories::commit(&cloned_repo_a, "User A adding data.csv")?;
                    // Push to the remote so User B can pull
                    repositories::push(&cloned_repo_a).await?;

                    // Clone repo to User B before we edit the schema metadata
                    let repo_dir_b = repo_dir_b.join("repo_b");
                    let cloned_repo_b =
                        repositories::clone_url(&remote_repo.remote.url, &repo_dir_b).await?;

                    // User A: Add schema metadata to the data.csv dataframe
                    println!("User A: Adding schema metadata...");
                    // Add schema metadata
                    let column_metadata = json!({
                        "my_custom": "name_column_metadata"
                    });
                    repositories::data_frames::schemas::add_column_metadata(
                        &cloned_repo_a,
                        data_filename,
                        column_name,
                        &column_metadata,
                    )?;
                    repositories::commit(&cloned_repo_a, "User A adding schema metadata")?;
                    println!("User A: Pushing schema metadata...");
                    repositories::push(&cloned_repo_a).await?;
                    println!("User A pushed ✅ 1");

                    // Make sure the schema metadata is in the schema
                    let commit = repositories::commits::head_commit(&cloned_repo_a)?;
                    let schema = repositories::data_frames::schemas::get_by_path(
                        &cloned_repo_a,
                        &commit,
                        data_filename,
                    )?
                    .expect("Schema should exist");
                    println!("User A: Schema: {schema:?}");

                    // Save the number of commits in User A
                    let num_commits_a = repositories::commits::list(&cloned_repo_a)?.len();
                    println!("User A: Number of commits: {num_commits_a}");
                    let commits = repositories::commits::list(&cloned_repo_a)?;
                    for commit in &commits {
                        println!("User A: GOT COMMIT {commit}");
                    }
                    println!("================================================");

                    // User B: Edit the README (but don't push yet)
                    println!("User B: Editing README...");
                    let readme_path = cloned_repo_b.path.join("README.md");
                    let readme_content = "Updated README by User B";
                    util::fs::write_to_path(&readme_path, readme_content)?;
                    repositories::add(&cloned_repo_b, &readme_path).await?;
                    repositories::commit(&cloned_repo_b, "User B editing README")?;

                    // User B should have all the commits from User A
                    let commits = repositories::commits::list(&cloned_repo_b)?;
                    for commit in &commits {
                        println!("User B: GOT COMMIT BEFORE {commit}");
                    }
                    println!("================================================");

                    // User B tries to push - should fail because remote is ahead
                    println!("User B: Attempting to push README changes (should fail because remote is ahead)...");
                    let push_result = repositories::push(&cloned_repo_b).await;
                    assert!(push_result.is_err(), "Push should fail when remote is ahead");
                    println!("User B push failed ✅ 3 (as expected - remote is ahead)");

                    // User B pulls - should succeed with fast-forward merge (no conflicts)
                    println!("User B: Pulling changes...");
                    repositories::pull(&cloned_repo_b).await?;
                    println!("User B pulled ✅ 4");

                    // User B should have all the commits from User A
                    let commits = repositories::commits::list(&cloned_repo_b)?;
                    for commit in &commits {
                        println!("User B: GOT COMMIT AFTER {commit}");
                    }
                    println!("================================================");
                    // assert_eq!(commits.len(), num_commits_a);

                    // Verify User B has both changes locally:
                    // 1. The README should still have User B's changes
                    let readme_content_after_pull = util::fs::read_from_path(&readme_path)?;
                    assert_eq!(
                        readme_content_after_pull, readme_content,
                        "README should still have User B's changes"
                    );

                    // 2. The schema should have User A's metadata
                    let commit = repositories::commits::head_commit(&cloned_repo_b)?;
                    println!("User B: CHECKING COMMIT {commit}");
                    let schema = repositories::data_frames::schemas::get_by_path(
                        &cloned_repo_b,
                        &commit,
                        data_filename,
                    )?
                    .expect("Schema should exist");
                    println!("User B: Schema: {schema:?}");
                    let name_field = schema.fields.iter()
                        .find(|field| field.name == column_name)
                        .expect("Schema should have a 'name' field");
                    assert_eq!(
                        name_field.metadata, Some(column_metadata.clone()),
                        "Schema should have User A's metadata on the 'name' field"
                    );

                    // User B pushes - should succeed now after pulling
                    println!("User B: Pushing README changes (should succeed after pull)...");
                    repositories::push(&cloned_repo_b).await?;
                    println!("User B pushed ✅ 5");

                    Ok(())
                })
                .await?;
                Ok(())
            })
            .await?;
            Ok(remote_repo_copy)
        })
        .await
    }
}
