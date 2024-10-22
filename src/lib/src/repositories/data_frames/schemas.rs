//! # oxen schemas
//!
//! Interact with schemas
//!

use std::collections::HashMap;
use std::path::PathBuf;

use crate::core;
use crate::core::versions::MinOxenVersion;

use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, Schema};
use crate::repositories;

use std::path::Path;

pub fn list(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::data_frames::schemas::list(repo, commit),
        MinOxenVersion::V0_19_0 => core::v0_19_0::data_frames::schemas::list(repo, commit),
    }
}

pub fn get_by_path(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
) -> Result<Option<Schema>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::data_frames::schemas::get_by_path(repo, commit, path)
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::data_frames::schemas::get_by_path(repo, commit, path)
        }
    }
}

/// Get a staged schema
pub fn get_staged(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
) -> Result<Option<Schema>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::data_frames::schemas::get_staged(repo, path),
        MinOxenVersion::V0_19_0 => core::v0_19_0::data_frames::schemas::get_staged(repo, path),
    }
}

/// List all the staged schemas
pub fn list_staged(repo: &LocalRepository) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::data_frames::schemas::list_staged(repo),
        MinOxenVersion::V0_19_0 => core::v0_19_0::data_frames::schemas::list_staged(repo),
    }
}

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
pub fn rm(repo: &LocalRepository, path: impl AsRef<Path>, staged: bool) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => core::v0_10_0::data_frames::schemas::rm(repo, path, staged),
        MinOxenVersion::V0_19_0 => core::v0_19_0::data_frames::schemas::rm(repo, path, staged),
    }
}

/// Add metadata to the schema
pub fn add_schema_metadata(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    metadata: &serde_json::Value,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::data_frames::schemas::add_schema_metadata(repo, path, metadata)
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::data_frames::schemas::add_schema_metadata(repo, path, metadata)
        }
    }
}

/// Add metadata to a specific column
pub fn add_column_metadata(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    column: impl AsRef<str>,
    metadata: &serde_json::Value,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            core::v0_10_0::data_frames::schemas::add_column_metadata(repo, path, column, metadata)
        }
        MinOxenVersion::V0_19_0 => {
            core::v0_19_0::data_frames::schemas::add_column_metadata(repo, path, column, metadata)
        }
    }
}

// unit tests
#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::test;
    use crate::util;
    use crate::{command, repositories};

    use serde_json::json;
    use std::path::{Path, PathBuf};

    #[test]
    fn test_command_schema_list() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed(|repo| {
            let commit = repositories::commits::head_commit(&repo)?;
            let schemas = repositories::data_frames::schemas::list(&repo, &commit)?;
            assert_eq!(schemas.len(), 7);
            let path = PathBuf::from("annotations")
                .join("train")
                .join("bounding_box.csv");

            let schema =
                repositories::data_frames::schemas::get_by_path(&repo, &commit, &path)?.unwrap();
            assert_eq!(schema.hash, "b821946753334c083124fd563377d795");
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[1].name, "label");
            assert_eq!(schema.fields[1].dtype, "str");
            assert_eq!(schema.fields[2].name, "min_x");
            assert_eq!(schema.fields[2].dtype, "f64");
            assert_eq!(schema.fields[3].name, "min_y");
            assert_eq!(schema.fields[3].dtype, "f64");
            assert_eq!(schema.fields[4].name, "width");
            assert_eq!(schema.fields[4].dtype, "i64");
            assert_eq!(schema.fields[5].name, "height");
            assert_eq!(schema.fields[5].dtype, "i64");

            Ok(())
        })
    }

    #[test]
    fn test_stage_and_commit_schema() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // Make sure no schemas are staged
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_schemas.len(), 0);

            // Schema should be staged when added
            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(bbox_filename);
            repositories::add(&repo, bbox_file)?;

            // Make sure it is staged
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_schemas.len(), 1);
            for (path, schema) in status.staged_schemas.iter() {
                println!("GOT SCHEMA {path:?} -> {schema:?}");
            }

            // Schema should be committed after commit
            let commit = repositories::commit(&repo, "Adding bounding box schema")?;

            // Make sure no schemas are staged after commit
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_schemas.len(), 0);
            let path = PathBuf::from("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Fetch schema from HEAD commit
            let schema =
                repositories::data_frames::schemas::get_by_path(&repo, &commit, &path)?.unwrap();
            assert_eq!(schema.hash, "b821946753334c083124fd563377d795");
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[1].name, "label");
            assert_eq!(schema.fields[1].dtype, "str");
            assert_eq!(schema.fields[2].name, "min_x");
            assert_eq!(schema.fields[2].dtype, "f64");
            assert_eq!(schema.fields[3].name, "min_y");
            assert_eq!(schema.fields[3].dtype, "f64");
            assert_eq!(schema.fields[4].name, "width");
            assert_eq!(schema.fields[4].dtype, "i64");
            assert_eq!(schema.fields[5].name, "height");
            assert_eq!(schema.fields[5].dtype, "i64");

            Ok(())
        })
    }

    #[test]
    fn test_copy_schemas_from_parent() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            // Make sure no schemas are staged
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_schemas.len(), 0);

            // Schema should be staged when added
            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(&bbox_filename);
            repositories::add(&repo, bbox_file)?;

            // Make sure it is staged
            let status = repositories::status(&repo)?;
            assert_eq!(status.staged_schemas.len(), 1);
            for (path, schema) in status.staged_schemas.iter() {
                println!("GOT SCHEMA {path:?} -> {schema:?}");
            }

            // Schema should be committed after commit
            repositories::commit(&repo, "Adding bounding box schema")?;

            // Write a new commit that is modifies any file
            let readme_filename = Path::new("README.md");
            let readme_file = repo.path.join(readme_filename);
            util::fs::write(&readme_file, "Changing the README")?;
            repositories::add(&repo, readme_file)?;
            let commit = repositories::commit(&repo, "Changing the README")?;

            // Fetch schema from HEAD commit, it should still be there in all it's glory
            let maybe_schema =
                repositories::data_frames::schemas::get_by_path(&repo, &commit, &bbox_filename)?;
            assert!(maybe_schema.is_some());

            let schema = maybe_schema.unwrap();
            assert_eq!(schema.hash, "b821946753334c083124fd563377d795");
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[1].name, "label");
            assert_eq!(schema.fields[1].dtype, "str");
            assert_eq!(schema.fields[2].name, "min_x");
            assert_eq!(schema.fields[2].dtype, "f64");
            assert_eq!(schema.fields[3].name, "min_y");
            assert_eq!(schema.fields[3].dtype, "f64");
            assert_eq!(schema.fields[4].name, "width");
            assert_eq!(schema.fields[4].dtype, "i64");
            assert_eq!(schema.fields[5].name, "height");
            assert_eq!(schema.fields[5].dtype, "i64");

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_schemas_add_staged() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Add the file
            repositories::add(&repo, &bbox_path)?;

            // Make sure it is staged
            let bbox_file = util::fs::path_relative_to_dir(&bbox_path, &repo.path)?;
            let schema =
                repositories::data_frames::schemas::get_staged(&repo, &bbox_path)?.unwrap();
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[1].name, "label");
            assert_eq!(schema.fields[1].dtype, "str");

            assert_eq!(schema.fields[2].name, "min_x");
            assert_eq!(schema.fields[2].dtype, "f64");

            assert_eq!(schema.fields[3].name, "min_y");
            assert_eq!(schema.fields[3].dtype, "f64");

            assert_eq!(schema.fields[4].name, "width");
            assert_eq!(schema.fields[4].dtype, "i64");
            assert_eq!(schema.fields[5].name, "height");
            assert_eq!(schema.fields[5].dtype, "i64");

            // Update the schema
            let min_x_meta = json!({
                "key": "val"
            });
            let updated_schemas = repositories::data_frames::schemas::add_column_metadata(
                &repo,
                &bbox_path,
                "min_x",
                &min_x_meta,
            )?;
            let updated_schema = updated_schemas
                .get(&bbox_file)
                .expect("Expected to find updated schema");
            let schema =
                repositories::data_frames::schemas::get_staged(&repo, &bbox_path)?.unwrap();
            assert_eq!(updated_schema.hash, schema.hash);
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[1].name, "label");
            assert_eq!(schema.fields[1].dtype, "str");

            assert_eq!(schema.fields[2].name, "min_x");
            assert_eq!(schema.fields[2].dtype, "f64");
            assert_eq!(schema.fields[2].metadata, Some(min_x_meta));

            assert_eq!(schema.fields[3].name, "min_y");
            assert_eq!(schema.fields[3].dtype, "f64");

            assert_eq!(schema.fields[4].name, "width");
            assert_eq!(schema.fields[4].dtype, "i64");
            assert_eq!(schema.fields[5].name, "height");
            assert_eq!(schema.fields[5].dtype, "i64");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_schemas_schema_rm_staged() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = util::fs::path_relative_to_dir(&bbox_path, &repo.path)?;
            let schema_ref = bbox_file.to_str().unwrap();

            // Add the schema
            let min_x_meta = json!({
                "key": "val"
            });
            repositories::add(&repo, &bbox_path)?;
            repositories::data_frames::schemas::add_column_metadata(
                &repo,
                schema_ref,
                "min_x",
                &min_x_meta,
            )?;

            let schema = repositories::data_frames::schemas::get_staged(&repo, schema_ref)?;
            assert!(schema.is_some());

            // Remove the schema
            repositories::data_frames::schemas::rm(&repo, schema_ref, true)?;

            // Make sure none are left
            let schema = repositories::data_frames::schemas::get_staged(&repo, schema_ref)?;
            assert!(schema.is_none());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_schemas_add_schema_metadata() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = util::fs::path_relative_to_dir(&bbox_path, &repo.path)?;

            // Add and commit the schema
            repositories::add(&repo, &bbox_path)?;
            repositories::commit(&repo, "Adding bounding box file")?;

            // Add the schema
            let metadata = json!({
                "task": "bounding_box",
                "description": "detect some bounding boxes"
            });
            repositories::data_frames::schemas::add_schema_metadata(&repo, &bbox_file, &metadata)?;

            let schema =
                repositories::data_frames::schemas::get_staged(&repo, &bbox_file)?.unwrap();
            assert_eq!(schema.metadata, Some(metadata));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_schemas_add_schema_metadata_and_col_metadata() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = util::fs::path_relative_to_dir(&bbox_path, &repo.path)?;

            // Add and commit the schema
            repositories::add(&repo, &bbox_path)?;
            repositories::commit(&repo, "Adding bounding box file")?;

            // Add the schema metadata
            let schema_metadata = json!({
                "task": "bounding_box",
                "description": "detect some bounding boxes"
            });
            let column_name = "file".to_string();
            let column_metadata = json!({
                "root": "images"
            });
            repositories::data_frames::schemas::add_schema_metadata(
                &repo,
                &bbox_file,
                &schema_metadata,
            )?;
            // Make sure to do this last for this test, because then we get str instead of path as the dtype_override
            repositories::data_frames::schemas::add_column_metadata(
                &repo,
                &bbox_file,
                column_name,
                &column_metadata,
            )?;

            let schema =
                repositories::data_frames::schemas::get_staged(&repo, &bbox_file)?.unwrap();
            assert_eq!(schema.metadata, Some(schema_metadata));
            assert_eq!(schema.fields[0].metadata, Some(column_metadata));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_schemas_add_column_metadata() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_path = repo.path.join(&bbox_file);

            // Stage the file
            repositories::add(&repo, &bbox_path)?;

            let status = repositories::status(&repo)?;
            println!("status: {:?}", status);
            status.print();

            // Add the schema
            let metadata = json!({
                "root": "images"
            });
            repositories::data_frames::schemas::add_column_metadata(
                &repo, &bbox_file, "file", &metadata,
            )?;
            let schema =
                repositories::data_frames::schemas::get_staged(&repo, &bbox_file)?.unwrap();
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[0].metadata, Some(metadata));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_schemas_add_column_to_committed_schema2() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_file = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_path = repo.path.join(&bbox_file);

            // Add the schema
            repositories::add(&repo, &bbox_path)?;
            let commit = repositories::commit(&repo, "Adding bounding box file")?;

            let schemas = repositories::data_frames::schemas::list(&repo, &commit)?;
            for (path, schema) in schemas.iter() {
                println!("GOT SCHEMA {path:?} -> {schema:?}");
            }

            // Add the schema
            let metadata = json!({
                "root": "images"
            });

            repositories::add(&repo, &bbox_path)?;
            repositories::data_frames::schemas::add_column_metadata(
                &repo, &bbox_file, "file", &metadata,
            )?;

            let schema =
                repositories::data_frames::schemas::get_staged(&repo, &bbox_file)?.unwrap();
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[0].metadata, Some(metadata.to_owned()));

            // Commit the schema
            let commit = repositories::commit(&repo, "Adding metadata to file column")?;

            let tree = repositories::tree::get_by_commit(&repo, &commit)?;
            println!("TREE");
            tree.print();

            // List the committed schemas
            let schemas = repositories::data_frames::schemas::list(&repo, &commit)?;
            for (path, schema) in schemas.iter() {
                println!("GOT SCHEMA {path:?} -> {schema:?}");
            }

            assert_eq!(schemas.len(), 1);
            // assert_eq!(schema_ref, schemas.keys().next().unwrap().to_string_lossy());
            let schema = schemas.values().next().unwrap();
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[0].metadata, Some(metadata));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_schemas_add_column_to_committed_schema_after_changing_data(
    ) -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Add the schema
            repositories::add(&repo, &bbox_path)?;
            let commit = repositories::commit(&repo, "Adding bounding box file")?;

            let schemas = repositories::data_frames::schemas::list(&repo, &commit)?;
            for (path, schema) in schemas.iter() {
                println!("GOT SCHEMA {path:?} -> {schema:?}");
            }

            let bbox_file = util::fs::path_relative_to_dir(&bbox_path, &repo.path)?;

            // Add the schema metadata
            let metadata = json!({
                "root": "images"
            });
            repositories::data_frames::schemas::add_column_metadata(
                &repo, &bbox_file, "file", &metadata,
            )?;

            // Commit the schema
            repositories::commit(&repo, "Adding metadata to file column")?;

            // Add a new column to the data frame
            command::df::add_column(&bbox_path, "new_column:0:i32")?;

            // Stage the file
            repositories::add(&repo, &bbox_path)?;

            // Make sure the metadata persisted
            let schema =
                repositories::data_frames::schemas::get_staged(&repo, &bbox_file)?.unwrap();
            assert_eq!(schema.fields.len(), 7);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[0].metadata, Some(metadata));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_schemas_persist_schema_types_across_commits() -> Result<(), OxenError> {
        test::run_select_data_repo_test_no_commits_async("annotations", |repo| async move {
            // Find the bbox csv
            let bbox_path = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");

            // Make sure it is staged
            let bbox_file = util::fs::path_relative_to_dir(&bbox_path, &repo.path)?;
            let file_metadata = json!({
                "root": "images"
            });
            repositories::add(&repo, &bbox_path)?;
            repositories::data_frames::schemas::add_column_metadata(
                &repo,
                &bbox_file,
                "file",
                &file_metadata,
            )?;

            // Fetch staged
            let schema =
                repositories::data_frames::schemas::get_staged(&repo, &bbox_file)?.unwrap();
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            assert_eq!(schema.fields[0].metadata, Some(file_metadata.to_owned()));
            assert_eq!(schema.fields[1].name, "label");
            assert_eq!(schema.fields[1].dtype, "str");
            assert_eq!(schema.fields[2].name, "min_x");
            assert_eq!(schema.fields[2].dtype, "f64");
            assert_eq!(schema.fields[3].name, "min_y");
            assert_eq!(schema.fields[3].dtype, "f64");
            assert_eq!(schema.fields[4].name, "width");
            assert_eq!(schema.fields[4].dtype, "i64");
            assert_eq!(schema.fields[5].name, "height");
            assert_eq!(schema.fields[5].dtype, "i64");

            // Commit the schema
            repositories::commit(&repo, "Adding bounding box schema")?;

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

            let schema =
                repositories::data_frames::schemas::get_staged(&repo, &bbox_file)?.unwrap();
            assert_eq!(updated_schema.hash, schema.hash);
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            // this was added in the previous commit, so it should still be there
            assert_eq!(schema.fields[0].metadata, Some(file_metadata.to_owned()));
            assert_eq!(schema.fields[1].name, "label");
            assert_eq!(schema.fields[1].dtype, "str");

            assert_eq!(schema.fields[2].name, "min_x");
            assert_eq!(schema.fields[2].dtype, "f64");
            assert_eq!(schema.fields[2].metadata, Some(min_x_metadata.to_owned()));

            assert_eq!(schema.fields[3].name, "min_y");
            assert_eq!(schema.fields[3].dtype, "f64");

            assert_eq!(schema.fields[4].name, "width");
            assert_eq!(schema.fields[4].dtype, "i64");

            assert_eq!(schema.fields[5].name, "height");
            assert_eq!(schema.fields[5].dtype, "i64");

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
            let schema =
                repositories::data_frames::schemas::get_staged(&repo, &bbox_file)?.unwrap();
            assert_eq!(updated_schema.hash, schema.hash);
            assert_eq!(schema.fields.len(), 6);
            assert_eq!(schema.fields[0].name, "file");
            assert_eq!(schema.fields[0].dtype, "str");
            // this was added in the previous commit, so it should still be there
            assert_eq!(schema.fields[0].metadata, Some(file_metadata.to_owned()));

            assert_eq!(schema.fields[1].name, "label");
            assert_eq!(schema.fields[1].dtype, "str");

            assert_eq!(schema.fields[2].name, "min_x");
            assert_eq!(schema.fields[2].dtype, "f64");
            // this was added in the previous commit, so it should still be there
            assert_eq!(schema.fields[2].metadata, Some(min_x_metadata));

            assert_eq!(schema.fields[3].name, "min_y");
            assert_eq!(schema.fields[3].dtype, "f64");

            assert_eq!(schema.fields[4].name, "width");
            assert_eq!(schema.fields[4].dtype, "i64");
            assert_eq!(schema.fields[4].metadata, Some(width_metadata));

            assert_eq!(schema.fields[5].name, "height");
            assert_eq!(schema.fields[5].dtype, "i64");

            Ok(())
        })
        .await
    }
}
