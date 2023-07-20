use liboxen::api;
use liboxen::command;
use liboxen::core::df::tabular;
use liboxen::error::OxenError;
use liboxen::opts::DFOpts;
use liboxen::test;
use liboxen::util;

use std::path::Path;

#[tokio::test]
async fn test_command_merge_dataframe_conflict_both_added_rows_checkout_theirs(
) -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|repo| async move {
        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        // Add a more rows on this branch
        let branch_name = "ox-add-rows";
        api::local::branches::create_checkout(&repo, branch_name)?;

        let bbox_filename = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_file = repo.path.join(&bbox_filename);
        let bbox_file =
            test::append_line_txt_file(bbox_file, "train/cat_3.jpg,cat,41.0,31.5,410,427")?;
        let their_branch_contents = util::fs::read_from_path(&bbox_file)?;
        let their_df = tabular::read_df(&bbox_file, DFOpts::empty())?;
        println!("their df {their_df}");

        command::add(&repo, &bbox_file)?;
        command::commit(&repo, "Adding new annotation as an Ox on a branch.")?;

        // Add a more rows on the main branch
        command::checkout(&repo, og_branch.name).await?;

        let bbox_file =
            test::append_line_txt_file(bbox_file, "train/dog_4.jpg,dog,52.0,62.5,256,429")?;

        command::add(&repo, &bbox_file)?;
        command::commit(&repo, "Adding new annotation on main branch")?;

        // Try to merge in the changes
        command::merge(&repo, branch_name)?;

        // We should have a conflict....
        let status = command::status(&repo)?;
        assert_eq!(status.merge_conflicts.len(), 1);

        // Run command::checkout_theirs() and make sure their changes get kept
        command::checkout_theirs(&repo, &bbox_filename)?;
        let restored_df = tabular::read_df(&bbox_file, DFOpts::empty())?;
        println!("restored df {restored_df}");

        let file_contents = util::fs::read_from_path(&bbox_file)?;

        assert_eq!(file_contents, their_branch_contents);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_command_merge_dataframe_conflict_both_added_rows_combine_uniq(
) -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|repo| async move {
        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        let bbox_filename = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_file = repo.path.join(&bbox_filename);

        // Add a more rows on this branch
        let branch_name = "ox-add-rows";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Add in a line in this branch
        let row_from_branch = "train/cat_3.jpg,cat,41.0,31.5,410,427";
        let bbox_file = test::append_line_txt_file(bbox_file, row_from_branch)?;

        // Add the changes
        command::add(&repo, &bbox_file)?;
        command::commit(&repo, "Adding new annotation as an Ox on a branch.")?;

        // Add a more rows on the main branch
        command::checkout(&repo, og_branch.name).await?;

        let row_from_main = "train/dog_4.jpg,dog,52.0,62.5,256,429";
        let bbox_file = test::append_line_txt_file(bbox_file, row_from_main)?;

        command::add(&repo, &bbox_file)?;
        command::commit(&repo, "Adding new annotation on main branch")?;

        // Try to merge in the changes
        command::merge(&repo, branch_name)?;

        // We should have a conflict....
        let status = command::status(&repo)?;
        assert_eq!(status.merge_conflicts.len(), 1);

        // Run command::checkout_theirs() and make sure their changes get kept
        command::checkout_combine(&repo, bbox_filename)?;
        let df = tabular::read_df(&bbox_file, DFOpts::empty())?;

        // This doesn't guarantee order, but let's make sure we have 7 annotations now
        assert_eq!(df.height(), 8);

        Ok(())
    })
    .await
}

#[tokio::test]
async fn test_command_merge_dataframe_conflict_error_added_col() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed_async(|repo| async move {
        let og_branch = api::local::branches::current_branch(&repo)?.unwrap();

        let bbox_filename = Path::new("annotations")
            .join("train")
            .join("bounding_box.csv");
        let bbox_file = repo.path.join(&bbox_filename);

        // Add a more columns on this branch
        let branch_name = "ox-add-column";
        api::local::branches::create_checkout(&repo, branch_name)?;

        // Add in a column in this branch
        let mut opts = DFOpts::empty();
        opts.add_col = Some(String::from("random_col:unknown:str"));
        let mut df = tabular::read_df(&bbox_file, opts)?;
        println!("WRITE DF IN BRANCH {df:?}");
        tabular::write_df(&mut df, &bbox_file)?;

        // Add the changes
        command::add(&repo, &bbox_file)?;
        command::commit(&repo, "Adding new column as an Ox on a branch.")?;

        // Add a more rows on the main branch
        command::checkout(&repo, og_branch.name).await?;

        let row_from_main = "train/dog_4.jpg,dog,52.0,62.5,256,429";
        let bbox_file = test::append_line_txt_file(bbox_file, row_from_main)?;

        command::add(&repo, bbox_file)?;
        command::commit(&repo, "Adding new row on main branch")?;

        // Try to merge in the changes
        command::merge(&repo, branch_name)?;

        // We should have a conflict....
        let status = command::status(&repo)?;
        assert_eq!(status.merge_conflicts.len(), 1);

        // Run command::checkout_theirs() and make sure we cannot
        let result = command::checkout_combine(&repo, bbox_filename);
        println!("{result:?}");
        assert!(result.is_err());

        Ok(())
    })
    .await
}