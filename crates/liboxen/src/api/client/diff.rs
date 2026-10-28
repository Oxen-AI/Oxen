use std::path::Path;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::{DiffEntry, RemoteRepository};
use crate::view::CompareEntriesResponse;
use crate::view::compare::{CompareEntries, CompareEntryResponse};

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

    use super::{diff_entries, list_diff_entries};
    use crate::constants;
    use crate::error::OxenError;
    use crate::model::DiffEntry;
    use crate::model::EntryDataType;
    use crate::model::diff::generic_diff::GenericDiff;
    use crate::model::diff::generic_diff_summary::GenericDiffSummary;
    use crate::model::metadata::generic_metadata::GenericMetadata;
    use crate::opts::PushOpts;
    use crate::opts::RmOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    /// Asserts that `entry` is the directory `path` with `status`, and returns its diff summary's
    /// (added, removed, modified) file counts.
    fn dir_file_counts(entry: &DiffEntry, path: &Path, status: &str) -> (usize, usize, usize) {
        assert_eq!(Path::new(&entry.filename), path);
        assert_eq!(entry.status, status, "status of {path:?}");
        assert_eq!(entry.data_type, EntryDataType::Dir, "data type of {path:?}");
        match &entry.diff_summary {
            Some(GenericDiffSummary::DirDiffSummary(summary)) => {
                let counts = &summary.dir.file_counts;
                (counts.added, counts.removed, counts.modified)
            }
            summary => panic!("{path:?} has no directory summary: {summary:?}"),
        }
    }

    #[cfg_attr(windows, ignore = "oxen-server is not supported on Windows")]
    #[tokio::test]
    async fn test_diff_and_list_diff_entries() -> Result<(), OxenError> {
        test::run_empty_data_repo_test_no_commits_async(|mut repo| async move {
            // Every case starts from these two commits, each in its own directory, then commits
            // its changes in turn, so diffing a case's commit against the one before it shows
            // only that case's changes.
            let images_dir = repo.path.join("images");
            util::fs::create_dir_all(&images_dir)?;
            let cat_1 = images_dir.join("cat_1.jpg");
            util::fs::copy(test::test_img_file_with_name("cat_1.jpg"), &cat_1)?;
            repositories::add(&repo, &images_dir).await?;
            let csv_dirs = ["add_rows", "add_and_remove_rows", "remove_columns"];
            for dir in csv_dirs {
                util::fs::create_dir_all(repo.path.join(dir))?;
                let csv = repo.path.join(dir).join("llm_fine_tune.csv");
                util::fs::copy(test::test_csv_file_with_name("llm_fine_tune.csv"), &csv)?;
                repositories::add(&repo, &csv).await?;
            }
            repositories::commit(&repo, "Adding the single-file cases' starting files")?;

            // test.csv gains a first row, and train.csv gains three columns
            let test_csv = |first_row: usize| {
                let rows: String = (first_row..10)
                    .map(|i| format!("images/test/{i}.png,cat\n"))
                    .collect();
                format!("file,label\n{rows}")
            };
            let train_rows = |extra_columns: &str| -> String {
                (0..10)
                    .map(|i| format!("images/train/{i}.png,cat{extra_columns}\n"))
                    .collect()
            };
            util::fs::write_to_path(repo.path.join("test.csv"), test_csv(1))?;
            util::fs::write_to_path(
                repo.path.join("train.csv"),
                format!("file,label\n{}", train_rows("")),
            )?;

            for dir in ["add_to_dir", "rm_dir", "change_dir"] {
                test::populate_dir_with_txt_files(repo.path.join(dir), "cat", 3)?;
            }
            test::populate_dir_with_txt_files(repo.path.join("change_dir"), "dog", 4)?;
            for dir in ["mixed_subdirs", "rm_parent", "two_levels", "add_to_subdirs"] {
                test::populate_dir_with_txt_files(repo.path.join(dir).join("cats"), "cat", 3)?;
            }

            repositories::add(&repo, &repo.path).await?;
            repositories::commit(&repo, "Adding the directory cases' starting files")?;
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            let remote_repo = test::connect_remote_repo(&mut repo).await?;
            repositories::push(&repo).await?;

            let branch_name = "feat/diff-cases";
            repositories::branches::create_checkout(&repo, branch_name)?;

            util::fs::write_to_path(repo.path.join("test.csv"), test_csv(0))?;
            util::fs::write_to_path(
                repo.path.join("train.csv"),
                format!(
                    "file,label,contributor,dataset,is_cool\n{}",
                    train_rows(",,cifar10,TRUE")
                ),
            )?;
            repositories::add(&repo, repo.path.join("test.csv")).await?;
            repositories::add(&repo, repo.path.join("train.csv")).await?;
            let cifar = repositories::commit(&repo, "A row for test.csv, columns for train.csv")?;

            let add_to_dir = repo.path.join("add_to_dir");
            test::populate_dir_with_txt_files(&add_to_dir, "dog", 4)?;
            repositories::add(&repo, &add_to_dir).await?;
            let added_to_dir = repositories::commit(&repo, "Adding dogs beside the cats")?;

            let mixed_subdirs = repo.path.join("mixed_subdirs");
            test::populate_dir_with_txt_files(mixed_subdirs.join("dogs"), "dog", 4)?;
            test::modify_txt_file(mixed_subdirs.join("cats").join("cat_1.txt"), "a new cat")?;
            repositories::add(&repo, &mixed_subdirs).await?;
            util::fs::remove_file(mixed_subdirs.join("cats").join("cat_2.txt"))?;
            let cat_2 = Path::new("mixed_subdirs").join("cats").join("cat_2.txt");
            repositories::rm(&repo, &RmOpts::from_path(cat_2)).await?;
            let mixed = repositories::commit(&repo, "Add dogs, and remove and modify some cats")?;

            let rm_parent_cats = repo.path.join("rm_parent").join("cats");
            for i in 0..3 {
                util::fs::remove_file(rm_parent_cats.join(format!("cat_{i}.txt")))?;
            }
            // THIS IS THE CRUX of this case, do not remove rm_parent/cats, just remove rm_parent/
            let rm_opts = RmOpts {
                paths: vec![PathBuf::from("rm_parent")],
                recursive: true,
                ..Default::default()
            };
            repositories::rm(&repo, &rm_opts).await?;
            let rmed_parent = repositories::commit(&repo, "Removing the cats by their parent")?;

            let puppers_dir = repo.path.join("two_levels").join("dogs").join("puppers");
            test::populate_dir_with_txt_files(&puppers_dir, "dog", 3)?;
            repositories::add(&repo, &puppers_dir).await?;
            let two_levels = repositories::commit(&repo, "Adding dogs 🐕")?;

            let add_to_subdirs = repo.path.join("add_to_subdirs");
            test::populate_dir_with_txt_files(add_to_subdirs.join("dogs"), "dog", 3)?;
            let dwight_vince = add_to_subdirs.join("cats").join("dwight_vince.txt");
            test::write_txt_file_to_path(dwight_vince, "dwight and vince")?;
            repositories::add(&repo, &add_to_subdirs).await?;
            let added_to_subdirs = repositories::commit(&repo, "Adding dogs, and dwight to cats")?;

            for i in 0..3 {
                util::fs::remove_file(repo.path.join("rm_dir").join(format!("cat_{i}.txt")))?;
            }
            let rm_opts = RmOpts {
                paths: vec![PathBuf::from("rm_dir")],
                recursive: true,
                ..Default::default()
            };
            repositories::rm(&repo, &rm_opts).await?;
            let rmed_dir = repositories::commit(&repo, "Removing the cats")?;

            let change_dir = repo.path.join("change_dir");
            for i in 0..3 {
                test::modify_txt_file(change_dir.join(format!("cat_{i}.txt")), "a resized cat")?;
            }
            test::write_txt_file_to_path(change_dir.join("dwight_vince.txt"), "dwight and vince")?;
            repositories::add(&repo, &change_dir).await?;
            let rm_opts = RmOpts::from_path(Path::new("change_dir").join("dog_1.txt"));
            repositories::rm(&repo, &rm_opts).await?;
            let changed_dir = repositories::commit(&repo, "Modify cats, remove a dog, add dwight")?;

            // The single-file cases diff the branch's last commit, so they can name it by branch
            let cat_2 = images_dir.join("cat_2.jpg");
            util::fs::copy(test::test_img_file_with_name("cat_2.jpg"), cat_2)?;
            let dims = 96;
            util::image::resize_and_save(&cat_1, &cat_1, dims)?;
            let add_rows_csv = repo.path.join("add_rows").join("llm_fine_tune.csv");
            test::append_line_txt_file(
                &add_rows_csv,
                "answer the question,what is the color of the sky?,blue,trivia",
            )?;
            test::append_line_txt_file(
                &add_rows_csv,
                "answer the question,what is the color of the ocean?,blue-ish green sometimes,trivia",
            )?;
            test::write_txt_file_to_path(
                repo.path.join("add_and_remove_rows").join("llm_fine_tune.csv"),
                r"instruction,context,response,category
answer the question,what is the capital of france?,paris,geography
answer the question,who was the 44th president of the united states?,barack obama,politics
who won the game,,I don't know what game you are referring to,sports
who won the game?,The packers beat up on the bears,packers,sports
define the word,what does the word 'the' mean?,it is a stopword.,language
",
            )?;
            test::write_txt_file_to_path(
                repo.path.join("remove_columns").join("llm_fine_tune.csv"),
                r#"instruction,context,response
answer the question,what is the capital of france?,paris
answer the question,who was the 44th president of the united states?,barack obama
turn xml to json,<body><name>Bessie</name></body>,{"body": {"name": "bessie"}}
who won the game,,I don't know what game you are referring to
who won the game,broncos 23 v chargers 17,broncos
who won the game?,The packers beat up on the bears,packers
"#,
            )?;
            repositories::add(&repo, &images_dir).await?;
            for dir in csv_dirs {
                repositories::add(&repo, repo.path.join(dir)).await?;
            }
            let files_changed = repositories::commit(&repo, "Add and resize cats, edit csvs")?;

            let opts = PushOpts {
                remote: constants::DEFAULT_REMOTE_NAME.to_string(),
                branch: branch_name.to_string(),
                ..Default::default()
            };
            repositories::push::push_remote_branch(&repo, &opts).await?;

            // test.csv gained a row and train.csv gained three columns
            let compare =
                list_diff_entries(&remote_repo, &og_branch.name, &cifar.id, 0, 100)
                    .await?;
            assert_eq!(compare.entries.len(), 2);

            let test_csv = compare.entries.first().unwrap();
            assert_eq!(test_csv.filename, "test.csv");
            assert_eq!(test_csv.status, "modified");
            match test_csv.diff_summary.as_ref().unwrap() {
                GenericDiffSummary::TabularDiffWrapper(summary) => {
                    assert_eq!(summary.tabular.num_added_rows, 1);
                    assert_eq!(summary.tabular.num_removed_rows, 0);
                    assert_eq!(summary.tabular.num_added_cols, 0);
                    assert_eq!(summary.tabular.num_removed_cols, 0);
                    assert!(!summary.tabular.schema_has_changed);
                }
                _ => panic!("Wrong summary type"),
            }

            let train_csv = compare.entries.get(1).unwrap();
            assert_eq!(train_csv.filename, "train.csv");
            assert_eq!(train_csv.status, "modified");
            match train_csv.diff_summary.as_ref().unwrap() {
                GenericDiffSummary::TabularDiffWrapper(summary) => {
                    assert_eq!(summary.tabular.num_added_rows, 0);
                    assert_eq!(summary.tabular.num_removed_rows, 0);
                    assert_eq!(summary.tabular.num_added_cols, 3);
                    assert_eq!(summary.tabular.num_removed_cols, 0);
                    assert!(summary.tabular.schema_has_changed);
                }
                _ => panic!("Wrong summary type"),
            }

            // Added 4 dogs beside the cats
            let compare =
                list_diff_entries(&remote_repo, &cifar.id, &added_to_dir.id, 0, 100)
                    .await?;
            assert_eq!(compare.entries.len(), 5, "4 dogs and their dir");
            let counts = dir_file_counts(&compare.entries[0], Path::new("add_to_dir"), "modified");
            assert_eq!(counts, (4, 0, 0));

            // Added 4 dogs, modified 1 cat, removed 1 cat
            let compare =
                list_diff_entries(&remote_repo, &added_to_dir.id, &mixed.id, 0, 100)
                    .await?;
            assert_eq!(compare.entries.len(), 9, "6 files and three dirs");
            // TODO: Fix these, should be (4, 1, 1), (0, 1, 1), and (4, 0, 0)
            dir_file_counts(&compare.entries[0], Path::new("mixed_subdirs"), "modified");
            dir_file_counts(
                &compare.entries[1],
                &Path::new("mixed_subdirs").join("cats"),
                "modified",
            );
            dir_file_counts(
                &compare.entries[2],
                &Path::new("mixed_subdirs").join("dogs"),
                "added",
            );

            // Removed 3 cats by removing the directory above theirs
            let compare =
                list_diff_entries(&remote_repo, &mixed.id, &rmed_parent.id, 0, 100)
                    .await?;
            assert_eq!(compare.entries.len(), 5, "3 cats and two parent dirs");
            let counts = dir_file_counts(&compare.entries[0], Path::new("rm_parent"), "removed");
            assert_eq!(counts, (0, 3, 0));
            let counts = dir_file_counts(
                &compare.entries[1],
                &Path::new("rm_parent").join("cats"),
                "removed",
            );
            assert_eq!(counts, (0, 3, 0));

            // Added 3 dogs two directories below an existing one
            let compare =
                list_diff_entries(&remote_repo, &rmed_parent.id, &two_levels.id, 0, 100)
                    .await?;
            assert_eq!(compare.entries.len(), 6, "3 dogs and three parent dirs");
            let counts = dir_file_counts(&compare.entries[0], Path::new("two_levels"), "modified");
            assert_eq!(counts, (3, 0, 0));
            let dogs = Path::new("two_levels").join("dogs");
            assert_eq!(dir_file_counts(&compare.entries[1], &dogs, "added"), (3, 0, 0));
            let puppers = dogs.join("puppers");
            assert_eq!(dir_file_counts(&compare.entries[2], &puppers, "added"), (3, 0, 0));

            // Added 3 dogs in a new subdir and 1 file in an existing one
            let compare = list_diff_entries(
                &remote_repo,
                &two_levels.id,
                &added_to_subdirs.id,
                0,
                100,
            )
            .await?;
            assert_eq!(compare.entries.len(), 7, "4 files and three parent dirs");
            let parent = Path::new("add_to_subdirs");
            assert_eq!(dir_file_counts(&compare.entries[0], parent, "modified"), (4, 0, 0));
            let cats = Path::new("add_to_subdirs").join("cats");
            assert_eq!(dir_file_counts(&compare.entries[1], &cats, "modified"), (1, 0, 0));
            let dogs = Path::new("add_to_subdirs").join("dogs");
            assert_eq!(dir_file_counts(&compare.entries[2], &dogs, "added"), (3, 0, 0));

            // Removed 3 cats by removing their dir
            let compare =
                list_diff_entries(&remote_repo, &added_to_subdirs.id, &rmed_dir.id, 0, 100)
                    .await?;
            assert_eq!(compare.entries.len(), 4, "3 cats and their dir");
            let counts = dir_file_counts(&compare.entries[0], Path::new("rm_dir"), "removed");
            assert_eq!(counts, (0, 3, 0));

            // Removed 1 dog, added 1 file, modified 3 cats
            let compare =
                list_diff_entries(&remote_repo, &rmed_dir.id, &changed_dir.id, 0, 100)
                    .await?;
            assert_eq!(compare.entries.len(), 6, "5 files and their dir");
            let parent = Path::new("change_dir");
            let (added, _, _) = dir_file_counts(&compare.entries[0], parent, "modified");
            // TODO: Fix this, should be modified 3 and removed 1
            assert_eq!(added, 1);

            // An added image
            let compare = diff_entries(
                &remote_repo,
                &changed_dir.id,
                branch_name,
                &PathBuf::from("images").join("cat_2.jpg"),
            )
            .await?;
            assert!(compare.base_entry.is_none());
            let entry = compare.head_entry.as_ref().unwrap();
            assert_eq!(entry.filename, "cat_2.jpg");
            assert_eq!(
                PathBuf::from(&entry.resource.as_ref().unwrap().path),
                PathBuf::from("images").join("cat_2.jpg")
            );
            assert_eq!(compare.status, "added");
            assert_eq!(entry.data_type, EntryDataType::Image);

            // A resized image, named by branch and by commit id
            for head in [branch_name, files_changed.id.as_str()] {
                let compare = diff_entries(
                    &remote_repo,
                    &changed_dir.id,
                    head,
                    &PathBuf::from("images").join("cat_1.jpg"),
                )
                .await?;
                assert!(compare.base_entry.is_some());
                let entry = compare.head_entry.as_ref().unwrap();
                assert_eq!(entry.filename, "cat_1.jpg");
                assert_eq!(
                    PathBuf::from(&entry.resource.as_ref().unwrap().path),
                    PathBuf::from("images").join("cat_1.jpg")
                );
                assert_eq!(compare.status, "modified", "cat_1.jpg diffed against {head}");
                assert_eq!(entry.data_type, EntryDataType::Image);
                match entry.metadata.as_ref().unwrap() {
                    GenericMetadata::MetadataImage(metadata) => {
                        assert_eq!(metadata.image.width, dims);
                        assert_eq!(metadata.image.height, dims);
                    }
                    _ => panic!("Wrong summary type"),
                }
            }

            // A csv with two rows appended
            let csv_path = PathBuf::from("add_rows").join("llm_fine_tune.csv");
            let compare =
                diff_entries(&remote_repo, &changed_dir.id, branch_name, &csv_path)
                    .await?;
            assert_eq!(compare.status, "modified");
            let head_entry = compare.head_entry.as_ref().unwrap();
            assert_eq!(head_entry.filename, "llm_fine_tune.csv");
            assert_eq!(head_entry.resource.as_ref().unwrap().path, csv_path);
            assert_eq!(head_entry.data_type, EntryDataType::Tabular);
            match head_entry.metadata.as_ref().unwrap() {
                GenericMetadata::MetadataTabular(metadata) => {
                    assert_eq!(metadata.tabular.height, 8);
                    assert_eq!(metadata.tabular.width, 4);
                }
                _ => panic!("Wrong summary type"),
            }
            let base_entry = compare.base_entry.as_ref().unwrap();
            assert_eq!(base_entry.filename, "llm_fine_tune.csv");
            assert_eq!(base_entry.resource.as_ref().unwrap().path, csv_path);
            assert_eq!(base_entry.data_type, EntryDataType::Tabular);
            match base_entry.metadata.as_ref().unwrap() {
                GenericMetadata::MetadataTabular(metadata) => {
                    assert_eq!(metadata.tabular.height, 6);
                    assert_eq!(metadata.tabular.width, 4);
                }
                _ => panic!("Wrong summary type"),
            }
            match compare.diff_summary.as_ref().unwrap() {
                GenericDiffSummary::TabularDiffWrapper(diff_summary) => {
                    assert_eq!(diff_summary.tabular.num_added_rows, 2);
                    assert_eq!(diff_summary.tabular.num_removed_rows, 0);
                    assert_eq!(diff_summary.tabular.num_added_cols, 0);
                    assert_eq!(diff_summary.tabular.num_removed_cols, 0);
                }
                _ => panic!("Wrong summary type"),
            }
            match compare.diff.as_ref().unwrap() {
                GenericDiff::TabularDiff(diff) => {
                    assert!(diff.tabular.added_cols.as_ref().is_none());
                    assert!(diff.tabular.removed_cols.as_ref().is_none());
                    assert_eq!(diff.tabular.added_rows.as_ref().unwrap().view_size.height, 2);
                    assert_eq!(diff.tabular.removed_rows.as_ref().unwrap().view_size.height, 0);
                }
                _ => panic!("Wrong summary type"),
            }

            // A csv with one row added and two removed
            let csv_path = PathBuf::from("add_and_remove_rows").join("llm_fine_tune.csv");
            let compare =
                diff_entries(&remote_repo, &changed_dir.id, branch_name, &csv_path)
                    .await?;
            assert_eq!(compare.status, "modified");
            let head_entry = compare.head_entry.as_ref().unwrap();
            assert_eq!(head_entry.filename, "llm_fine_tune.csv");
            assert_eq!(head_entry.resource.as_ref().unwrap().path, csv_path);
            assert_eq!(head_entry.data_type, EntryDataType::Tabular);
            match head_entry.metadata.as_ref().unwrap() {
                GenericMetadata::MetadataTabular(metadata) => {
                    assert_eq!(metadata.tabular.height, 5);
                    assert_eq!(metadata.tabular.width, 4);
                }
                _ => panic!("Wrong summary type"),
            }
            let base_entry = compare.base_entry.as_ref().unwrap();
            assert_eq!(base_entry.filename, "llm_fine_tune.csv");
            assert_eq!(base_entry.resource.as_ref().unwrap().path, csv_path);
            assert_eq!(base_entry.data_type, EntryDataType::Tabular);
            match base_entry.metadata.as_ref().unwrap() {
                GenericMetadata::MetadataTabular(metadata) => {
                    assert_eq!(metadata.tabular.height, 6);
                    assert_eq!(metadata.tabular.width, 4);
                }
                _ => panic!("Wrong summary type"),
            }
            match compare.diff_summary.as_ref().unwrap() {
                GenericDiffSummary::TabularDiffWrapper(diff_summary) => {
                    assert_eq!(diff_summary.tabular.num_added_rows, 1);
                    assert_eq!(diff_summary.tabular.num_removed_rows, 2);
                    assert_eq!(diff_summary.tabular.num_added_cols, 0);
                    assert_eq!(diff_summary.tabular.num_removed_cols, 0);
                }
                _ => panic!("Wrong summary type"),
            }
            match compare.diff.as_ref().unwrap() {
                GenericDiff::TabularDiff(diff) => {
                    assert_eq!(diff.tabular.added_rows.as_ref().unwrap().view_size.height, 1);
                    assert_eq!(diff.tabular.removed_rows.as_ref().unwrap().view_size.height, 2);
                    assert!(diff.tabular.added_cols.as_ref().is_none());
                    assert!(diff.tabular.removed_cols.as_ref().is_none());
                }
                _ => panic!("Wrong summary type"),
            }

            // A csv with a column removed
            let csv_path = PathBuf::from("remove_columns").join("llm_fine_tune.csv");
            let compare =
                diff_entries(&remote_repo, &changed_dir.id, branch_name, &csv_path)
                    .await?;
            assert_eq!(compare.status, "modified");
            let head_entry = compare.head_entry.as_ref().unwrap();
            assert_eq!(head_entry.filename, "llm_fine_tune.csv");
            assert_eq!(head_entry.resource.as_ref().unwrap().path, csv_path);
            assert_eq!(head_entry.data_type, EntryDataType::Tabular);
            match head_entry.metadata.as_ref().unwrap() {
                GenericMetadata::MetadataTabular(metadata) => {
                    assert_eq!(metadata.tabular.height, 6);
                    assert_eq!(metadata.tabular.width, 3);
                }
                _ => panic!("Wrong summary type"),
            }
            let base_entry = compare.base_entry.as_ref().unwrap();
            assert_eq!(base_entry.filename, "llm_fine_tune.csv");
            assert_eq!(base_entry.resource.as_ref().unwrap().path, csv_path);
            assert_eq!(base_entry.data_type, EntryDataType::Tabular);
            match base_entry.metadata.as_ref().unwrap() {
                GenericMetadata::MetadataTabular(metadata) => {
                    assert_eq!(metadata.tabular.height, 6);
                    assert_eq!(metadata.tabular.width, 4);
                }
                _ => panic!("Wrong summary type"),
            }
            match compare.diff_summary.as_ref().unwrap() {
                GenericDiffSummary::TabularDiffWrapper(diff_summary) => {
                    assert_eq!(diff_summary.tabular.num_added_rows, 0);
                    assert_eq!(diff_summary.tabular.num_removed_rows, 0);
                    assert_eq!(diff_summary.tabular.num_added_cols, 0);
                    assert_eq!(diff_summary.tabular.num_removed_cols, 1);
                }
                _ => panic!("Wrong summary type"),
            }
            match compare.diff.as_ref().unwrap() {
                GenericDiff::TabularDiff(diff) => {
                    let removed_cols = diff.tabular.removed_cols.as_ref().unwrap();
                    assert_eq!(removed_cols.view_size.height, 6);
                    assert_eq!(removed_cols.view_size.width, 1);
                }
                _ => panic!("Wrong summary type"),
            }

            Ok(())
        })
        .await
    }
}
