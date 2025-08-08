use std::path::{Path, PathBuf};

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::merge_conflict::MergeConflict;
use crate::model::Commit;
use crate::model::{Branch, LocalRepository};

#[derive(Debug)]
pub struct MergeCommits {
    pub lca: Commit,
    pub base: Commit,
    pub merge: Commit,
}

impl MergeCommits {
    pub fn is_fast_forward_merge(&self) -> bool {
        self.lca.id == self.base.id
    }
}

pub fn list_conflicts(repo: &LocalRepository) -> Result<Vec<MergeConflict>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => {
            let conflicts = core::v_latest::merge::list_conflicts(repo)?;
            Ok(conflicts
                .iter()
                .map(|conflict| conflict.to_merge_conflict())
                .collect())
        }
    }
}

pub async fn has_conflicts(
    repo: &LocalRepository,
    base_branch: &Branch,
    merge_branch: &Branch,
) -> Result<bool, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::merge::has_conflicts(repo, base_branch, merge_branch).await,
    }
}

pub fn mark_conflict_as_resolved(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("mark_conflict_as_resolved not supported for oxen v0.10"),
        _ => core::v_latest::merge::mark_conflict_as_resolved(repo, path),
    }
}

pub async fn can_merge_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    merge_commit: &Commit,
) -> Result<bool, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::merge::can_merge_commits(repo, base_commit, merge_commit).await,
    }
}

pub async fn list_conflicts_between_branches(
    repo: &LocalRepository,
    base_branch: &Branch,
    merge_branch: &Branch,
) -> Result<Vec<PathBuf>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => {
            core::v_latest::merge::list_conflicts_between_branches(repo, base_branch, merge_branch)
                .await
        }
    }
}

pub fn list_commits_between_branches(
    repo: &LocalRepository,
    base_branch: &Branch,
    head_branch: &Branch,
) -> Result<Vec<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::merge::list_commits_between_branches(repo, base_branch, head_branch),
    }
}

pub fn list_commits_between_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    head_commit: &Commit,
) -> Result<Vec<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::merge::list_commits_between_commits(repo, base_commit, head_commit),
    }
}

pub async fn list_conflicts_between_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    merge_commit: &Commit,
) -> Result<Vec<PathBuf>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => {
            core::v_latest::merge::list_conflicts_between_commits(repo, base_commit, merge_commit)
                .await
        }
    }
}

pub async fn merge_into_base(
    repo: &LocalRepository,
    merge_branch: &Branch,
    base_branch: &Branch,
) -> Result<Option<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::merge::merge_into_base(repo, merge_branch, base_branch).await,
    }
}

pub async fn merge(
    repo: &LocalRepository,
    branch_name: impl AsRef<str>,
) -> Result<Option<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::merge::merge(repo, branch_name).await,
    }
}

pub async fn merge_commit_into_base(
    repo: &LocalRepository,
    merge_commit: &Commit,
    base_commit: &Commit,
) -> Result<Option<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::merge::merge_commit_into_base(repo, merge_commit, base_commit).await,
    }
}

pub async fn merge_commit_into_base_on_branch(
    repo: &LocalRepository,
    merge_commit: &Commit,
    base_commit: &Commit,
    branch: &Branch,
) -> Result<Option<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => {
            core::v_latest::merge::merge_commit_into_base_on_branch(
                repo,
                merge_commit,
                base_commit,
                branch,
            )
            .await
        }
    }
}

pub fn has_file(repo: &LocalRepository, path: &Path) -> Result<bool, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::merge::has_file(repo, path),
    }
}

pub fn remove_conflict_path(repo: &LocalRepository, path: &Path) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::merge::remove_conflict_path(repo, path),
    }
}

pub fn find_merge_commits<S: AsRef<str>>(
    repo: &LocalRepository,
    branch_name: S,
) -> Result<MergeCommits, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::merge::find_merge_commits(repo, branch_name),
    }
}

pub fn lowest_common_ancestor_from_commits(
    repo: &LocalRepository,
    base_commit: &Commit,
    merge_commit: &Commit,
) -> Result<Commit, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::merge::lowest_common_ancestor_from_commits(
            repo,
            base_commit,
            merge_commit,
        ),
    }
}

#[cfg(test)]
mod tests {

    use std::path::Path;

    use crate::core::df::tabular;
    use crate::core::merge::node_merge_conflict_reader::NodeMergeConflictReader;

    use crate::error::OxenError;
    use crate::model::{Commit, LocalRepository};
    use crate::opts::DFOpts;
    use crate::repositories;
    use crate::test;
    use crate::util;

    async fn populate_threeway_merge_repo(
        repo: &LocalRepository,
        merge_branch_name: &str,
    ) -> Result<Commit, OxenError> {
        // Need to have main branch get ahead of branch so that you can traverse to directory to it, but they
        // have a common ancestor
        // Ex) We want to merge E into D to create F
        // A - C - D - F
        //    \      /
        //     B - E

        let a_branch = repositories::branches::current_branch(repo)?.unwrap();
        let a_path = repo.path.join("a.txt");
        util::fs::write_to_path(&a_path, "a")?;
        repositories::add(repo, a_path).await?;
        // Return the lowest common ancestor for the tests
        let lca = repositories::commit(repo, "Committing a.txt file")?;

        // Make changes on B
        repositories::branches::create_checkout(repo, merge_branch_name)?;
        let b_path = repo.path.join("b.txt");
        util::fs::write_to_path(&b_path, "b")?;
        repositories::add(repo, b_path).await?;
        repositories::commit(repo, "Committing b.txt file")?;

        // Checkout A again to make another change
        repositories::checkout(repo, &a_branch.name).await?;
        let c_path = repo.path.join("c.txt");
        util::fs::write_to_path(&c_path, "c")?;
        repositories::add(repo, c_path).await?;
        repositories::commit(repo, "Committing c.txt file")?;

        let d_path = repo.path.join("d.txt");
        util::fs::write_to_path(&d_path, "d")?;
        repositories::add(repo, d_path).await?;
        repositories::commit(repo, "Committing d.txt file")?;

        // Checkout merge branch (B) to make another change
        repositories::checkout(repo, merge_branch_name).await?;

        let e_path = repo.path.join("e.txt");
        util::fs::write_to_path(&e_path, "e")?;
        repositories::add(repo, e_path).await?;
        repositories::commit(repo, "Committing e.txt file")?;

        // Checkout the OG branch again so that we can merge into it
        repositories::checkout(repo, &a_branch.name).await?;

        Ok(lca)
    }

    #[tokio::test]
    async fn test_merge_one_commit_add_fast_forward() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // Write and commit hello file to main branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;
            repositories::add(&repo, hello_file).await?;
            repositories::commit(&repo, "Adding hello file")?;

            // Branch to add world
            let branch_name = "add-world";
            repositories::branches::create_checkout(&repo, branch_name)?;

            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World")?;
            repositories::add(&repo, &world_file).await?;
            repositories::commit(&repo, "Adding world file")?;
            // Fetch the branch again to get the latest commit
            let merge_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Checkout and merge additions
            let og_branch = repositories::checkout(&repo, &og_branch.name)
                .await?
                .unwrap();

            // Make sure world file doesn't exist until we merge it in
            assert!(!world_file.exists());

            let commit = repositories::merge::merge_into_base(&repo, &merge_branch, &og_branch)
                .await?
                .unwrap();

            // Now that we've merged in, world file should exist
            assert!(world_file.exists());

            // Check that HEAD has updated to the merge commit
            let head_commit = repositories::commits::head_commit(&repo)?;
            assert_eq!(head_commit.id, commit.id);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_one_commit_remove_fast_forward() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // Write and add hello file
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;
            repositories::add(&repo, hello_file).await?;

            // Write and add world file
            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World")?;
            repositories::add(&repo, &world_file).await?;

            // Commit two files
            repositories::commit(&repo, "Adding hello & world files")?;

            // Branch to remove world
            let branch_name = "remove-world";
            let merge_branch = repositories::branches::create_checkout(&repo, branch_name)?;

            // Remove the file
            let world_file = repo.path.join("world.txt");
            util::fs::remove_file(&world_file)?;

            // Commit the removal
            repositories::add(&repo, &world_file).await?;
            repositories::commit(&repo, "Removing world file")?;

            // Checkout and merge additions
            repositories::checkout(&repo, &og_branch.name).await?;

            // Make sure world file exists until we merge the removal in
            assert!(world_file.exists(), "World file should exist before merge");

            let merge_result = repositories::merge::merge(&repo, &merge_branch.name).await?;

            merge_result.unwrap();

            // Now that we've merged in, world file should not exist
            assert!(
                !world_file.exists(),
                "World file should not exist after merge"
            );

            Ok(())
        })
        .await
    }
    #[tokio::test]
    async fn test_merge_one_commit_modified_fast_forward() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // Write and add hello file
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;
            repositories::add(&repo, hello_file).await?;

            // Write and add world file
            let world_file = repo.path.join("world.txt");
            let og_contents = "World";
            util::fs::write_to_path(&world_file, og_contents)?;
            repositories::add(&repo, &world_file).await?;

            // Commit two files
            repositories::commit(&repo, "Adding hello & world files")?;

            // Branch to remove world
            let branch_name = "modify-world";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Modify the file
            let new_contents = "Around the world";
            let world_file = test::modify_txt_file(world_file, new_contents)?;

            // Commit the removal
            repositories::add(&repo, &world_file).await?;
            repositories::commit(&repo, "Modifying world file")?;

            // Checkout and merge additions
            repositories::checkout(&repo, &og_branch.name).await?;

            // Make sure world file exists in it's original form
            let contents = util::fs::read_from_path(&world_file)?;
            assert_eq!(contents, og_contents);

            repositories::merge::merge(&repo, branch_name)
                .await?
                .unwrap();

            // Now that we've merged in, world file should be new content
            assert!(world_file.exists(), "World file should exist after merge");
            let contents = util::fs::read_from_path(&world_file)?;
            assert_eq!(contents, new_contents);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_is_three_way_merge() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let merge_branch_name = "B"; // see populate function
            populate_threeway_merge_repo(&repo, merge_branch_name).await?;

            // Make sure the merger can detect the three way merge
            let merge_commits = repositories::merge::find_merge_commits(&repo, merge_branch_name)?;
            let is_fast_forward = merge_commits.is_fast_forward_merge();
            assert!(!is_fast_forward);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_get_lowest_common_ancestor() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let merge_branch_name = "B"; // see populate function
            let lca = populate_threeway_merge_repo(&repo, merge_branch_name).await?;

            // Make sure the merger can detect the three way merge
            let guess =
                repositories::merge::lowest_common_ancestor_from_commits(&repo, &lca, &lca)?;
            assert_eq!(lca.id, guess.id);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_no_conflict_three_way_merge() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let merge_branch_name = "B";
            // this will checkout main again so we can try to merge

            populate_threeway_merge_repo(&repo, merge_branch_name).await?;

            {
                // Make sure the merger can detect the three way merge
                let merge_commit = repositories::merge::merge(&repo, merge_branch_name)
                    .await?
                    .unwrap();

                // Two way merge should have two parent IDs so we know where the merge came from
                assert_eq!(merge_commit.parent_ids.len(), 2);

                // There should be 5 files: [a.txt, b.txt, c.txt, d.txt e.txt]
                let file_prefixes = ["a", "b", "c", "d", "e"];
                for prefix in file_prefixes.iter() {
                    let filename = format!("{prefix}.txt");
                    let filepath = repo.path.join(filename);
                    println!(
                        "test_merge_no_conflict_three_way_merge checking file exists {filepath:?}"
                    );
                    assert!(filepath.exists());
                }
            }

            let commit_history = repositories::commits::list(&repo)?;

            // We should have the merge commit + the branch commits here
            assert_eq!(7, commit_history.len());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_conflict_three_way_merge() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // This test has a conflict where user on the main line, and user on the branch, both modify a.txt

            // Ex) We want to merge E into D to create F
            // A - C - D - F
            //    \      /
            //     B - E

            let a_branch = repositories::branches::current_branch(&repo)?.unwrap();
            let a_path = repo.path.join("a.txt");
            util::fs::write_to_path(&a_path, "a")?;
            repositories::add(&repo, &a_path).await?;
            // Return the lowest common ancestor for the tests
            repositories::commit(&repo, "Committing a.txt file")?;

            // Make changes on B
            let merge_branch_name = "B";
            repositories::branches::create_checkout(&repo, merge_branch_name)?;

            // Add a text new text file
            let b_path = repo.path.join("b.txt");
            util::fs::write_to_path(&b_path, "b")?;
            repositories::add(&repo, &b_path).await?;

            // Modify the text file a.txt
            test::modify_txt_file(&a_path, "a modified from branch")?;
            repositories::add(&repo, &a_path).await?;

            // Commit changes
            repositories::commit(&repo, "Committing b.txt file")?;

            // Checkout main branch again to make another change
            repositories::checkout(&repo, &a_branch.name).await?;

            // Add new file c.txt on main branch
            let c_path = repo.path.join("c.txt");
            util::fs::write_to_path(&c_path, "c")?;
            repositories::add(&repo, &c_path).await?;

            // Modify a.txt from main branch
            test::modify_txt_file(&a_path, "a modified from main line")?;
            repositories::add(&repo, &a_path).await?;

            // Commit changes to main branch
            repositories::commit(&repo, "Committing c.txt file")?;

            // Commit some more changes to main branch
            let d_path = repo.path.join("d.txt");
            util::fs::write_to_path(&d_path, "d")?;
            repositories::add(&repo, &d_path).await?;
            repositories::commit(&repo, "Committing d.txt file")?;

            // Checkout merge branch (B) to make another change
            repositories::checkout(&repo, merge_branch_name).await?;

            // Add another branch
            let e_path = repo.path.join("e.txt");
            util::fs::write_to_path(&e_path, "e")?;
            repositories::add(&repo, &e_path).await?;
            repositories::commit(&repo, "Committing e.txt file")?;

            // Checkout the OG branch again so that we can merge into it
            repositories::checkout(&repo, &a_branch.name).await?;

            repositories::merge::merge(&repo, merge_branch_name).await?;

            let conflict_reader = NodeMergeConflictReader::new(&repo)?;
            let has_conflicts = conflict_reader.has_conflicts()?;
            let conflicts = conflict_reader.list_conflicts()?;

            assert!(has_conflicts);
            assert_eq!(conflicts.len(), 1);

            let local_a_path = util::fs::path_relative_to_dir(&a_path, &repo.path)?;
            assert_eq!(conflicts[0].base_entry.1, local_a_path);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_conflict_three_way_merge_post_merge_branch() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // This case for a three way merge was failing, if one branch gets fast forwarded, then the next
            // should have a conflict from the LCA

            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();
            let labels_path = repo.path.join("labels.txt");
            util::fs::write_to_path(&labels_path, "cat\ndog")?;
            repositories::add(&repo, &labels_path).await?;
            // Return the lowest common ancestor for the tests
            repositories::commit(&repo, "Add initial labels.txt file with cat and dog")?;

            // Add a fish label to the file on a branch
            let fish_branch_name = "add-fish-label";
            repositories::branches::create_checkout(&repo, fish_branch_name)?;
            let labels_path = test::modify_txt_file(labels_path, "cat\ndog\nfish")?;
            repositories::add(&repo, &labels_path).await?;
            repositories::commit(&repo, "Adding fish to labels.txt file")?;

            // Checkout main, and branch from it to another branch to add a human label
            repositories::checkout(&repo, &og_branch.name).await?;
            let human_branch_name = "add-human-label";
            repositories::branches::create_checkout(&repo, human_branch_name)?;
            let labels_path = test::modify_txt_file(labels_path, "cat\ndog\nhuman")?;
            repositories::add(&repo, labels_path).await?;
            repositories::commit(&repo, "Adding human to labels.txt file")?;

            // Checkout main again
            repositories::checkout(&repo, &og_branch.name).await?;

            // Merge in a scope so that it closes the db
            repositories::merge::merge(&repo, fish_branch_name).await?;

            // Checkout main again, merge again
            repositories::checkout(&repo, &og_branch.name).await?;
            repositories::merge::merge(&repo, human_branch_name).await?;

            let conflict_reader = NodeMergeConflictReader::new(&repo)?;
            let has_conflicts = conflict_reader.has_conflicts()?;
            let conflicts = conflict_reader.list_conflicts()?;

            assert!(has_conflicts);
            assert_eq!(conflicts.len(), 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merger_has_merge_conflicts_without_merging() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // This case for a three way merge was failing, if one branch gets fast forwarded, then the next
            // should have a conflict from the LCA

            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();
            let labels_path = repo.path.join("labels.txt");
            util::fs::write_to_path(&labels_path, "cat\ndog")?;
            repositories::add(&repo, &labels_path).await?;
            // Return the lowest common ancestor for the tests
            repositories::commit(&repo, "Add initial labels.txt file with cat and dog")?;

            // Add a fish label to the file on a branch
            let fish_branch_name = "add-fish-label";
            repositories::branches::create_checkout(&repo, fish_branch_name)?;
            let labels_path = test::modify_txt_file(labels_path, "cat\ndog\nfish")?;
            repositories::add(&repo, &labels_path).await?;
            repositories::commit(&repo, "Adding fish to labels.txt file")?;

            // Checkout main, and branch from it to another branch to add a human label
            repositories::checkout(&repo, &og_branch.name).await?;
            let human_branch_name = "add-human-label";
            repositories::branches::create_checkout(&repo, human_branch_name)?;
            let labels_path = test::modify_txt_file(labels_path, "cat\ndog\nhuman")?;
            repositories::add(&repo, labels_path).await?;
            repositories::commit(&repo, "Adding human to labels.txt file")?;

            // Checkout main again
            repositories::checkout(&repo, &og_branch.name).await?;

            // Merge the fish branch in, and then the human branch should have conflicts

            let result = repositories::merge::merge(&repo, fish_branch_name).await?;
            assert!(result.is_some());

            // But now there should be conflicts when trying to merge in the human branch
            let base_branch = repositories::branches::get_by_name(&repo, &og_branch.name)?.unwrap();
            let merge_branch =
                repositories::branches::get_by_name(&repo, human_branch_name)?.unwrap();

            // Check if there are conflicts
            let has_conflicts =
                repositories::merge::has_conflicts(&repo, &base_branch, &merge_branch).await?;
            assert!(has_conflicts);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_list_merge_conflicts_without_merging() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // This case for a three way merge was failing, if one branch gets fast forwarded, then the next
            // should have a conflict from the LCA

            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();
            let labels_path = repo.path.join("labels.txt");
            util::fs::write_to_path(&labels_path, "cat\ndog")?;
            repositories::add(&repo, &labels_path).await?;
            // Return the lowest common ancestor for the tests
            repositories::commit(&repo, "Add initial labels.txt file with cat and dog")?;

            // Add a fish label to the file on a branch
            let fish_branch_name = "add-fish-label";
            repositories::branches::create_checkout(&repo, fish_branch_name)?;
            let labels_path = test::modify_txt_file(labels_path, "cat\ndog\nfish")?;
            repositories::add(&repo, &labels_path).await?;
            repositories::commit(&repo, "Adding fish to labels.txt file")?;

            // Checkout main, and branch from it to another branch to add a human label
            repositories::checkout(&repo, &og_branch.name).await?;
            let human_branch_name = "add-human-label";
            repositories::branches::create_checkout(&repo, human_branch_name)?;
            let labels_path = test::modify_txt_file(labels_path, "cat\ndog\nhuman")?;
            repositories::add(&repo, labels_path).await?;
            let human_commit = repositories::commit(&repo, "Adding human to labels.txt file")?;

            // Checkout main again
            repositories::checkout(&repo, &og_branch.name).await?;

            // Merge the fish branch in, and then the human branch should have conflicts
            let result_commit = repositories::merge::merge(&repo, fish_branch_name).await?;

            assert!(result_commit.is_some());

            // There should be one file that is in conflict
            let base_commit = result_commit.unwrap();
            let conflicts = repositories::merge::list_conflicts_between_commits(
                &repo,
                &base_commit,
                &human_commit,
            )
            .await?;
            assert_eq!(conflicts.len(), 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_merge_dataframe_conflict_both_added_rows_checkout_theirs(
    ) -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Add a more rows on this branch
            let branch_name = "ox-add-rows";
            repositories::branches::create_checkout(&repo, branch_name)?;

            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(&bbox_filename);
            let bbox_file =
                test::append_line_txt_file(bbox_file, "train/cat_3.jpg,cat,41.0,31.5,410,427")?;
            let their_branch_contents = util::fs::read_from_path(&bbox_file)?;

            repositories::add(&repo, &bbox_file).await?;
            repositories::commit(&repo, "Adding new annotation as an Ox on a branch.")?;

            // Add a more rows on the main branch
            repositories::checkout(&repo, og_branch.name).await?;

            let bbox_file =
                test::append_line_txt_file(bbox_file, "train/dog_4.jpg,dog,52.0,62.5,256,429")?;

            repositories::add(&repo, &bbox_file).await?;
            repositories::commit(&repo, "Adding new annotation on main branch")?;

            // Try to merge in the changes
            repositories::merge::merge(&repo, branch_name).await?;

            // We should have a conflict....
            println!("status plz");
            let status = repositories::status(&repo)?;
            assert_eq!(status.merge_conflicts.len(), 1);

            println!("checkout theirs plz");

            // Run repositories::checkout::checkout_theirs() and make sure their changes get kept
            repositories::checkout::checkout_theirs(&repo, &bbox_filename).await?;

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
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(&bbox_filename);

            // Add a more rows on this branch
            let branch_name = "ox-add-rows";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Add in a line in this branch
            let row_from_branch = "train/cat_3.jpg,cat,41.0,31.5,410,427";
            let bbox_file = test::append_line_txt_file(bbox_file, row_from_branch)?;

            // Add the changes
            repositories::add(&repo, &bbox_file).await?;
            repositories::commit(&repo, "Adding new annotation as an Ox on a branch.")?;

            // Add a more rows on the main branch
            repositories::checkout(&repo, og_branch.name).await?;

            let row_from_main = "train/dog_4.jpg,dog,52.0,62.5,256,429";
            let bbox_file = test::append_line_txt_file(bbox_file, row_from_main)?;

            repositories::add(&repo, &bbox_file).await?;
            repositories::commit(&repo, "Adding new annotation on main branch")?;

            // Try to merge in the changes
            repositories::merge::merge(&repo, branch_name).await?;

            // We should have a conflict....
            let status = repositories::status(&repo)?;
            assert_eq!(status.merge_conflicts.len(), 1);

            // Run repositories::checkout::checkout_theirs() and make sure their changes get kept
            repositories::checkout::checkout_combine(&repo, bbox_filename).await?;
            let df = tabular::read_df(&bbox_file, DFOpts::empty()).await?;

            // This doesn't guarantee order, but let's make sure we have 7 annotations now
            assert_eq!(df.height(), 8);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_command_merge_dataframe_conflict_error_added_col() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            let bbox_filename = Path::new("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = repo.path.join(&bbox_filename);

            // Add a more columns on this branch
            let branch_name = "ox-add-column";
            repositories::branches::create_checkout(&repo, branch_name)?;

            // Add in a column in this branch
            let mut opts = DFOpts::empty();
            opts.add_col = Some(String::from("random_col:unknown:str"));
            let mut df = tabular::read_df(&bbox_file, opts).await?;
            println!("WRITE DF IN BRANCH {df:?}");
            tabular::write_df(&mut df, &bbox_file)?;

            // Add the changes
            repositories::add(&repo, &bbox_file).await?;
            repositories::commit(&repo, "Adding new column as an Ox on a branch.")?;

            // Add a more rows on the main branch
            repositories::checkout(&repo, og_branch.name).await?;

            let row_from_main = "train/dog_4.jpg,dog,52.0,62.5,256,429";
            let bbox_file = test::append_line_txt_file(bbox_file, row_from_main)?;

            repositories::add(&repo, bbox_file).await?;
            repositories::commit(&repo, "Adding new row on main branch")?;

            // Try to merge in the changes
            repositories::merge::merge(&repo, branch_name).await?;

            // We should have a conflict....
            let status = repositories::status(&repo)?;
            assert_eq!(status.merge_conflicts.len(), 1);

            // Run repositories::checkout::checkout_theirs() and make sure we cannot
            let result = repositories::checkout::checkout_combine(&repo, bbox_filename).await;
            println!("{result:?}");
            assert!(result.is_err());

            Ok(())
        })
        .await
    }

    // Test fast forward merge on pull
    /*
    oxen init
    oxen add .
    oxen commit -m "add data"
    oxen push
    oxen clone repo_b
    # update data frame file
    oxen add .
    oxen commit -m "update data"
    oxen push
    oxen pull repo_a (should be fast forward)
    */
    #[tokio::test]
    async fn test_command_merge_fast_forward_pull() -> Result<(), OxenError> {
        test::run_training_data_fully_sync_remote(|_local_repo, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();
            test::run_empty_dir_test_async(|repo_dir_a| async move {
                let repo_dir_a = repo_dir_a.join("repo_a");
                let cloned_repo_a =
                    repositories::clone_url(&remote_repo.remote.url, &repo_dir_a).await?;

                test::run_empty_dir_test_async(|repo_dir_b| async move {
                    let repo_dir_b = repo_dir_b.join("repo_b");
                    let cloned_repo_b =
                        repositories::clone_url(&remote_repo.remote.url, &repo_dir_b).await?;

                    // Add a more rows on this branch
                    let bbox_filename = Path::new("annotations")
                        .join("train")
                        .join("bounding_box.csv");
                    let bbox_file = cloned_repo_a.path.join(&bbox_filename);
                    let og_df = tabular::read_df(&bbox_file, DFOpts::empty()).await?;
                    let bbox_file = test::append_line_txt_file(
                        bbox_file,
                        "train/cat_3.jpg,cat,41.0,31.5,410,427",
                    )?;
                    repositories::add(&cloned_repo_a, &bbox_file).await?;
                    repositories::commit(&cloned_repo_a, "Adding new annotation as an Ox.")?;

                    repositories::push(&cloned_repo_a).await?;

                    // Pull in the changes
                    repositories::pull(&cloned_repo_b).await?;

                    // Check that we have the new data
                    let bbox_file = cloned_repo_b.path.join(&bbox_filename);
                    let df = tabular::read_df(&bbox_file, DFOpts::empty()).await?;
                    assert_eq!(df.height(), og_df.height() + 1);

                    // make the changes again from repo_a
                    // Add a more rows on this branch
                    let bbox_filename = Path::new("annotations")
                        .join("train")
                        .join("bounding_box.csv");
                    let bbox_file = cloned_repo_a.path.join(&bbox_filename);
                    let bbox_file = test::append_line_txt_file(
                        bbox_file,
                        "train/cat_13.jpg,cat,41.0,31.5,410,427",
                    )?;
                    repositories::add(&cloned_repo_a, &bbox_file).await?;
                    repositories::commit(
                        &cloned_repo_a,
                        "Adding another new annotation as an Ox.",
                    )?;

                    repositories::push(&cloned_repo_a).await?;

                    // Pull in the changes
                    repositories::pull(&cloned_repo_b).await?;

                    // Check that we have the new data
                    let bbox_file = cloned_repo_b.path.join(&bbox_filename);
                    let df = tabular::read_df(&bbox_file, DFOpts::empty()).await?;
                    assert_eq!(df.height(), og_df.height() + 2);

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

    #[tokio::test]
    async fn test_merge_no_commit_needed() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // 1. Commit something in main branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();
            let labels_path = repo.path.join("labels.txt");
            util::fs::write_to_path(&labels_path, "cat\ndog")?;
            repositories::add(&repo, &labels_path).await?;
            repositories::commit(&repo, "Add initial labels.txt file with cat and dog")?;

            // 2. Create a new branch
            let new_branch_name = "new_branch";
            let new_branch = repositories::branches::create_checkout(&repo, new_branch_name)?;

            // 3. Commit something in new branch
            let labels_path = test::modify_txt_file(labels_path, "cat\ndog\nfish")?;
            repositories::add(&repo, &labels_path).await?;
            repositories::commit(&repo, "Adding fish to labels.txt file")?;

            // 4. merge main onto new branch
            let merge_result =
                repositories::merge::merge_into_base(&repo, &og_branch, &new_branch).await;

            // 5. There should be no commit
            assert_eq!(
                merge_result.unwrap_err().to_string(),
                OxenError::basic_str("No changes to commit").to_string()
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_diverged_branches_then_merge_again() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // 1. Commit something in main branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();
            let file1_path = repo.path.join("file1.txt");
            util::fs::write_to_path(&file1_path, "Initial content for file1")?;
            repositories::add(&repo, &file1_path).await?;
            let _ = repositories::commit(&repo, "Commit file1 to main")?;

            // 2. Create and checkout a new branch
            let new_branch_name = "feature-branch";
            repositories::branches::create_checkout(&repo, new_branch_name)?;

            // 3. Commit something in new branch
            let file2_path = repo.path.join("file2.txt");
            util::fs::write_to_path(&file2_path, "Content for file2 in feature branch")?;
            repositories::add(&repo, &file2_path).await?;
            let feature_commit1 = repositories::commit(&repo, "Commit file2 to feature-branch")?;

            // 4. Checkout main branch
            repositories::checkout(&repo, &og_branch.name).await?;

            // 5. Commit something in main branch to make it diverge
            let file3_path = repo.path.join("file3.txt");
            util::fs::write_to_path(&file3_path, "Content for file3 in main branch")?;
            repositories::add(&repo, &file3_path).await?;
            let main_commit2 = repositories::commit(&repo, "Commit file3 to main, diverging")?;

            // 6. Merge new branch onto main branch
            // There should be a new merge commit
            let merge_result1 = repositories::merge::merge(&repo, new_branch_name).await?;
            assert!(
                merge_result1.is_some(),
                "First merge should create a merge commit"
            );
            let merge_commit1 = merge_result1.unwrap();
            assert_ne!(
                merge_commit1.id, main_commit2.id,
                "Merge commit ID should be new"
            );
            assert_ne!(
                merge_commit1.id, feature_commit1.id,
                "Merge commit ID should be new"
            );
            assert_eq!(
                merge_commit1.parent_ids.len(),
                2,
                "Merge commit should have two parents"
            );

            // 7. Merge new branch onto main branch again.
            // There should be no new merge commit
            let merge_result2 = repositories::merge::merge(&repo, new_branch_name).await;
            assert_eq!(
                merge_result2.unwrap_err().to_string(),
                OxenError::basic_str("No changes to commit").to_string(),
                "Second merge attempt should not create a new commit as it's already merged"
            );

            // Verify HEAD is still the first merge commit
            let head_commit_after_second_merge = repositories::commits::head_commit(&repo)?;
            assert_eq!(
                head_commit_after_second_merge.id, merge_commit1.id,
                "HEAD should remain at the first merge commit"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_immediately_after_checkout() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // Need to have main branch get ahead of branch so that you can traverse to directory to it, but they
            // have a common ancestor
            // 1. Commit something in main branch
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();
            let labels_path = repo.path.join("labels.txt");
            util::fs::write_to_path(&labels_path, "cat\ndog")?;
            repositories::add(&repo, &labels_path).await?;
            repositories::commit(&repo, "Add initial labels.txt file with cat and dog")?;

            // 2. Create a new branch
            let new_branch_name = "new_branch";
            let _new_branch = repositories::branches::create_checkout(&repo, new_branch_name)?;

            // 4. merge main onto new branch
            let commit = repositories::merge::merge(&repo, og_branch.name).await?;

            // 5. There should be no commit
            assert!(commit.is_none());

            Ok(())
        })
        .await
    }
}
