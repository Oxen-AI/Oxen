use std::path::{Path, PathBuf};

use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::Commit;
use crate::model::merge_conflict::MergeConflict;
use crate::model::{Branch, LocalRepository};

#[derive(Debug)]
pub struct MergeCommits {
    pub lca: Option<Commit>,
    pub base: Commit,
    pub merge: Commit,
}

impl MergeCommits {
    pub fn commit_message(&self) -> String {
        format!("Merge commit {} into {}", self.merge.id, self.base.id)
    }

    pub fn is_fast_forward_merge(&self) -> bool {
        self.lca.as_ref().is_some_and(|lca| lca.id == self.base.id)
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
) -> Result<Commit, OxenError> {
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

/// Server-safe merge of two commits. Does not touch the working directory or
/// HEAD — the caller is responsible for updating the branch ref.
///
/// Use this variant from server code paths that must not mutate on-disk files.
/// For the client-side equivalent that updates the checkout and HEAD, see
/// [`merge_commit_into_base`].
pub async fn merge_commit_into_base_server_safe(
    repo: &LocalRepository,
    merge_commit: &Commit,
    base_commit: &Commit,
) -> Result<Option<Commit>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => {
            core::v_latest::merge::merge_commit_into_base_server_safe(
                repo,
                merge_commit,
                base_commit,
            )
            .await
        }
    }
}

/// Client-side merge of two commits. Updates files on disk and advances HEAD.
///
/// For the server-side equivalent that never touches the working directory,
/// see [`merge_commit_into_base_server_safe`].
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

/// Abandon an interrupted or in-conflict client-side merge. Clears `MERGE_IN_PROGRESS`,
/// `MERGE_HEAD`, `ORIG_HEAD`, and the merge-conflicts DB, and restores the working tree to HEAD.
pub async fn abort_merge(repo: &LocalRepository) -> Result<(), OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => panic!("v0.10.0 no longer supported"),
        _ => core::v_latest::merge::abort_merge(repo).await,
    }
}

pub async fn merge_commit_into_base_on_branch(
    repo: &LocalRepository,
    merge_commit: &Commit,
    base_commit: &Commit,
    branch: &Branch,
) -> Result<Commit, OxenError> {
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
) -> Result<Option<Commit>, OxenError> {
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

    use crate::core::v_latest::merge_marker;
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
            let _og_branch = repositories::checkout(&repo, &og_branch.name)
                .await?
                .unwrap();

            // Make sure world file doesn't exist until we merge it in
            assert!(!world_file.exists());

            let commit = repositories::merge::merge(&repo, &merge_branch.name)
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
                repositories::merge::lowest_common_ancestor_from_commits(&repo, &lca, &lca)?
                    .unwrap();
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
            let base_branch = repositories::branches::get_by_name(&repo, &og_branch.name)?;
            let merge_branch = repositories::branches::get_by_name(&repo, human_branch_name)?;

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
    async fn test_command_merge_dataframe_conflict_both_added_rows_checkout_theirs()
    -> Result<(), OxenError> {
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
            let status = repositories::status(&repo).await?;
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
    async fn test_command_merge_dataframe_conflict_both_added_rows_combine_uniq()
    -> Result<(), OxenError> {
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
            let status = repositories::status(&repo).await?;
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
            let status = repositories::status(&repo).await?;
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
            repositories::branches::create_checkout(&repo, new_branch_name)?;

            // 3. Commit something in new branch
            let labels_path = test::modify_txt_file(labels_path, "cat\ndog\nfish")?;
            repositories::add(&repo, &labels_path).await?;
            repositories::commit(&repo, "Adding fish to labels.txt file")?;

            // 4. merge main onto new branch (re-fetch branches to get current commit IDs)
            let og_branch = repositories::branches::get_by_name(&repo, &og_branch.name)?;
            let new_branch = repositories::branches::get_by_name(&repo, new_branch_name)?;
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

    // Regression test: files modified only on the merge branch whose parent directory
    // hash is shared between the LCA and base should not trigger "your local changes
    // would be overwritten."
    #[tokio::test]
    async fn test_merge_three_way_file_modified_only_on_merge_branch() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // Setup: create initial files in a subdirectory
            let models_dir = repo.path.join("models");
            util::fs::create_dir_all(&models_dir)?;

            let a_path = models_dir.join("a.toml");
            let b_path = models_dir.join("b.toml");
            util::fs::write_to_path(&a_path, "version = 1")?;
            util::fs::write_to_path(&b_path, "version = 1")?;
            repositories::add(&repo, &models_dir).await?;
            repositories::commit(&repo, "Add models/a.toml and models/b.toml")?;

            let main_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Create branch B and modify models/a.toml
            let merge_branch_name = "B";
            repositories::branches::create_checkout(&repo, merge_branch_name)?;
            util::fs::write_to_path(&a_path, "version = 2")?;
            repositories::add(&repo, &a_path).await?;
            repositories::commit(&repo, "Update models/a.toml on branch B")?;

            // Back on main, make a diverging commit (different file, outside models/)
            repositories::checkout(&repo, &main_branch.name).await?;
            let other_path = repo.path.join("other.txt");
            util::fs::write_to_path(&other_path, "hello")?;
            repositories::add(&repo, &other_path).await?;
            repositories::commit(&repo, "Add other.txt on main")?;

            // Merge branch B into main — this should succeed without conflict
            let merge_commit = repositories::merge::merge(&repo, merge_branch_name).await?;
            assert!(
                merge_commit.is_some(),
                "Merge should succeed without conflict"
            );

            // Verify the merged file has the updated content from branch B
            let content = util::fs::read_from_path(&a_path)?;
            assert_eq!(content, "version = 2");

            // Verify the other files are still intact
            assert!(b_path.exists());
            assert!(other_path.exists());

            Ok(())
        })
        .await
    }

    // --- Client-side merge tests for delete edge cases ---

    #[tokio::test]
    async fn test_merge_client_base_deletes_merge_unchanged() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch_name = repositories::branches::current_branch(&repo)?.unwrap().name;

            // LCA: create two files
            let file_a = repo.path.join("a.txt");
            let file_b = repo.path.join("b.txt");
            util::fs::write_to_path(&file_a, "a")?;
            util::fs::write_to_path(&file_b, "b")?;
            repositories::add(&repo, &file_a).await?;
            repositories::add(&repo, &file_b).await?;
            repositories::commit(&repo, "Adding a.txt and b.txt")?;

            // Feature branch makes an unrelated change (doesn't touch a.txt)
            repositories::branches::create_checkout(&repo, "feature")?;
            util::fs::write_to_path(&file_b, "b modified on feature")?;
            repositories::add(&repo, &file_b).await?;
            repositories::commit(&repo, "Modifying b.txt on feature")?;

            // Main branch deletes a.txt
            repositories::checkout(&repo, &base_branch_name).await?;
            util::fs::remove_file(&file_a)?;
            repositories::add(&repo, &file_a).await?;
            repositories::commit(&repo, "Deleting a.txt on main")?;

            // Merge feature into main — base's deletion of a.txt should be preserved
            let merge_commit = repositories::merge::merge(&repo, "feature").await?;
            assert!(merge_commit.is_some());

            // a.txt should still be deleted in working dir (base's deletion wins)
            assert!(
                !file_a.exists(),
                "a.txt should remain deleted after merge — base's deletion wins"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_client_modify_delete_conflict() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch_name = repositories::branches::current_branch(&repo)?.unwrap().name;

            // LCA: create a file
            let file = repo.path.join("shared.txt");
            util::fs::write_to_path(&file, "original")?;
            repositories::add(&repo, &file).await?;
            repositories::commit(&repo, "Adding shared.txt")?;

            // Feature branch modifies the file
            repositories::branches::create_checkout(&repo, "feature")?;
            util::fs::write_to_path(&file, "modified on feature")?;
            repositories::add(&repo, &file).await?;
            repositories::commit(&repo, "Modifying shared.txt on feature")?;

            // Main branch deletes the same file
            repositories::checkout(&repo, &base_branch_name).await?;
            util::fs::remove_file(&file)?;
            repositories::add(&repo, &file).await?;
            repositories::commit(&repo, "Deleting shared.txt on main")?;

            // Merge should detect conflict (delete on base + modify on merge)
            let merge_result = repositories::merge::merge(&repo, "feature").await?;
            assert!(
                merge_result.is_none(),
                "Delete on base + modify on merge should be a conflict"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_client_delete_modify_conflict() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch_name = repositories::branches::current_branch(&repo)?.unwrap().name;

            // LCA: create a file
            let file = repo.path.join("shared.txt");
            util::fs::write_to_path(&file, "original")?;
            repositories::add(&repo, &file).await?;
            repositories::commit(&repo, "Adding shared.txt")?;

            // Feature branch deletes the file
            repositories::branches::create_checkout(&repo, "feature")?;
            util::fs::remove_file(&file)?;
            repositories::add(&repo, &file).await?;
            repositories::commit(&repo, "Deleting shared.txt on feature")?;

            // Main branch modifies the same file
            repositories::checkout(&repo, &base_branch_name).await?;
            util::fs::write_to_path(&file, "modified on main")?;
            repositories::add(&repo, &file).await?;
            repositories::commit(&repo, "Modifying shared.txt on main")?;

            // Merge should detect conflict (modify on base + delete on merge)
            let merge_result = repositories::merge::merge(&repo, "feature").await?;
            assert!(
                merge_result.is_none(),
                "Modify on base + delete on merge should be a conflict"
            );

            Ok(())
        })
        .await
    }

    // --- Server-side merge tests (merge_into_base) ---

    #[tokio::test]
    async fn test_merge_into_base_fast_forward() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Add a file and commit on main
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Adding hello file")?;

            // Create a feature branch and add a commit
            repositories::branches::create_checkout(&repo, "feature")?;
            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World")?;
            repositories::add(&repo, &world_file).await?;
            repositories::commit(&repo, "Adding world file")?;

            // Go back to main so HEAD is on the base branch
            repositories::checkout(&repo, &base_branch.name).await?;

            // Re-fetch branches to get current commit IDs
            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch.name)?;

            // Server-side merge: should succeed and update the branch ref
            let merge_commit =
                repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await?;

            assert_eq!(merge_commit.id, merge_branch.commit_id);

            // The base branch ref should now point to the merge commit
            let updated_branch = repositories::branches::get_by_name(&repo, &base_branch.name)?;
            assert_eq!(updated_branch.commit_id, merge_commit.id);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_into_base_three_way_no_conflicts() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch_name = repositories::branches::current_branch(&repo)?.unwrap().name;

            // Commit a file on main (LCA)
            let a_file = repo.path.join("a.txt");
            util::fs::write_to_path(&a_file, "a")?;
            repositories::add(&repo, &a_file).await?;
            repositories::commit(&repo, "Adding a.txt")?;

            // Create feature branch and add a different file
            repositories::branches::create_checkout(&repo, "feature")?;
            let b_file = repo.path.join("b.txt");
            util::fs::write_to_path(&b_file, "b")?;
            repositories::add(&repo, &b_file).await?;
            repositories::commit(&repo, "Adding b.txt on feature")?;

            // Go back to main and add another different file (diverge)
            repositories::checkout(&repo, &base_branch_name).await?;
            let c_file = repo.path.join("c.txt");
            util::fs::write_to_path(&c_file, "c")?;
            repositories::add(&repo, &c_file).await?;
            repositories::commit(&repo, "Adding c.txt on main")?;

            // Re-fetch branches
            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch_name)?;

            // Server-side three-way merge should succeed
            let merge_commit =
                repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await?;

            // Merge commit should have two parents
            assert_eq!(merge_commit.parent_ids.len(), 2);
            assert!(merge_commit.parent_ids.contains(&base_branch.commit_id));
            assert!(merge_commit.parent_ids.contains(&merge_branch.commit_id));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_into_base_three_way_with_conflicts() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch_name = repositories::branches::current_branch(&repo)?.unwrap().name;

            // Commit a file on main (LCA)
            let labels_file = repo.path.join("labels.txt");
            util::fs::write_to_path(&labels_file, "cat\ndog")?;
            repositories::add(&repo, &labels_file).await?;
            repositories::commit(&repo, "Adding labels.txt")?;

            // Create feature branch and modify the same file
            repositories::branches::create_checkout(&repo, "feature")?;
            util::fs::write_to_path(&labels_file, "cat\ndog\nfish")?;
            repositories::add(&repo, &labels_file).await?;
            repositories::commit(&repo, "Adding fish on feature")?;

            // Go back to main and modify the same file differently (conflict)
            repositories::checkout(&repo, &base_branch_name).await?;
            util::fs::write_to_path(&labels_file, "cat\ndog\nbird")?;
            repositories::add(&repo, &labels_file).await?;
            repositories::commit(&repo, "Adding bird on main")?;

            // Re-fetch branches
            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch_name)?;

            // Server-side merge should return UpstreamMergeConflict error
            let result =
                repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await;
            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(
                matches!(err, OxenError::UpstreamMergeConflict(_)),
                "Expected UpstreamMergeConflict, got: {err:?}"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_into_base_does_not_modify_working_dir() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Commit a file on main
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Adding hello file")?;

            // Create feature branch and add a new file
            repositories::branches::create_checkout(&repo, "feature")?;
            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World")?;
            repositories::add(&repo, &world_file).await?;
            repositories::commit(&repo, "Adding world file")?;

            // Go back to main — world.txt should not exist in working dir
            repositories::checkout(&repo, &base_branch.name).await?;
            assert!(!world_file.exists());

            // Re-fetch branches
            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch.name)?;

            // Server-side merge
            repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await?;

            // world.txt should STILL not exist — server merge doesn't touch working dir
            assert!(
                !world_file.exists(),
                "Server-side merge should not create files in working directory"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_into_base_ff_with_modification() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch = repositories::branches::current_branch(&repo)?.unwrap();

            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Adding hello file")?;

            // Feature branch modifies the file
            repositories::branches::create_checkout(&repo, "feature")?;
            util::fs::write_to_path(&hello_file, "Hello, modified")?;
            repositories::add(&repo, &hello_file).await?;
            repositories::commit(&repo, "Modifying hello file")?;

            repositories::checkout(&repo, &base_branch.name).await?;
            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch.name)?;

            let merge_commit =
                repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await?;

            assert_eq!(merge_commit.id, merge_branch.commit_id);

            // Working dir should still have old content
            let content = util::fs::read_from_path(&hello_file)?;
            assert_eq!(content, "Hello");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_into_base_ff_with_deletion() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch = repositories::branches::current_branch(&repo)?.unwrap();

            let hello_file = repo.path.join("hello.txt");
            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&hello_file, "Hello")?;
            util::fs::write_to_path(&world_file, "World")?;
            repositories::add(&repo, &hello_file).await?;
            repositories::add(&repo, &world_file).await?;
            repositories::commit(&repo, "Adding files")?;

            // Feature branch deletes world.txt
            repositories::branches::create_checkout(&repo, "feature")?;
            util::fs::remove_file(&world_file)?;
            repositories::add(&repo, &world_file).await?;
            repositories::commit(&repo, "Removing world file")?;

            repositories::checkout(&repo, &base_branch.name).await?;
            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch.name)?;

            let merge_commit =
                repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await?;

            assert_eq!(merge_commit.id, merge_branch.commit_id);

            // Working dir should still have the file (server merge doesn't touch it)
            assert!(world_file.exists());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_into_base_ff_with_subdirectories() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch = repositories::branches::current_branch(&repo)?.unwrap();

            let models_dir = repo.path.join("models").join("kling");
            util::fs::create_dir_all(&models_dir)?;
            let file_a = models_dir.join("a.toml");
            util::fs::write_to_path(&file_a, "version = 1")?;
            repositories::add(&repo, &file_a).await?;
            repositories::commit(&repo, "Adding models/kling/a.toml")?;

            // Feature branch adds a file in a subdirectory
            repositories::branches::create_checkout(&repo, "feature")?;
            let file_b = models_dir.join("b.toml");
            util::fs::write_to_path(&file_b, "version = 1")?;
            repositories::add(&repo, &file_b).await?;
            repositories::commit(&repo, "Adding models/kling/b.toml")?;

            repositories::checkout(&repo, &base_branch.name).await?;
            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch.name)?;

            let merge_commit =
                repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await?;

            assert_eq!(merge_commit.id, merge_branch.commit_id);
            let updated_branch = repositories::branches::get_by_name(&repo, &base_branch.name)?;
            assert_eq!(updated_branch.commit_id, merge_commit.id);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_into_base_three_way_with_subdirectories() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch_name = repositories::branches::current_branch(&repo)?.unwrap().name;

            // Create initial files in subdirectories
            let models_dir = repo.path.join("models");
            util::fs::create_dir_all(&models_dir)?;
            let file_a = models_dir.join("a.toml");
            util::fs::write_to_path(&file_a, "version = 1")?;
            repositories::add(&repo, &file_a).await?;
            repositories::commit(&repo, "Adding models/a.toml")?;

            // Feature branch adds a new file in the same subdirectory
            repositories::branches::create_checkout(&repo, "feature")?;
            let file_b = models_dir.join("b.toml");
            util::fs::write_to_path(&file_b, "version = 1")?;
            repositories::add(&repo, &file_b).await?;
            repositories::commit(&repo, "Adding models/b.toml on feature")?;

            // Main branch adds a file in a different subdirectory (diverge, no conflict)
            repositories::checkout(&repo, &base_branch_name).await?;
            let scripts_dir = repo.path.join("scripts");
            util::fs::create_dir_all(&scripts_dir)?;
            let file_c = scripts_dir.join("deploy.sh");
            util::fs::write_to_path(&file_c, "#!/bin/bash")?;
            repositories::add(&repo, &file_c).await?;
            repositories::commit(&repo, "Adding scripts/deploy.sh on main")?;

            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch_name)?;

            let merge_commit =
                repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await?;

            assert_eq!(merge_commit.parent_ids.len(), 2);

            // Working dir should not have the feature branch file
            assert!(
                !file_b.exists(),
                "Server merge should not create models/b.toml on disk"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_into_base_three_way_modification_no_conflict() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch_name = repositories::branches::current_branch(&repo)?.unwrap().name;

            // Create two files
            let file_a = repo.path.join("a.txt");
            let file_b = repo.path.join("b.txt");
            util::fs::write_to_path(&file_a, "a version 1")?;
            util::fs::write_to_path(&file_b, "b version 1")?;
            repositories::add(&repo, &file_a).await?;
            repositories::add(&repo, &file_b).await?;
            repositories::commit(&repo, "Adding a.txt and b.txt")?;

            // Feature branch modifies a.txt
            repositories::branches::create_checkout(&repo, "feature")?;
            util::fs::write_to_path(&file_a, "a version 2")?;
            repositories::add(&repo, &file_a).await?;
            repositories::commit(&repo, "Modifying a.txt on feature")?;

            // Main branch modifies b.txt (different file, no conflict)
            repositories::checkout(&repo, &base_branch_name).await?;
            util::fs::write_to_path(&file_b, "b version 2")?;
            repositories::add(&repo, &file_b).await?;
            repositories::commit(&repo, "Modifying b.txt on main")?;

            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch_name)?;

            let merge_commit =
                repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await?;

            assert_eq!(merge_commit.parent_ids.len(), 2);

            // a.txt should still have the base version in working dir
            let content = util::fs::read_from_path(&file_a)?;
            assert_eq!(content, "a version 1");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_into_base_three_way_deletion_no_conflict() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch_name = repositories::branches::current_branch(&repo)?.unwrap().name;

            // Create two files
            let file_a = repo.path.join("a.txt");
            let file_b = repo.path.join("b.txt");
            util::fs::write_to_path(&file_a, "a")?;
            util::fs::write_to_path(&file_b, "b")?;
            repositories::add(&repo, &file_a).await?;
            repositories::add(&repo, &file_b).await?;
            repositories::commit(&repo, "Adding a.txt and b.txt")?;

            // Feature branch deletes a.txt
            repositories::branches::create_checkout(&repo, "feature")?;
            util::fs::remove_file(&file_a)?;
            repositories::add(&repo, &file_a).await?;
            repositories::commit(&repo, "Removing a.txt on feature")?;

            // Main branch modifies b.txt (different file, no conflict)
            repositories::checkout(&repo, &base_branch_name).await?;
            util::fs::write_to_path(&file_b, "b modified")?;
            repositories::add(&repo, &file_b).await?;
            repositories::commit(&repo, "Modifying b.txt on main")?;

            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch_name)?;

            let merge_commit =
                repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await?;

            assert_eq!(merge_commit.parent_ids.len(), 2);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_into_base_three_way_conflict_in_subdirectory() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch_name = repositories::branches::current_branch(&repo)?.unwrap().name;

            let models_dir = repo.path.join("models");
            util::fs::create_dir_all(&models_dir)?;
            let config = models_dir.join("config.toml");
            util::fs::write_to_path(&config, "version = 1")?;
            repositories::add(&repo, &config).await?;
            repositories::commit(&repo, "Adding models/config.toml")?;

            // Feature branch modifies the config
            repositories::branches::create_checkout(&repo, "feature")?;
            util::fs::write_to_path(&config, "version = 2")?;
            repositories::add(&repo, &config).await?;
            repositories::commit(&repo, "Updating config on feature")?;

            // Main branch also modifies the same config (conflict)
            repositories::checkout(&repo, &base_branch_name).await?;
            util::fs::write_to_path(&config, "version = 3")?;
            repositories::add(&repo, &config).await?;
            repositories::commit(&repo, "Updating config on main")?;

            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch_name)?;

            let result =
                repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await;
            assert!(matches!(
                result.unwrap_err(),
                OxenError::UpstreamMergeConflict(_)
            ));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_into_base_three_way_both_add_same_path() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch_name = repositories::branches::current_branch(&repo)?.unwrap().name;

            // LCA: no new.txt yet
            let placeholder = repo.path.join("placeholder.txt");
            util::fs::write_to_path(&placeholder, "anchor")?;
            repositories::add(&repo, &placeholder).await?;
            repositories::commit(&repo, "Anchor commit")?;

            // Feature branch adds new.txt
            repositories::branches::create_checkout(&repo, "feature")?;
            let new_file = repo.path.join("new.txt");
            util::fs::write_to_path(&new_file, "from feature")?;
            repositories::add(&repo, &new_file).await?;
            repositories::commit(&repo, "Adding new.txt on feature")?;

            // Main branch also adds new.txt with different content
            repositories::checkout(&repo, &base_branch_name).await?;
            util::fs::write_to_path(&new_file, "from main")?;
            repositories::add(&repo, &new_file).await?;
            repositories::commit(&repo, "Adding new.txt on main")?;

            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch_name)?;

            let result =
                repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await;
            assert!(matches!(
                result.unwrap_err(),
                OxenError::UpstreamMergeConflict(_)
            ));

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_into_base_three_way_modify_delete_conflict() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch_name = repositories::branches::current_branch(&repo)?.unwrap().name;

            // LCA: create a file
            let file = repo.path.join("shared.txt");
            util::fs::write_to_path(&file, "original")?;
            repositories::add(&repo, &file).await?;
            repositories::commit(&repo, "Adding shared.txt")?;

            // Feature branch deletes the file
            repositories::branches::create_checkout(&repo, "feature")?;
            util::fs::remove_file(&file)?;
            repositories::add(&repo, &file).await?;
            repositories::commit(&repo, "Deleting shared.txt on feature")?;

            // Main branch modifies the same file
            repositories::checkout(&repo, &base_branch_name).await?;
            util::fs::write_to_path(&file, "modified on main")?;
            repositories::add(&repo, &file).await?;
            repositories::commit(&repo, "Modifying shared.txt on main")?;

            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch_name)?;

            let result =
                repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await;
            assert!(
                matches!(result.unwrap_err(), OxenError::UpstreamMergeConflict(_)),
                "Modify on base + delete on merge should be a conflict"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_into_base_three_way_base_deletes_merge_unchanged() -> Result<(), OxenError>
    {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch_name = repositories::branches::current_branch(&repo)?.unwrap().name;

            // LCA: create two files
            let file_a = repo.path.join("a.txt");
            let file_b = repo.path.join("b.txt");
            util::fs::write_to_path(&file_a, "a")?;
            util::fs::write_to_path(&file_b, "b")?;
            repositories::add(&repo, &file_a).await?;
            repositories::add(&repo, &file_b).await?;
            repositories::commit(&repo, "Adding a.txt and b.txt")?;

            // Feature branch makes an unrelated change (doesn't touch a.txt)
            repositories::branches::create_checkout(&repo, "feature")?;
            util::fs::write_to_path(&file_b, "b modified on feature")?;
            repositories::add(&repo, &file_b).await?;
            repositories::commit(&repo, "Modifying b.txt on feature")?;

            // Main branch deletes a.txt
            repositories::checkout(&repo, &base_branch_name).await?;
            util::fs::remove_file(&file_a)?;
            repositories::add(&repo, &file_a).await?;
            repositories::commit(&repo, "Deleting a.txt on main")?;

            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch_name)?;

            // Base's deletion should win since merge didn't change the file
            let merge_commit =
                repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await?;

            assert_eq!(merge_commit.parent_ids.len(), 2);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_into_base_three_way_delete_modify_conflict() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch_name = repositories::branches::current_branch(&repo)?.unwrap().name;

            // LCA: create a file
            let file = repo.path.join("shared.txt");
            util::fs::write_to_path(&file, "original")?;
            repositories::add(&repo, &file).await?;
            repositories::commit(&repo, "Adding shared.txt")?;

            // Feature branch modifies the file
            repositories::branches::create_checkout(&repo, "feature")?;
            util::fs::write_to_path(&file, "modified on feature")?;
            repositories::add(&repo, &file).await?;
            repositories::commit(&repo, "Modifying shared.txt on feature")?;

            // Main branch deletes the same file
            repositories::checkout(&repo, &base_branch_name).await?;
            util::fs::remove_file(&file)?;
            repositories::add(&repo, &file).await?;
            repositories::commit(&repo, "Deleting shared.txt on main")?;

            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch_name)?;

            let result =
                repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await;
            assert!(
                matches!(result.unwrap_err(), OxenError::UpstreamMergeConflict(_)),
                "Delete on base + modify on merge should be a conflict"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_into_base_three_way_dir_metadata() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch_name = repositories::branches::current_branch(&repo)?.unwrap().name;

            // LCA: create three files (a.txt, b.txt, c.txt)
            let file_a = repo.path.join("a.txt");
            let file_b = repo.path.join("b.txt");
            let file_c = repo.path.join("c.txt");
            util::fs::write_to_path(&file_a, "aaa")?; // 3 bytes
            util::fs::write_to_path(&file_b, "bbb")?; // 3 bytes
            util::fs::write_to_path(&file_c, "ccc")?; // 3 bytes
            repositories::add(&repo, &file_a).await?;
            repositories::add(&repo, &file_b).await?;
            repositories::add(&repo, &file_c).await?;
            let lca_commit = repositories::commit(&repo, "LCA: adding a, b, c")?;

            // Snapshot the LCA root metadata for reference
            let lca_root =
                repositories::tree::get_dir_without_children(&repo, &lca_commit, "", None)?
                    .unwrap();
            let lca_dir = lca_root.dir()?;
            // The initial commit from run_one_commit_local_repo_test_async adds a file too, so
            // we track the LCA state as our baseline rather than hardcoding counts.
            let lca_num_files = lca_dir.num_files();
            let lca_num_bytes = lca_dir.num_bytes();

            // Feature branch: add d.txt, modify b.txt, delete c.txt
            repositories::branches::create_checkout(&repo, "feature")?;
            let file_d = repo.path.join("d.txt");
            util::fs::write_to_path(&file_d, "ddddd")?; // 5 bytes (added)
            util::fs::write_to_path(&file_b, "bbbbbbb")?; // 7 bytes (was 3)
            util::fs::remove_file(&file_c)?; // removed (was 3 bytes)
            repositories::add(&repo, &file_d).await?;
            repositories::add(&repo, &file_b).await?;
            repositories::add(&repo, &file_c).await?;
            repositories::commit(&repo, "Feature: add d, modify b, delete c")?;

            // Main branch: add e.txt to diverge (forces three-way merge)
            repositories::checkout(&repo, &base_branch_name).await?;
            let file_e = repo.path.join("e.txt");
            util::fs::write_to_path(&file_e, "ee")?; // 2 bytes
            repositories::add(&repo, &file_e).await?;
            repositories::commit(&repo, "Main: add e")?;

            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch_name)?;

            // Server-side three-way merge
            let merge_commit =
                repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await?;
            assert_eq!(merge_commit.parent_ids.len(), 2);

            // Read the root DirNode from the merge commit
            let merge_root =
                repositories::tree::get_dir_without_children(&repo, &merge_commit, "", None)?
                    .unwrap();
            let merge_dir = merge_root.dir()?;

            // Expected changes relative to LCA:
            //   +1 file (d.txt added), +1 file (e.txt added), -1 file (c.txt deleted) = net +1
            assert_eq!(
                merge_dir.num_files(),
                lca_num_files + 1, // net +1 file
                "num_files: LCA had {lca_num_files}, merge should have +1 (add d, add e, del c)"
            );

            // Expected byte changes relative to LCA:
            //   b.txt: 3 -> 7 (+4), d.txt: +5, e.txt: +2, c.txt: -3 = net +8
            assert_eq!(
                merge_dir.num_bytes(),
                lca_num_bytes + 8,
                "num_bytes: LCA had {lca_num_bytes}, merge should have +8 bytes"
            );

            // Verify the right files exist in the merge commit tree
            assert!(
                repositories::tree::get_file_by_path(&repo, &merge_commit, "a.txt")?.is_some(),
                "a.txt should exist"
            );
            assert!(
                repositories::tree::get_file_by_path(&repo, &merge_commit, "b.txt")?.is_some(),
                "b.txt should exist"
            );
            assert!(
                repositories::tree::get_file_by_path(&repo, &merge_commit, "c.txt")?.is_none(),
                "c.txt should be deleted"
            );
            assert!(
                repositories::tree::get_file_by_path(&repo, &merge_commit, "d.txt")?.is_some(),
                "d.txt should exist"
            );
            assert!(
                repositories::tree::get_file_by_path(&repo, &merge_commit, "e.txt")?.is_some(),
                "e.txt should exist"
            );

            Ok(())
        })
        .await
    }

    /// Regression test: deletion of a file in a directory that's shared between LCA and base
    /// (i.e., the directory hash is identical in both, so `unique_dir_entries` prunes it).
    /// The merge branch deletes a file from that shared directory.
    #[tokio::test]
    async fn test_merge_into_base_three_way_delete_in_shared_dir() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch_name = repositories::branches::current_branch(&repo)?.unwrap().name;

            // LCA: create files in a subdirectory
            let dir = repo.path.join("data");
            util::fs::create_dir_all(&dir)?;
            let file_a = dir.join("a.txt");
            let file_b = dir.join("b.txt");
            util::fs::write_to_path(&file_a, "aaa")?;
            util::fs::write_to_path(&file_b, "bbb")?;
            repositories::add(&repo, &file_a).await?;
            repositories::add(&repo, &file_b).await?;
            repositories::commit(&repo, "LCA: adding data/a.txt and data/b.txt")?;

            // Feature branch: delete data/b.txt (leaving data/a.txt untouched)
            repositories::branches::create_checkout(&repo, "feature")?;
            util::fs::remove_file(&file_b)?;
            repositories::add(&repo, &file_b).await?;
            repositories::commit(&repo, "Feature: delete data/b.txt")?;

            // Main branch: add an unrelated file to force divergence (3-way merge).
            // Crucially, do NOT touch data/ — so data/ is identical in LCA and base.
            repositories::checkout(&repo, &base_branch_name).await?;
            let unrelated = repo.path.join("other.txt");
            util::fs::write_to_path(&unrelated, "x")?;
            repositories::add(&repo, &unrelated).await?;
            repositories::commit(&repo, "Main: add other.txt")?;

            let merge_branch = repositories::branches::get_by_name(&repo, "feature")?;
            let base_branch = repositories::branches::get_by_name(&repo, &base_branch_name)?;

            let merge_commit =
                repositories::merge::merge_into_base(&repo, &merge_branch, &base_branch).await?;
            assert_eq!(merge_commit.parent_ids.len(), 2);

            // data/a.txt should still exist, data/b.txt should be deleted
            assert!(
                repositories::tree::get_file_by_path(&repo, &merge_commit, "data/a.txt")?.is_some(),
                "data/a.txt should exist in merge commit"
            );
            assert!(
                repositories::tree::get_file_by_path(&repo, &merge_commit, "data/b.txt")?.is_none(),
                "data/b.txt should be deleted in merge commit"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_commit_into_base_server_safe_ff_does_not_touch_working_dir()
    -> Result<(), OxenError> {
        // Verifies that merge_commit_into_base_server_safe succeeds even when
        // stale files exist in the repo directory (no "local changes would be
        // overwritten" error).
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let main_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Add a file and commit
            let readme = repo.path.join("README.md");
            util::fs::write_to_path(&readme, "original")?;
            repositories::add(&repo, &readme).await?;
            let base_commit = repositories::commit(&repo, "Add README")?;

            // Branch, modify the file, and commit
            repositories::branches::create_checkout(&repo, "feature")?;
            util::fs::write_to_path(&readme, "modified on feature")?;
            repositories::add(&repo, &readme).await?;
            let feature_commit = repositories::commit(&repo, "Modify README")?;

            // Go back to main so HEAD is at base_commit (FF scenario)
            repositories::checkout(&repo, &main_branch.name).await?;

            // Simulate server state: write a stale/different file at the path.
            // On a real server there is no checkout, but the repo directory may
            // contain leftover files that differ from the tree.
            util::fs::write_to_path(&readme, "stale server copy")?;

            // Server-safe merge should NOT check working-dir files → no error
            let result = repositories::merge::merge_commit_into_base_server_safe(
                &repo,
                &feature_commit,
                &base_commit,
            )
            .await?;

            assert!(result.is_some(), "FF merge should return the merge commit");
            assert_eq!(result.unwrap().id, feature_commit.id);

            // The stale file on disk should be UNTOUCHED (server never writes)
            let on_disk = util::fs::read_from_path(&readme)?;
            assert_eq!(
                on_disk, "stale server copy",
                "server-safe merge must not modify files on disk"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_commit_into_base_server_safe_three_way_does_not_touch_working_dir()
    -> Result<(), OxenError> {
        // Verifies that merge_commit_into_base_server_safe uses the server-safe
        // three-way merge path and never touches the working directory.
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let main_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Shared ancestor
            let a_path = repo.path.join("a.txt");
            util::fs::write_to_path(&a_path, "a")?;
            repositories::add(&repo, &a_path).await?;
            repositories::commit(&repo, "Add a.txt")?;

            // Feature branch: add b.txt
            repositories::branches::create_checkout(&repo, "feature")?;
            let b_path = repo.path.join("b.txt");
            util::fs::write_to_path(&b_path, "b")?;
            repositories::add(&repo, &b_path).await?;
            let feature_commit = repositories::commit(&repo, "Add b.txt")?;

            // Back to main: add c.txt (diverge → forces three-way merge)
            repositories::checkout(&repo, &main_branch.name).await?;
            let c_path = repo.path.join("c.txt");
            util::fs::write_to_path(&c_path, "c")?;
            repositories::add(&repo, &c_path).await?;
            let base_commit = repositories::commit(&repo, "Add c.txt")?;

            // Simulate stale server state
            util::fs::write_to_path(&a_path, "stale")?;

            // Server-safe merge should succeed without checking disk
            let result = repositories::merge::merge_commit_into_base_server_safe(
                &repo,
                &feature_commit,
                &base_commit,
            )
            .await?;
            assert!(
                result.is_some(),
                "three-way merge should return a merge commit"
            );

            // Stale file should be untouched
            let on_disk = util::fs::read_from_path(&a_path)?;
            assert_eq!(
                on_disk, "stale",
                "server-safe merge must not modify files on disk"
            );

            Ok(())
        })
        .await
    }

    // Simulates an `oxen pull` that was killed mid-restore after it had already
    // rewritten a tracked file to the merge target's content but before HEAD
    // was advanced. On resume the file differs from HEAD (commit A) yet matches
    // the merge target (commit B), and must be treated as a successful no-op
    // rather than flagged as "cannot_overwrite".
    #[tokio::test]
    async fn test_merge_resumes_after_partial_fast_forward() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let og_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Commit A on main: world.txt = "World"
            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World")?;
            repositories::add(&repo, &world_file).await?;
            repositories::commit(&repo, "Adding world file")?;

            // Commit B on feature: modify world.txt
            let branch_name = "update-world";
            repositories::branches::create_checkout(&repo, branch_name)?;
            util::fs::write_to_path(&world_file, "World2")?;
            repositories::add(&repo, &world_file).await?;
            let target_commit = repositories::commit(&repo, "Updating world")?;
            let merge_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Back on main: checkout restores world.txt to commit A's content.
            repositories::checkout(&repo, &og_branch.name).await?;
            assert_eq!(util::fs::read_from_path(&world_file)?, "World");

            // Simulate an interrupted fast-forward merge: the previous pull
            // rewrote world.txt to the target's content on disk but crashed
            // before advancing HEAD. HEAD still points to commit A.
            util::fs::write_to_path(&world_file, "World2")?;

            // The resumed merge must not fail with `cannot_overwrite_files`:
            // world.txt differs from HEAD (= commit A) but already matches the
            // merge target, so restoring is a safe no-op.
            let commit = repositories::merge::merge(&repo, &merge_branch.name)
                .await?
                .expect("resumed fast-forward merge should produce a commit");

            assert_eq!(commit.id, target_commit.id);
            let head_commit = repositories::commits::head_commit(&repo)?;
            assert_eq!(head_commit.id, target_commit.id);
            assert_eq!(util::fs::read_from_path(&world_file)?, "World2");
            Ok(())
        })
        .await
    }

    // Helper: build a feature branch with a single file-modification commit, return
    // (branch_name, target_commit_id) back on main.
    async fn make_feature_branch_with_modification(
        repo: &LocalRepository,
        file_name: &str,
        base_contents: &str,
        merge_contents: &str,
        branch: &str,
    ) -> Result<(String, Commit, Commit), OxenError> {
        let og_branch = repositories::branches::current_branch(repo)?.unwrap();
        let path = repo.path.join(file_name);
        util::fs::write_to_path(&path, base_contents)?;
        repositories::add(repo, &path).await?;
        let base_commit = repositories::commit(repo, "base commit")?;

        repositories::branches::create_checkout(repo, branch)?;
        util::fs::write_to_path(&path, merge_contents)?;
        repositories::add(repo, &path).await?;
        let target_commit = repositories::commit(repo, "target commit")?;

        repositories::checkout(repo, &og_branch.name).await?;
        Ok((og_branch.name, base_commit, target_commit))
    }

    // A truncated file (content matches neither base nor target) must still resume
    // cleanly when the MERGE_IN_PROGRESS marker names the same target commit: is_resume
    // bypasses the conflict check and force-restores the file from the version store.
    #[tokio::test]
    async fn test_merge_resume_truncated_fast_forward() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let (_main, _base, target) = make_feature_branch_with_modification(
                &repo,
                "world.txt",
                "World",
                "World2",
                "update-world",
            )
            .await?;

            let world_file = repo.path.join("world.txt");
            assert_eq!(util::fs::read_from_path(&world_file)?, "World");

            // Simulate an interrupted merge that truncated the file mid-write:
            // content matches neither base ("World") nor target ("World2").
            util::fs::write_to_path(&world_file, "partial")?;
            merge_marker::write(&repo, &target.id).await?;

            let commit = repositories::merge::merge(&repo, "update-world")
                .await?
                .expect("resumed merge should produce a commit");
            assert_eq!(commit.id, target.id);
            assert_eq!(util::fs::read_from_path(&world_file)?, "World2");
            assert!(
                !merge_marker::exists(&repo).await?,
                "marker must be cleared after a successful resume"
            );
            Ok(())
        })
        .await
    }

    // If the marker names a different target than the current merge, abort with
    // MergeInProgressMismatch without touching the marker or the working tree.
    #[tokio::test]
    async fn test_merge_aborts_on_marker_mismatch() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let (_main, _base, _target) = make_feature_branch_with_modification(
                &repo,
                "world.txt",
                "World",
                "World2",
                "update-world",
            )
            .await?;

            // A stale marker points at an unrelated commit id (e.g. from a previous
            // merge against a different branch).
            let stale = "deadbeefdeadbeefdeadbeefdeadbeef";
            merge_marker::write(&repo, stale).await?;

            let err = repositories::merge::merge(&repo, "update-world")
                .await
                .expect_err("mismatched marker must abort the new merge");
            match err {
                OxenError::MergeInProgressMismatch { ref expected, .. } => {
                    assert_eq!(expected, stale);
                }
                other => panic!("expected MergeInProgressMismatch, got {other:?}"),
            }

            // Marker is untouched so the user can run `oxen merge --abort`.
            let current = merge_marker::read(&repo).await?;
            assert_eq!(current.as_deref(), Some(stale));
            Ok(())
        })
        .await
    }

    // A normal successful merge never leaves MERGE_IN_PROGRESS behind.
    #[tokio::test]
    async fn test_merge_clears_marker_on_success() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let (_main, _base, _target) = make_feature_branch_with_modification(
                &repo,
                "world.txt",
                "World",
                "World2",
                "update-world",
            )
            .await?;

            assert!(!merge_marker::exists(&repo).await?);
            repositories::merge::merge(&repo, "update-world")
                .await?
                .unwrap();
            assert!(
                !merge_marker::exists(&repo).await?,
                "marker must be absent after a clean merge"
            );
            Ok(())
        })
        .await
    }

    // Abort restores the working tree from a fast-forward-in-progress state and
    // clears every merge marker.
    #[tokio::test]
    async fn test_merge_abort_resets_partial_ff() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let (_main, base, target) = make_feature_branch_with_modification(
                &repo,
                "world.txt",
                "World",
                "World2",
                "update-world",
            )
            .await?;

            let world_file = repo.path.join("world.txt");
            util::fs::write_to_path(&world_file, "World2")?;
            merge_marker::write(&repo, &target.id).await?;

            repositories::merge::abort_merge(&repo).await?;

            assert_eq!(util::fs::read_from_path(&world_file)?, "World");
            assert!(!merge_marker::exists(&repo).await?);
            let head = repositories::commits::head_commit(&repo)?;
            assert_eq!(head.id, base.id, "HEAD must remain at the pre-merge base");
            Ok(())
        })
        .await
    }

    // An interrupted merge can leave a file truncated mid-write: its hash matches neither the
    // pre-merge base nor the merge target. Abort must force-restore HEAD's version regardless —
    // the core motivating case for Fix B.
    #[tokio::test]
    async fn test_merge_abort_resets_truncated_file() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let (_main, base, target) = make_feature_branch_with_modification(
                &repo,
                "world.txt",
                "World",
                "World2",
                "update-world",
            )
            .await?;

            let world_file = repo.path.join("world.txt");
            // Content matches neither base ("World") nor target ("World2").
            util::fs::write_to_path(&world_file, "TRUNC")?;
            merge_marker::write(&repo, &target.id).await?;

            repositories::merge::abort_merge(&repo).await?;

            assert_eq!(util::fs::read_from_path(&world_file)?, "World");
            assert!(!merge_marker::exists(&repo).await?);
            let head = repositories::commits::head_commit(&repo)?;
            assert_eq!(head.id, base.id);
            Ok(())
        })
        .await
    }

    // Abort wipes the 3-way merge conflict state (MERGE_HEAD / ORIG_HEAD / conflict DB)
    // left behind when a 3-way merge detects conflicts.
    #[tokio::test]
    async fn test_merge_abort_clears_conflict_state() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let base_branch = repositories::branches::current_branch(&repo)?.unwrap().name;

            let file = repo.path.join("shared.txt");
            util::fs::write_to_path(&file, "lca")?;
            repositories::add(&repo, &file).await?;
            repositories::commit(&repo, "lca")?;

            repositories::branches::create_checkout(&repo, "feature")?;
            util::fs::write_to_path(&file, "feature side")?;
            repositories::add(&repo, &file).await?;
            repositories::commit(&repo, "feature modification")?;

            repositories::checkout(&repo, &base_branch).await?;
            util::fs::write_to_path(&file, "base side")?;
            repositories::add(&repo, &file).await?;
            repositories::commit(&repo, "base modification")?;

            // Conflicting 3-way merge: leaves MERGE_HEAD / ORIG_HEAD / conflicts DB.
            let merge_result = repositories::merge::merge(&repo, "feature").await?;
            assert!(merge_result.is_none(), "expected a conflict");

            let hidden = util::fs::oxen_hidden_dir(&repo.path);
            assert!(hidden.join(crate::constants::MERGE_HEAD_FILE).exists());
            assert!(hidden.join(crate::constants::ORIG_HEAD_FILE).exists());

            repositories::merge::abort_merge(&repo).await?;

            assert!(!hidden.join(crate::constants::MERGE_HEAD_FILE).exists());
            assert!(!hidden.join(crate::constants::ORIG_HEAD_FILE).exists());
            assert!(
                !hidden
                    .join(crate::constants::MERGE_IN_PROGRESS_FILE)
                    .exists()
            );
            Ok(())
        })
        .await
    }

    // A marker left over from a merge whose target commit no longer exists locally — exactly the
    // "retry the original target" escape hatch in the MergeInProgressMismatch hint. Abort must
    // still clean up the working tree and the marker without requiring the target to be resolvable.
    #[tokio::test]
    async fn test_merge_abort_with_unknown_marker_target() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let (_main, base, _target) = make_feature_branch_with_modification(
                &repo,
                "world.txt",
                "World",
                "World2",
                "update-world",
            )
            .await?;

            let world_file = repo.path.join("world.txt");
            // Simulate an interrupted merge mid-write.
            util::fs::write_to_path(&world_file, "TRUNC")?;
            // Marker points at a commit id that does not exist in this repo.
            let unknown = "deadbeefdeadbeefdeadbeefdeadbeef";
            merge_marker::write(&repo, unknown).await?;

            repositories::merge::abort_merge(&repo).await?;

            assert_eq!(util::fs::read_from_path(&world_file)?, "World");
            assert!(!merge_marker::exists(&repo).await?);
            let head = repositories::commits::head_commit(&repo)?;
            assert_eq!(head.id, base.id);
            Ok(())
        })
        .await
    }

    // Abort with nothing to abort returns a specific error variant, not a silent no-op.
    #[tokio::test]
    async fn test_merge_abort_errors_when_nothing_to_abort() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let err = repositories::merge::abort_merge(&repo)
                .await
                .expect_err("abort on a clean repo should error");
            assert!(
                matches!(err, OxenError::NoMergeInProgress),
                "expected NoMergeInProgress, got {err:?}"
            );
            Ok(())
        })
        .await
    }
}
