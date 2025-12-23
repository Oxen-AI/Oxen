use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::repositories;
use std::collections::HashMap;
use std::path::PathBuf;

/// Squash sequential non-delete commits before HEAD - N commits
///
/// This function analyzes the commit history and squashes consecutive commits
/// that don't delete files (additions and modifications are allowed) into single commits.
///
/// # Arguments
/// * `repo` - The local repository
/// * `n` - Number of commits from HEAD to preserve (don't squash these)
///
/// # Returns
/// A vector of commit groups that should be squashed together
pub fn analyze_squashable_commits(
    repo: &LocalRepository,
    n: usize,
) -> Result<Vec<Vec<Commit>>, OxenError> {
    let commits = repositories::commits::list(repo)?;

    if commits.len() <= n {
        return Ok(vec![]);
    }

    let commits_to_analyze = &commits[n..];
    let mut squash_groups: Vec<Vec<Commit>> = vec![];
    let mut current_group: Vec<Commit> = vec![];

    for commit in commits_to_analyze {
        // Skip initial commits (no parents) - they can't be squashed
        if commit.parent_ids.is_empty() {
            if current_group.len() > 1 {
                squash_groups.push(current_group.clone());
            }
            current_group.clear();
            continue;
        }

        if is_squashable_commit(repo, commit)? {
            current_group.push(commit.clone());
        } else {
            if current_group.len() > 1 {
                squash_groups.push(current_group.clone());
            }
            current_group.clear();
        }
    }

    // Don't add the final group if it would include commits without proper parents
    if current_group.len() > 1 {
        squash_groups.push(current_group);
    }

    Ok(squash_groups)
}

/// Check if a commit is squashable (no deletions, modifications are allowed)
///
/// Returns false for commits without parents (initial commits) or commits that delete files.
/// Commits that add or modify files are considered squashable.
fn is_squashable_commit(repo: &LocalRepository, commit: &Commit) -> Result<bool, OxenError> {
    // Initial commits should not be considered squashable for squashing purposes
    if commit.parent_ids.is_empty() {
        return Ok(false);
    }

    let parent_commit = repositories::commits::get_by_id(repo, &commit.parent_ids[0])?
        .ok_or_else(|| OxenError::basic_str("Parent commit not found"))?;

    let current_files = get_commit_files(repo, commit)?;
    let parent_files = get_commit_files(repo, &parent_commit)?;

    // Only check if any files were deleted (in parent but not in current)
    // Modifications and additions are allowed
    for path in parent_files.keys() {
        if !current_files.contains_key(path) {
            return Ok(false);
        }
    }

    // If we get here, no files were deleted - this commit is squashable
    Ok(true)
}

/// Get all files in a commit as a map of path -> hash
fn get_commit_files(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<HashMap<PathBuf, String>, OxenError> {
    let mut files = HashMap::new();

    let Some(tree) = repositories::tree::get_root_with_children(repo, commit)? else {
        return Ok(files);
    };

    let (file_nodes, _) = repositories::tree::list_files_and_dirs(&tree)?;

    for file_with_dir in file_nodes {
        let path = file_with_dir.dir.join(file_with_dir.file_node.name());
        let hash = file_with_dir.file_node.hash().to_string();
        files.insert(path, hash);
    }

    Ok(files)
}

/// Squash a group of commits into a single commit
///
/// This creates a new commit that combines all the changes from the group
/// and updates the branch pointer to point to the new squashed commit.
///
/// The commits should be ordered from newest to oldest (reverse chronological order).
pub fn squash_commits(
    repo: &LocalRepository,
    commits: &[Commit],
    new_message: &str,
) -> Result<Commit, OxenError> {
    if commits.is_empty() {
        return Err(OxenError::basic_str("No commits to squash"));
    }

    if commits.len() == 1 {
        return Ok(commits[0].clone());
    }

    let oldest_commit = commits.last().unwrap();
    let newest_commit = commits.first().unwrap();

    let parent_commit_id = if !oldest_commit.parent_ids.is_empty() {
        oldest_commit.parent_ids[0].clone()
    } else {
        return Err(OxenError::basic_str("Cannot squash root commits"));
    };

    let _parent_commit = repositories::commits::get_by_id(repo, &parent_commit_id)?
        .ok_or_else(|| OxenError::basic_str("Parent commit not found"))?;

    let new_commit = repositories::commits::create_empty_commit(
        repo,
        repositories::branches::current_branch(repo)?
            .ok_or_else(|| OxenError::basic_str("No current branch"))?
            .name,
        &Commit {
            id: newest_commit.id.clone(),
            parent_ids: vec![parent_commit_id],
            message: new_message.to_string(),
            author: newest_commit.author.clone(),
            email: newest_commit.email.clone(),
            timestamp: newest_commit.timestamp,
        },
    )?;

    Ok(new_commit)
}

/// Execute squashing for all identified squashable commit groups
///
/// This will squash all groups of sequential add-only commits before HEAD - N
pub fn execute_squash(repo: &LocalRepository, n: usize) -> Result<Vec<Commit>, OxenError> {
    let squash_groups = analyze_squashable_commits(repo, n)?;

    if squash_groups.is_empty() {
        return Ok(vec![]);
    }

    let mut squashed_commits = vec![];

    for group in squash_groups {
        let messages: Vec<String> = group.iter().map(|c| c.message.clone()).collect();
        let combined_message = messages.join(", ");

        let squashed = squash_commits(repo, &group, &combined_message)?;
        squashed_commits.push(squashed);
    }

    Ok(squashed_commits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_analyze_squashable_commits() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let file1 = repo.path.join("file1.txt");
            util::fs::write_to_path(&file1, "content1")?;
            repositories::add(&repo, &file1).await?;
            repositories::commit(&repo, "add file1")?;

            let file2 = repo.path.join("file2.txt");
            util::fs::write_to_path(&file2, "content2")?;
            repositories::add(&repo, &file2).await?;
            repositories::commit(&repo, "add file2")?;

            let file3 = repo.path.join("file3.txt");
            util::fs::write_to_path(&file3, "content3")?;
            repositories::add(&repo, &file3).await?;
            repositories::commit(&repo, "add file3")?;

            let squashable = analyze_squashable_commits(&repo, 0)?;

            assert_eq!(squashable.len(), 1);
            assert_eq!(squashable[0].len(), 3);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_is_squashable_commit() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let file1 = repo.path.join("file1.txt");
            util::fs::write_to_path(&file1, "content1")?;
            repositories::add(&repo, &file1).await?;
            let commit1 = repositories::commit(&repo, "add file1")?;

            let file2 = repo.path.join("file2.txt");
            util::fs::write_to_path(&file2, "content2")?;
            repositories::add(&repo, &file2).await?;
            let commit2 = repositories::commit(&repo, "add file2")?;

            assert!(is_squashable_commit(&repo, &commit2)?);

            // Modifications are now allowed - this should still be squashable
            util::fs::write_to_path(&file1, "modified content")?;
            repositories::add(&repo, &file1).await?;
            let commit3 = repositories::commit(&repo, "modify file1")?;

            assert!(is_squashable_commit(&repo, &commit3)?);

            // Deletions are not allowed - this should not be squashable
            std::fs::remove_file(&file2)?;
            repositories::add(&repo, &file2).await?;
            let commit4 = repositories::commit(&repo, "delete file2")?;

            assert!(!is_squashable_commit(&repo, &commit4)?);

            Ok(())
        })
        .await
    }
}
