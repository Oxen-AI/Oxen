use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::repositories;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use time::OffsetDateTime;

fn compute_commit_hash(
    parent_ids: &[String],
    message: &str,
    author: &str,
    email: &str,
    timestamp: OffsetDateTime,
) -> String {
    use xxhash_rust::xxh3::Xxh3;
    let mut hasher = Xxh3::new();
    hasher.update(b"commit");
    hasher.update(format!("{:?}", parent_ids).as_bytes());
    hasher.update(message.as_bytes());
    hasher.update(author.as_bytes());
    hasher.update(email.as_bytes());
    hasher.update(&timestamp.unix_timestamp().to_le_bytes());
    format!("{:032x}", hasher.digest128())
}

pub fn analyze_squashable_commits(
    repo: &LocalRepository,
    n: usize, // Number of commits from HEAD to preserve
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

fn is_squashable_commit(repo: &LocalRepository, commit: &Commit) -> Result<bool, OxenError> {
    if commit.parent_ids.is_empty() {
        return Ok(false);
    }

    // Merge commits (multiple parents) should not be squashed to preserve branch topology
    if commit.parent_ids.len() > 1 {
        return Ok(false);
    }

    let parent_commit = repositories::commits::get_by_id(repo, &commit.parent_ids[0])?
        .ok_or_else(|| OxenError::basic_str("Parent commit not found"))?;

    let current_files = get_commit_files(repo, commit)?;
    let parent_files = get_commit_files(repo, &parent_commit)?;

    for path in parent_files.keys() {
        if !current_files.contains_key(path) {
            return Ok(false);
        }
    }

    Ok(true)
}

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

// The commits should be ordered from newest to oldest (reverse chronological order).
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

    // Compute a fresh commit ID based on the new commit's actual fields
    let new_id = compute_commit_hash(
        &[parent_commit_id.clone()],
        new_message,
        &newest_commit.author,
        &newest_commit.email,
        newest_commit.timestamp,
    );

    let new_commit = repositories::commits::create_empty_commit(
        repo,
        repositories::branches::current_branch(repo)?
            .ok_or_else(|| OxenError::basic_str("No current branch"))?
            .name,
        &Commit {
            id: new_id,
            parent_ids: vec![parent_commit_id],
            message: new_message.to_string(),
            author: newest_commit.author.clone(),
            email: newest_commit.email.clone(),
            timestamp: newest_commit.timestamp,
        },
        Some(&newest_commit.id), // preserve tree from newest commit, not branch HEAD
    )?;

    Ok(new_commit)
}

pub fn execute_squash(repo: &LocalRepository, n: usize) -> Result<Vec<Commit>, OxenError> {
    let squash_groups = analyze_squashable_commits(repo, n)?;
    if squash_groups.is_empty() {
        return Ok(vec![]);
    }

    let all_commits = repositories::commits::list(repo)?;
    let commits_to_process = &all_commits[n..];

    // Build map: commit_id -> group_index
    let mut commit_to_group: HashMap<String, usize> = HashMap::new();
    for (group_idx, group) in squash_groups.iter().enumerate() {
        for commit in group {
            commit_to_group.insert(commit.id.clone(), group_idx);
        }
    }

    // Identify oldest and newest commit in each group
    let mut group_oldest_id: HashMap<usize, String> = HashMap::new();
    let mut group_newest: HashMap<usize, &Commit> = HashMap::new();
    for (group_idx, group) in squash_groups.iter().enumerate() {
        group_oldest_id.insert(group_idx, group.last().unwrap().id.clone());
        group_newest.insert(group_idx, group.first().unwrap());
    }

    // Track parent remapping: old_id -> new_id
    let mut parent_map: HashMap<String, String> = HashMap::new();
    let mut squashed_commits = vec![];
    let mut processed_groups: HashSet<usize> = HashSet::new();

    let branch_name = repositories::branches::current_branch(repo)?
        .ok_or_else(|| OxenError::basic_str("No current branch"))?
        .name;

    // Process commits from oldest to newest
    for commit in commits_to_process.iter().rev() {
        if commit.parent_ids.is_empty() {
            continue;
        }

        let original_parent = &commit.parent_ids[0];
        let mapped_parent = parent_map
            .get(original_parent)
            .unwrap_or(original_parent)
            .clone();

        if let Some(&group_idx) = commit_to_group.get(&commit.id) {
            // This commit is part of a squash group
            if group_oldest_id.get(&group_idx) == Some(&commit.id)
                && !processed_groups.contains(&group_idx)
            {
                // This is the oldest commit in the group - create squashed commit
                let newest = group_newest.get(&group_idx).unwrap();
                let group = &squash_groups[group_idx];
                let messages: Vec<String> = group.iter().map(|c| c.message.clone()).collect();
                let combined_message = messages.join(", ");

                let new_id = compute_commit_hash(
                    &[mapped_parent.clone()],
                    &combined_message,
                    &newest.author,
                    &newest.email,
                    newest.timestamp,
                );

                let new_commit = repositories::commits::create_empty_commit(
                    repo,
                    &branch_name,
                    &Commit {
                        id: new_id.clone(),
                        parent_ids: vec![mapped_parent],
                        message: combined_message,
                        author: newest.author.clone(),
                        email: newest.email.clone(),
                        timestamp: newest.timestamp,
                    },
                    Some(&newest.id), // preserve tree from newest commit
                )?;

                parent_map.insert(newest.id.clone(), new_commit.id.clone());
                squashed_commits.push(new_commit);
                processed_groups.insert(group_idx);
            }
            // Skip other commits in the group
        } else {
            // Boundary commit - rewrite with updated parent
            let new_id = compute_commit_hash(
                &[mapped_parent.clone()],
                &commit.message,
                &commit.author,
                &commit.email,
                commit.timestamp,
            );

            let new_commit = repositories::commits::create_empty_commit(
                repo,
                &branch_name,
                &Commit {
                    id: new_id.clone(),
                    parent_ids: vec![mapped_parent],
                    message: commit.message.clone(),
                    author: commit.author.clone(),
                    email: commit.email.clone(),
                    timestamp: commit.timestamp,
                },
                Some(&commit.id), // preserve tree from original
            )?;

            parent_map.insert(commit.id.clone(), new_commit.id.clone());
        }
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
            assert_eq!(squashable[0].len(), 2); // root commit excluded (no parent)

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
            let _commit1 = repositories::commit(&repo, "add file1")?;

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

    #[tokio::test]
    async fn test_multi_group_squash_preserves_history() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Create two squashable groups separated by a delete (non-squashable boundary)
            // commit1 (root) -> commit2 -> commit3 -> commit4 (delete) -> commit5 -> commit6
            //                   [--- group 2 ---]                        [--- group 1 ---]

            let file1 = repo.path.join("file1.txt");
            util::fs::write_to_path(&file1, "content1")?;
            repositories::add(&repo, &file1).await?;
            let _commit1 = repositories::commit(&repo, "add file1")?; // root

            let file2 = repo.path.join("file2.txt");
            util::fs::write_to_path(&file2, "content2")?;
            repositories::add(&repo, &file2).await?;
            let _commit2 = repositories::commit(&repo, "add file2")?;

            let file3 = repo.path.join("file3.txt");
            util::fs::write_to_path(&file3, "content3")?;
            repositories::add(&repo, &file3).await?;
            let _commit3 = repositories::commit(&repo, "add file3")?;

            // Delete creates a boundary (non-squashable)
            std::fs::remove_file(&file2)?;
            repositories::add(&repo, &file2).await?;
            let commit4 = repositories::commit(&repo, "delete file2")?;

            let file4 = repo.path.join("file4.txt");
            util::fs::write_to_path(&file4, "content4")?;
            repositories::add(&repo, &file4).await?;
            let _commit5 = repositories::commit(&repo, "add file4")?;

            let file5 = repo.path.join("file5.txt");
            util::fs::write_to_path(&file5, "content5")?;
            repositories::add(&repo, &file5).await?;
            let _commit6 = repositories::commit(&repo, "add file5")?;

            // Verify we have two squashable groups
            let groups = analyze_squashable_commits(&repo, 0)?;
            assert_eq!(groups.len(), 2);

            // Execute squash
            let squashed = execute_squash(&repo, 0)?;
            assert_eq!(squashed.len(), 2);

            // Verify commit history is properly connected
            let commits_after = repositories::commits::list(&repo)?;

            // Should have: squashed1 -> boundary' -> squashed2 -> root (4 commits)
            assert_eq!(commits_after.len(), 4);

            // Walk the history and verify parent linkage
            for i in 0..commits_after.len() - 1 {
                let commit = &commits_after[i];
                let expected_parent = &commits_after[i + 1];
                assert!(
                    commit.parent_ids.contains(&expected_parent.id),
                    "Commit {} should have parent {}",
                    commit.id,
                    expected_parent.id
                );
            }

            // The boundary commit (rewritten) should exist with same message
            assert!(
                commits_after.iter().any(|c| c.message == commit4.message),
                "Boundary commit message should still exist in history"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_merge_commits_are_not_squashable() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // Create a three-way merge scenario:
            // A - C - D - M (merge commit with two parents)
            //    \       /
            //     B - E

            // Get main branch (repo already has initial commit from test helper)
            let main_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Initial commit A
            let file_a = repo.path.join("a.txt");
            util::fs::write_to_path(&file_a, "a")?;
            repositories::add(&repo, &file_a).await?;
            repositories::commit(&repo, "Commit A")?;

            // Create feature branch and add commits
            let feature_branch = "feature";
            repositories::branches::create_checkout(&repo, feature_branch)?;

            let file_b = repo.path.join("b.txt");
            util::fs::write_to_path(&file_b, "b")?;
            repositories::add(&repo, &file_b).await?;
            repositories::commit(&repo, "Commit B")?;

            let file_e = repo.path.join("e.txt");
            util::fs::write_to_path(&file_e, "e")?;
            repositories::add(&repo, &file_e).await?;
            repositories::commit(&repo, "Commit E")?;

            // Go back to main and add more commits
            repositories::checkout(&repo, &main_branch.name).await?;

            let file_c = repo.path.join("c.txt");
            util::fs::write_to_path(&file_c, "c")?;
            repositories::add(&repo, &file_c).await?;
            repositories::commit(&repo, "Commit C")?;

            let file_d = repo.path.join("d.txt");
            util::fs::write_to_path(&file_d, "d")?;
            repositories::add(&repo, &file_d).await?;
            repositories::commit(&repo, "Commit D")?;

            // Merge feature branch into main - creates merge commit M
            let merge_commit = repositories::merge::merge(&repo, feature_branch).await?;
            assert!(merge_commit.is_some(), "Merge should succeed");
            let merge_commit = merge_commit.unwrap();

            // Verify merge commit has two parents
            assert_eq!(
                merge_commit.parent_ids.len(),
                2,
                "Merge commit should have exactly 2 parents"
            );

            // Verify merge commit is NOT squashable
            assert!(
                !is_squashable_commit(&repo, &merge_commit)?,
                "Merge commits should not be squashable"
            );

            // Add a regular commit after the merge
            let file_f = repo.path.join("f.txt");
            util::fs::write_to_path(&file_f, "f")?;
            repositories::add(&repo, &file_f).await?;
            let regular_commit = repositories::commit(&repo, "Commit F")?;

            // Verify regular commit IS squashable
            assert!(
                is_squashable_commit(&repo, &regular_commit)?,
                "Regular single-parent commits should be squashable"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_squash_groups_split_at_merge_commits() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            // Create: A -> B -> C -> M (merge) -> D -> E
            // Expected groups: [B, C] and [D, E] split by merge M

            // Get main branch (repo already has initial commit from test helper)
            let main_branch = repositories::branches::current_branch(&repo)?.unwrap();

            // Commit A
            let file_a = repo.path.join("a.txt");
            util::fs::write_to_path(&file_a, "a")?;
            repositories::add(&repo, &file_a).await?;
            repositories::commit(&repo, "Commit A")?;

            // Commit B
            let file_b = repo.path.join("b.txt");
            util::fs::write_to_path(&file_b, "b")?;
            repositories::add(&repo, &file_b).await?;
            repositories::commit(&repo, "Commit B")?;

            // Create feature branch for merge
            let feature_branch = "feature";
            repositories::branches::create_checkout(&repo, feature_branch)?;
            let file_feature = repo.path.join("feature.txt");
            util::fs::write_to_path(&file_feature, "feature")?;
            repositories::add(&repo, &file_feature).await?;
            repositories::commit(&repo, "Feature commit")?;

            // Back to main, add commit C
            repositories::checkout(&repo, &main_branch.name).await?;
            let file_c = repo.path.join("c.txt");
            util::fs::write_to_path(&file_c, "c")?;
            repositories::add(&repo, &file_c).await?;
            repositories::commit(&repo, "Commit C")?;

            // Merge - creates merge commit M
            let merge_result = repositories::merge::merge(&repo, feature_branch).await?;
            assert!(merge_result.is_some(), "Merge should succeed");

            // Commits D and E after merge
            let file_d = repo.path.join("d.txt");
            util::fs::write_to_path(&file_d, "d")?;
            repositories::add(&repo, &file_d).await?;
            repositories::commit(&repo, "Commit D")?;

            let file_e = repo.path.join("e.txt");
            util::fs::write_to_path(&file_e, "e")?;
            repositories::add(&repo, &file_e).await?;
            repositories::commit(&repo, "Commit E")?;

            // Analyze squashable commits (preserve 0 from HEAD)
            let groups = analyze_squashable_commits(&repo, 0)?;

            // The merge commit should act as a boundary, splitting groups
            // We should have at least one group, and the merge commit should not be in any group
            for group in &groups {
                for commit in group {
                    assert!(
                        commit.parent_ids.len() <= 1,
                        "No merge commits should be in squash groups"
                    );
                }
            }

            Ok(())
        })
        .await
    }
}
