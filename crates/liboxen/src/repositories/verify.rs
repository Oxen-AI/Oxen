//! # oxen verify
//!
//! Read-only integrity checks for a repository.
//!
//! Answers one question: does everything this repository's branches reference actually exist, and
//! does it look like what the tree says it is. Nothing here mutates a repository.
//!
//! Findings are reported as counts plus a bounded sample.

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;

use crate::error::OxenError;
use crate::model::{Commit, CommitEntry, LocalRepository, MerkleHash, NewCommit};
use crate::repositories;
use crate::repositories::commits::commit_writer::compute_commit_id;

/// Examples of each finding a report carries alongside the count.
const SAMPLE_LIMIT: usize = 10;

/// Concurrent size probes against the version store.
const MAX_CONCURRENT_SIZE_PROBES: usize = 16;

/// One class of finding: how many there are, and a bounded sample for triage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Findings<T> {
    pub count: usize,
    pub sample: Vec<T>,
}

// Hand-written so an empty `Findings` needs nothing of `T`, which a derive would demand.
impl<T> Default for Findings<T> {
    fn default() -> Self {
        Self {
            count: 0,
            sample: Vec::new(),
        }
    }
}

impl<T> Findings<T> {
    fn record(&mut self, item: T) {
        self.count += 1;
        if self.sample.len() < SAMPLE_LIMIT {
            self.sample.push(item);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
}

/// A branch whose head names a commit the repository does not have.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DanglingBranch {
    pub branch: String,
    pub commit_id: String,
}

/// A commit naming a parent the repository does not have.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DanglingParent {
    pub commit_id: String,
    pub parent_id: String,
}

/// A commit whose recorded id is not the hash of the fields it stores.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MisaddressedCommit {
    pub recorded_id: String,
    pub computed_id: String,
}

/// A version blob whose stored size disagrees with the size its file node declares.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SizeMismatch {
    pub hash: String,
    pub path: PathBuf,
    pub declared_bytes: u64,
    pub stored_bytes: u64,
}

/// What a [`verify_repo`] pass found.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct VerifyReport {
    pub branches_checked: usize,
    pub commits_checked: usize,
    pub entries_checked: usize,
    pub dangling_branches: Findings<DanglingBranch>,
    pub dangling_parents: Findings<DanglingParent>,
    pub misaddressed_commits: Findings<MisaddressedCommit>,
    pub missing_nodes: Findings<String>,
    pub missing_versions: Findings<String>,
    pub size_mismatches: Findings<SizeMismatch>,
}

impl VerifyReport {
    /// Whether the pass found nothing wrong.
    pub fn is_healthy(&self) -> bool {
        self.dangling_branches.is_empty()
            && self.dangling_parents.is_empty()
            && self.misaddressed_commits.is_empty()
            && self.missing_nodes.is_empty()
            && self.missing_versions.is_empty()
            && self.size_mismatches.is_empty()
    }

    /// Findings across every class.
    pub fn total_findings(&self) -> usize {
        self.dangling_branches.count
            + self.dangling_parents.count
            + self.misaddressed_commits.count
            + self.missing_nodes.count
            + self.missing_versions.count
            + self.size_mismatches.count
    }
}

/// Check that everything the repository's branches reference is present and the right size.
///
/// Reads only. Cost scales with the number of reachable entries: one version-store metadata probe
/// per distinct content hash, and no blob contents are read.
pub async fn verify_repo(repo: &LocalRepository) -> Result<VerifyReport, OxenError> {
    let mut report = VerifyReport::default();

    let heads = resolve_branch_heads(repo, &mut report).await?;
    check_commits(repo, &mut report).await?;
    check_reachable_objects(repo, &heads, &mut report).await?;
    check_entry_sizes(repo, &heads, &mut report).await?;

    Ok(report)
}

/// The commit each branch points at, recording the branches whose head does not resolve.
async fn resolve_branch_heads(
    repo: &LocalRepository,
    report: &mut VerifyReport,
) -> Result<Vec<Commit>, OxenError> {
    let branches = repositories::branches::list(repo).await?;
    report.branches_checked = branches.len();

    let walk_repo = repo.clone();
    let (heads, dangling) = tokio::task::spawn_blocking(
        move || -> Result<(Vec<Commit>, Vec<DanglingBranch>), OxenError> {
            let mut heads = Vec::new();
            let mut dangling = Vec::new();
            for branch in branches {
                match repositories::commits::get_by_id(&walk_repo, &branch.commit_id)? {
                    Some(commit) => heads.push(commit),
                    None => dangling.push(DanglingBranch {
                        branch: branch.name,
                        commit_id: branch.commit_id,
                    }),
                }
            }
            Ok((heads, dangling))
        },
    )
    .await??;

    for branch in dangling {
        report.dangling_branches.record(branch);
    }
    Ok(heads)
}

/// Every reachable commit hashes to the id it is filed under, and every parent it names resolves.
async fn check_commits(repo: &LocalRepository, report: &mut VerifyReport) -> Result<(), OxenError> {
    type CommitFindings = (usize, Vec<DanglingParent>, Vec<MisaddressedCommit>);

    let walk_repo = repo.clone();
    let (commits_checked, dangling, misaddressed) =
        tokio::task::spawn_blocking(move || -> Result<CommitFindings, OxenError> {
            let commits = repositories::commits::list_all(&walk_repo)?;
            let mut dangling = Vec::new();
            let mut misaddressed = Vec::new();

            for commit in &commits {
                if let Some(finding) = misaddressed_commit(commit)? {
                    misaddressed.push(finding);
                }
                for parent_id in &commit.parent_ids {
                    let resolves = match parent_id.parse::<MerkleHash>() {
                        Ok(hash) => {
                            repositories::commits::get_by_hash(&walk_repo, &hash)?.is_some()
                        }
                        Err(_) => false,
                    };
                    if !resolves {
                        dangling.push(DanglingParent {
                            commit_id: commit.id.clone(),
                            parent_id: parent_id.clone(),
                        });
                    }
                }
            }
            Ok((commits.len(), dangling, misaddressed))
        })
        .await??;

    report.commits_checked = commits_checked;
    for parent in dangling {
        report.dangling_parents.record(parent);
    }
    for commit in misaddressed {
        report.misaddressed_commits.record(commit);
    }
    Ok(())
}

/// A commit's own fields, re-hashed, against the id it is filed under.
///
/// Calls the function the commit writer uses, so the check cannot drift from the rule it enforces.
fn misaddressed_commit(commit: &Commit) -> Result<Option<MisaddressedCommit>, OxenError> {
    let computed = compute_commit_id(&NewCommit {
        parent_ids: commit.parent_ids.clone(),
        message: commit.message.clone(),
        author: commit.author.clone(),
        email: commit.email.clone(),
        timestamp: commit.timestamp,
    })?;

    if computed.to_string() == commit.id {
        return Ok(None);
    }
    Ok(Some(MisaddressedCommit {
        recorded_id: commit.id.clone(),
        computed_id: computed.to_string(),
    }))
}

/// Merkle nodes and version blobs the branch trees reference but the repository does not hold.
async fn check_reachable_objects(
    repo: &LocalRepository,
    heads: &[Commit],
    report: &mut VerifyReport,
) -> Result<(), OxenError> {
    let mut seen_nodes = HashSet::new();
    let mut seen_versions = HashSet::new();

    for head in heads {
        // `None` as the base widens the walk from a delta to the whole tree.
        let missing = repositories::tree::find_missing_added_objects(repo, None, head).await?;
        for node in missing.nodes {
            if seen_nodes.insert(node.clone()) {
                report.missing_nodes.record(node);
            }
        }
        for version in missing.versions {
            if seen_versions.insert(version.clone()) {
                report.missing_versions.record(version);
            }
        }
    }
    Ok(())
}

/// Stored blob size against the size each file node declares.
async fn check_entry_sizes(
    repo: &LocalRepository,
    heads: &[Commit],
    report: &mut VerifyReport,
) -> Result<(), OxenError> {
    let version_store = repo.version_store();
    let walk_repo = repo.clone();
    let heads = heads.to_vec();
    let (entries_checked, entries) =
        tokio::task::spawn_blocking(move || -> Result<(usize, Vec<CommitEntry>), OxenError> {
            let mut probed = HashSet::new();
            let mut entries = Vec::new();
            let mut entries_checked = 0;

            for head in &heads {
                // A head whose tree is absent has no entries to size, and the missing node is
                // already reported.
                if repositories::tree::get_root_with_children(&walk_repo, head)?.is_none() {
                    continue;
                }
                for entry in repositories::entries::list_for_commit(&walk_repo, head)? {
                    entries_checked += 1;
                    if probed.insert(entry.hash.clone()) {
                        entries.push(entry);
                    }
                }
            }
            Ok((entries_checked, entries))
        })
        .await??;

    report.entries_checked = entries_checked;

    let max_concurrent = MAX_CONCURRENT_SIZE_PROBES.min(entries.len().max(1));
    let mut probes = futures_util::stream::iter(entries)
        .map(|entry| {
            let version_store = version_store.clone();
            async move {
                // An absent blob has no size and is already reported as a missing version.
                let stored = version_store.get_version_size(&entry.hash).await.ok();
                (entry, stored)
            }
        })
        .buffer_unordered(max_concurrent);

    while let Some((entry, stored)) = probes.next().await {
        let Some(stored_bytes) = stored else { continue };
        if stored_bytes != entry.num_bytes {
            report.size_mismatches.record(SizeMismatch {
                hash: entry.hash,
                path: entry.path,
                declared_bytes: entry.num_bytes,
                stored_bytes,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test;
    use crate::util;

    /// The single stored version blob under `dir`, for tests that corrupt one deliberately.
    fn find_only_version_blob(dir: &std::path::Path) -> Result<PathBuf, OxenError> {
        let mut found = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(path) = stack.pop() {
            for entry in std::fs::read_dir(&path)? {
                let entry = entry?;
                let entry_path = entry.path();
                if entry.metadata()?.is_dir() {
                    stack.push(entry_path);
                } else if entry_path.file_name().is_some_and(|name| name == "data") {
                    found.push(entry_path);
                }
            }
        }
        match found.len() {
            1 => Ok(found.remove(0)),
            n => Err(OxenError::basic_str(format!(
                "expected exactly one version blob, found {n}"
            ))),
        }
    }

    #[tokio::test]
    async fn test_a_sound_repository_reports_no_problems() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let path = repo.path.join("a.txt");
            util::fs::write_to_path(&path, "alpha")?;
            repositories::add(&repo, &path).await?;
            repositories::commit(&repo, "Add a")?;

            let report = verify_repo(&repo).await?;

            assert!(report.is_healthy(), "a sound repository has no findings");
            assert_eq!(report.branches_checked, 1);
            assert_eq!(report.entries_checked, 1);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_an_absent_version_blob_is_reported_missing() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let path = repo.path.join("a.txt");
            util::fs::write_to_path(&path, "alpha")?;
            repositories::add(&repo, &path).await?;
            let commit = repositories::commit(&repo, "Add a")?;

            let entries = repositories::entries::list_for_commit(&repo, &commit)?;
            let hash = entries.first().expect("one entry").hash.clone();
            repo.version_store().delete_version(&hash).await?;

            let report = verify_repo(&repo).await?;

            assert_eq!(report.missing_versions.count, 1);
            assert_eq!(report.missing_versions.sample, vec![hash]);
            assert_eq!(
                report.size_mismatches.count, 0,
                "an absent blob is missing, not the wrong size"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_a_blob_whose_size_disagrees_with_the_tree_is_reported() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let path = repo.path.join("a.txt");
            util::fs::write_to_path(&path, "the original contents")?;
            repositories::add(&repo, &path).await?;
            let commit = repositories::commit(&repo, "Add a")?;

            let entries = repositories::entries::list_for_commit(&repo, &commit)?;
            let entry = entries.first().expect("one entry").clone();

            // Shorten the stored blob, leaving the tree's declared size alone. Written straight
            // to the file because `store_version` verifies content against the hash, which is
            // exactly the guarantee this condition violates.
            let blob = find_only_version_blob(&repo.path.join(".oxen").join("versions"))?;
            std::fs::write(&blob, b"short")?;

            let report = verify_repo(&repo).await?;

            assert_eq!(report.size_mismatches.count, 1);
            let mismatch = &report.size_mismatches.sample[0];
            assert_eq!(mismatch.declared_bytes, entry.num_bytes);
            assert_eq!(mismatch.stored_bytes, 5);
            assert_eq!(
                report.missing_versions.count, 0,
                "a present blob is the wrong size, not missing"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_a_commit_that_does_not_hash_to_its_id_is_reported() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let path = repo.path.join("a.txt");
            util::fs::write_to_path(&path, "alpha")?;
            repositories::add(&repo, &path).await?;
            let commit = repositories::commit(&repo, "Add a")?;

            // The id is the hash of the commit's own fields, so altering one leaves the commit
            // filed under an id its contents no longer produce.
            let tampered = Commit {
                message: "Add a, but edited afterwards".to_string(),
                ..commit.clone()
            };
            assert!(
                misaddressed_commit(&tampered)?.is_some(),
                "a commit whose fields changed no longer hashes to its id"
            );
            assert!(
                misaddressed_commit(&commit)?.is_none(),
                "an untouched commit hashes to its id"
            );

            let report = verify_repo(&repo).await?;
            assert_eq!(
                report.misaddressed_commits.count, 0,
                "the repository's own commit is sound"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_a_branch_head_naming_an_absent_commit_is_reported() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let path = repo.path.join("a.txt");
            util::fs::write_to_path(&path, "alpha")?;
            repositories::add(&repo, &path).await?;
            let commit = repositories::commit(&repo, "Add a")?;

            repo.merkle_node_store().delete(&commit.hash()?)?;

            let report = verify_repo(&repo).await?;

            assert_eq!(report.dangling_branches.count, 1);
            assert_eq!(report.dangling_branches.sample[0].commit_id, commit.id);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_a_commit_naming_an_absent_parent_is_reported() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let first_path = repo.path.join("a.txt");
            util::fs::write_to_path(&first_path, "alpha")?;
            repositories::add(&repo, &first_path).await?;
            let first = repositories::commit(&repo, "Add a")?;

            let second_path = repo.path.join("b.txt");
            util::fs::write_to_path(&second_path, "beta")?;
            repositories::add(&repo, &second_path).await?;
            let second = repositories::commit(&repo, "Add b")?;

            repo.merkle_node_store().delete(&first.hash()?)?;

            let report = verify_repo(&repo).await?;

            assert_eq!(report.dangling_parents.count, 1);
            let dangling = &report.dangling_parents.sample[0];
            assert_eq!(dangling.commit_id, second.id);
            assert_eq!(dangling.parent_id, first.id);
            assert_eq!(
                report.dangling_branches.count, 0,
                "the branch head itself still resolves"
            );

            Ok(())
        })
        .await
    }
}
