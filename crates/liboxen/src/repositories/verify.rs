//! # oxen verify
//!
//! Read-only integrity checks for a repository.
//!
//! Answers one question: does everything this repository's commits reference actually exist, and
//! does it look like what the tree says it is. Nothing here mutates a repository.
//!
//! Findings are reported as counts plus a bounded sample.

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, MerkleHash, NewCommit};
use crate::repositories;
use crate::repositories::commits::commit_writer::compute_commit_id;
use crate::repositories::tree::AddedFile;

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
    pub versions_checked: usize,
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

/// Check that everything the repository's commits reference is present and the right size.
///
/// Reads only. Cost scales with the repository's distinct object count, which is also held in
/// memory: no blob contents are read, and each distinct content hash costs at most one
/// version-store existence probe and one metadata probe.
pub async fn verify_repo(repo: &LocalRepository) -> Result<VerifyReport, OxenError> {
    let mut report = VerifyReport::default();

    check_branch_heads(repo, &mut report).await?;
    let commits = check_commits(repo, &mut report).await?;
    let versions = check_tree_nodes(repo, &commits, &mut report).await?;
    check_versions(repo, versions, &mut report).await?;

    Ok(report)
}

/// Every branch head names a commit the repository holds.
async fn check_branch_heads(
    repo: &LocalRepository,
    report: &mut VerifyReport,
) -> Result<(), OxenError> {
    let branches = repositories::branches::list(repo).await?;
    report.branches_checked = branches.len();

    let walk_repo = repo.clone();
    let dangling =
        tokio::task::spawn_blocking(move || -> Result<Findings<DanglingBranch>, OxenError> {
            let mut dangling = Findings::default();
            for branch in branches {
                if repositories::commits::get_by_id(&walk_repo, &branch.commit_id)?.is_none() {
                    dangling.record(DanglingBranch {
                        branch: branch.name,
                        commit_id: branch.commit_id,
                    });
                }
            }
            Ok(dangling)
        })
        .await??;

    report.dangling_branches = dangling;
    Ok(())
}

/// Every reachable commit hashes to the id it is filed under, and every parent it names resolves.
async fn check_commits(
    repo: &LocalRepository,
    report: &mut VerifyReport,
) -> Result<Vec<Commit>, OxenError> {
    type CommitFindings = (
        Vec<Commit>,
        Findings<DanglingParent>,
        Findings<MisaddressedCommit>,
    );

    let walk_repo = repo.clone();
    let (commits, dangling, misaddressed) =
        tokio::task::spawn_blocking(move || -> Result<CommitFindings, OxenError> {
            let commits: Vec<Commit> = repositories::commits::list_all(&walk_repo)?
                .into_iter()
                .collect();
            let mut dangling = Findings::default();
            let mut misaddressed = Findings::default();

            for commit in &commits {
                if let Some(finding) = misaddressed_commit(commit)? {
                    misaddressed.record(finding);
                }
                for parent_id in &commit.parent_ids {
                    let resolves = match parent_id.parse::<MerkleHash>() {
                        Ok(hash) => {
                            repositories::commits::get_by_hash(&walk_repo, &hash)?.is_some()
                        }
                        Err(_) => false,
                    };
                    if !resolves {
                        dangling.record(DanglingParent {
                            commit_id: commit.id.clone(),
                            parent_id: parent_id.clone(),
                        });
                    }
                }
            }
            Ok((commits, dangling, misaddressed))
        })
        .await??;

    report.commits_checked = commits.len();
    report.dangling_parents = dangling;
    report.misaddressed_commits = misaddressed;
    Ok(commits)
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

/// Merkle nodes the repository's commits reference but do not hold, returning the version blobs
/// those commits reference so the store can be checked once for the whole set.
///
/// Every commit is covered, not only the branch heads, so a blob whose file was deleted in a later
/// commit stays in scope for the commit that still has it. Each commit is walked as a delta against
/// its first parent, and a commit whose parent the repository lacks is walked whole, so the total
/// cost is the repository's distinct object count rather than the sum of every tree.
async fn check_tree_nodes(
    repo: &LocalRepository,
    commits: &[Commit],
    report: &mut VerifyReport,
) -> Result<HashMap<String, AddedFile>, OxenError> {
    type TreeFindings = (HashMap<String, AddedFile>, Findings<String>);

    // Sync core: the merkle walk and the node-existence probes are sync DB IO, one blocking unit.
    let walk_repo = repo.clone();
    let commits = commits.to_vec();
    let (versions, missing_nodes) =
        tokio::task::spawn_blocking(move || -> Result<TreeFindings, OxenError> {
            let mut by_hash: HashMap<MerkleHash, &Commit> = HashMap::new();
            for commit in &commits {
                by_hash.insert(commit.hash()?, commit);
            }

            let store = walk_repo.merkle_node_store();
            let mut versions: HashMap<String, AddedFile> = HashMap::new();
            let mut seen_nodes: HashSet<MerkleHash> = HashSet::new();
            let mut missing_nodes = Findings::default();

            for commit in &commits {
                // Parents resolve by the rule the dangling-parent check uses, so a parent that
                // check reports as present is one this walk takes a delta against.
                let base = commit
                    .parent_ids
                    .first()
                    .and_then(|id| id.parse::<MerkleHash>().ok())
                    .and_then(|hash| by_hash.get(&hash).copied());
                let Some(added) = repositories::tree::added_objects(&walk_repo, base, commit)?
                else {
                    // No root directory node means the commit's tree was never written.
                    missing_nodes.record(commit.hash()?.to_string());
                    continue;
                };

                for hash in added.nodes {
                    if seen_nodes.insert(hash) && !store.exists(&hash)? {
                        missing_nodes.record(hash.to_string());
                    }
                }
                for (hash, file) in added.versions {
                    versions.entry(hash).or_insert(file);
                }
            }
            Ok((versions, missing_nodes))
        })
        .await??;

    report.versions_checked = versions.len();
    report.missing_nodes = missing_nodes;
    Ok(versions)
}

/// Version blobs the repository does not hold, and blobs whose stored size disagrees with the size
/// the file node referencing them declares.
async fn check_versions(
    repo: &LocalRepository,
    versions: HashMap<String, AddedFile>,
    report: &mut VerifyReport,
) -> Result<(), OxenError> {
    let version_store = repo.version_store();

    let hashes: Vec<String> = versions.keys().cloned().collect();
    let missing: HashSet<String> = version_store
        .find_missing_versions(&hashes)
        .await?
        .into_iter()
        .collect();
    for hash in &missing {
        report.missing_versions.record(hash.clone());
    }

    // An absent blob has no size to disagree with, and is already reported.
    let present: Vec<(String, AddedFile)> = versions
        .into_iter()
        .filter(|(hash, _)| !missing.contains(hash))
        .collect();

    let max_concurrent = MAX_CONCURRENT_SIZE_PROBES.min(present.len().max(1));
    let mut probes = futures_util::stream::iter(present)
        .map(|(hash, file)| {
            let version_store = version_store.clone();
            async move {
                let stored = version_store.get_version_size(&hash).await.ok();
                (hash, file, stored)
            }
        })
        .buffer_unordered(max_concurrent);

    while let Some((hash, file, stored)) = probes.next().await {
        let Some(stored_bytes) = stored else { continue };
        if stored_bytes != file.num_bytes {
            report.size_mismatches.record(SizeMismatch {
                hash,
                path: file.path,
                declared_bytes: file.num_bytes,
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
    use std::path::Path;

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

    #[test]
    fn test_findings_count_everything_but_keep_a_bounded_sample() {
        let mut findings = Findings::default();
        for i in 0..(SAMPLE_LIMIT * 3) {
            findings.record(i);
        }

        assert_eq!(findings.count, SAMPLE_LIMIT * 3, "every finding is counted");
        assert_eq!(
            findings.sample.len(),
            SAMPLE_LIMIT,
            "a badly damaged repository does not grow the report"
        );
        assert_eq!(
            findings.sample,
            (0..SAMPLE_LIMIT).collect::<Vec<_>>(),
            "the sample is the first findings seen"
        );
    }

    #[tokio::test]
    async fn test_a_blob_only_older_commits_reference_is_still_checked() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let doomed = repo.path.join("doomed.txt");
            util::fs::write_to_path(&doomed, "only in the first commit")?;
            repositories::add(&repo, &doomed).await?;
            let first = repositories::commit(&repo, "Add doomed")?;

            let entries = repositories::entries::list_for_commit(&repo, &first)?;
            let doomed_hash = entries.first().expect("one entry").hash.clone();

            // Remove it, so the branch head's tree no longer references the blob but history does.
            repositories::rm(
                &repo,
                &crate::opts::RmOpts::from_path(Path::new("doomed.txt")),
            )
            .await?;
            repositories::commit(&repo, "Remove doomed")?;

            repo.version_store().delete_version(&doomed_hash).await?;

            let report = verify_repo(&repo).await?;

            assert_eq!(
                report.missing_versions.count, 1,
                "a blob an older commit still references is missing: {report:?}"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_the_size_of_a_blob_only_older_commits_reference_is_checked()
    -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let nested = repo.path.join("nested");
            util::fs::create_dir_all(&nested)?;
            let doomed = nested.join("doomed.txt");
            util::fs::write_to_path(&doomed, "only in the first commit")?;
            repositories::add(&repo, &doomed).await?;
            let first = repositories::commit(&repo, "Add doomed")?;

            let entries = repositories::entries::list_for_commit(&repo, &first)?;
            let declared_bytes = entries.first().expect("one entry").num_bytes;

            // Remove it, so the branch head's tree no longer references the blob but history does.
            repositories::rm(
                &repo,
                &crate::opts::RmOpts::from_path(Path::new("nested/doomed.txt")),
            )
            .await?;
            repositories::commit(&repo, "Remove doomed")?;

            let blob = find_only_version_blob(&repo.path.join(".oxen").join("versions"))?;
            std::fs::write(&blob, b"short")?;

            let report = verify_repo(&repo).await?;

            assert_eq!(
                report.size_mismatches.count, 1,
                "a blob an older commit still references is sized: {report:?}"
            );
            let mismatch = &report.size_mismatches.sample[0];
            assert_eq!(mismatch.declared_bytes, declared_bytes);
            assert_eq!(mismatch.stored_bytes, 5);
            assert_eq!(
                mismatch.path,
                PathBuf::from("nested").join("doomed.txt"),
                "the path is the one the tree records, not the blob's storage path"
            );

            Ok(())
        })
        .await
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
            assert_eq!(report.versions_checked, 1);

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
