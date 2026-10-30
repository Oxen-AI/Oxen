//! # oxen verify
//!
//! Read-only integrity checks for a repository.
//!
//! Answers one question: does everything this repository's commits and workspaces reference
//! actually exist, and does it look like what the tree says it is. Nothing here mutates a
//! repository.
//!
//! Findings are reported as counts plus a bounded sample.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use crate::core::db::dir_hashes::dir_hashes_db::dir_hash_db_path;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, MerkleHash, NewCommit};
use crate::repositories::branches;
use crate::repositories::commits;
use crate::repositories::commits::commit_writer::compute_commit_id;
use crate::repositories::tree::{DeclaredSizes, added_objects};
use crate::repositories::workspaces;

/// Examples of each finding a report carries alongside the count.
const SAMPLE_LIMIT: usize = 10;

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

/// A workspace pinned to a commit the repository does not have.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DanglingWorkspace {
    /// The workspace's own id when its config records one, else the directory it lives in.
    pub workspace: String,
    pub name: Option<String>,
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

/// A merkle node the repository holds but cannot read, leaving whatever it contains unexamined.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnreadableNode {
    pub hash: String,
    pub error: String,
}

/// A version blob whose metadata probe failed, leaving its size neither confirmed nor refuted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UncheckedVersion {
    pub hash: String,
    pub error: String,
}

/// What a [`verify_repo`] pass found.
///
/// Travels over the wire, so a class absent from the JSON reads as empty: that means the sender
/// did not report it, not that it looked and found nothing.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct VerifyReport {
    pub branches_checked: usize,
    pub commits_checked: usize,
    pub versions_checked: usize,
    pub workspaces_checked: usize,
    pub dangling_branches: Findings<DanglingBranch>,
    pub dangling_workspaces: Findings<DanglingWorkspace>,
    pub dangling_parents: Findings<DanglingParent>,
    pub misaddressed_commits: Findings<MisaddressedCommit>,
    /// Commits whose directory index, which resolves the tree by path, is absent.
    pub missing_dir_hash_indexes: Findings<String>,
    pub missing_nodes: Findings<String>,
    pub unreadable_nodes: Findings<UnreadableNode>,
    pub missing_versions: Findings<String>,
    pub size_mismatches: Findings<SizeMismatch>,
    pub unchecked_versions: Findings<UncheckedVersion>,
    /// Nodes already counted in `unreadable_nodes`, however many references reach each one.
    #[serde(skip)]
    unreadable_seen: HashSet<MerkleHash>,
}

impl VerifyReport {
    /// Whether the pass found nothing wrong.
    pub fn is_healthy(&self) -> bool {
        self.dangling_branches.is_empty()
            && self.dangling_workspaces.is_empty()
            && self.dangling_parents.is_empty()
            && self.misaddressed_commits.is_empty()
            && self.missing_dir_hash_indexes.is_empty()
            && self.missing_nodes.is_empty()
            && self.unreadable_nodes.is_empty()
            && self.missing_versions.is_empty()
            && self.size_mismatches.is_empty()
            && self.unchecked_versions.is_empty()
    }

    /// Findings across every class.
    pub fn total_findings(&self) -> usize {
        self.dangling_branches.count
            + self.dangling_workspaces.count
            + self.dangling_parents.count
            + self.misaddressed_commits.count
            + self.missing_dir_hash_indexes.count
            + self.missing_nodes.count
            + self.unreadable_nodes.count
            + self.missing_versions.count
            + self.size_mismatches.count
            + self.unchecked_versions.count
    }

    /// Record nodes that could not be read, once per node.
    fn record_unreadable(&mut self, nodes: Vec<(MerkleHash, String)>) {
        for (hash, error) in nodes {
            if self.unreadable_seen.insert(hash) {
                self.unreadable_nodes.record(UnreadableNode {
                    hash: hash.to_string(),
                    error,
                });
            }
        }
    }
}

/// Check that everything the repository's commits and workspaces reference is present and the
/// right size.
///
/// Reads only. Cost scales with the repository's distinct object count, which is also held in
/// memory: no blob contents are read, and each distinct content hash costs one version-store
/// metadata probe however many paths reference it.
pub async fn verify_repo(repo: &LocalRepository) -> Result<VerifyReport, OxenError> {
    let mut report = VerifyReport::default();

    let commits = check_commits(repo, &mut report).await?;
    check_workspace_pins(repo, &mut report).await?;
    let versions = check_tree_nodes(repo, commits, &mut report).await?;
    check_versions(repo, versions, &mut report).await?;

    Ok(report)
}

/// Every workspace's pinned commit resolves.
///
/// A workspace pins the commit it was created against, independently of where any branch points.
/// The commit walk covers branch ancestry only.
async fn check_workspace_pins(
    repo: &LocalRepository,
    report: &mut VerifyReport,
) -> Result<(), OxenError> {
    type WorkspaceFindings = (
        usize,
        Findings<DanglingWorkspace>,
        Vec<(MerkleHash, String)>,
    );

    // Sync core: listing workspace dirs, reading each config, and resolving each pin is file and
    // DB IO, one blocking unit.
    let walk_repo = repo.clone();
    let (checked, dangling, unreadable) =
        tokio::task::spawn_blocking(move || -> Result<WorkspaceFindings, OxenError> {
            let mut checked = 0;
            let mut dangling = Findings::default();
            let mut unreadable = Vec::new();

            for dir in workspaces::list_dirs(&walk_repo)? {
                // A directory holding no config is not a workspace.
                let Some(config) = workspaces::read_config(&dir)? else {
                    continue;
                };
                checked += 1;

                if let Ok(hash) = config.workspace_commit_id.parse::<MerkleHash>() {
                    match commits::get_by_hash(&walk_repo, &hash) {
                        Ok(Some(_)) => continue,
                        Ok(None) => {}
                        Err(err) => {
                            unreadable.push((hash, err.to_string()));
                            continue;
                        }
                    }
                }

                let workspace = config.workspace_id.unwrap_or_else(|| {
                    dir.file_name().map_or_else(
                        || dir.display().to_string(),
                        |name| name.to_string_lossy().into_owned(),
                    )
                });
                dangling.record(DanglingWorkspace {
                    workspace,
                    name: config.workspace_name,
                    commit_id: config.workspace_commit_id,
                });
            }

            Ok((checked, dangling, unreadable))
        })
        .await??;

    report.workspaces_checked = checked;
    report.dangling_workspaces = dangling;
    report.record_unreadable(unreadable);
    Ok(())
}

/// Every branch head points at a commit the repository holds, and every commit reachable from one
/// hashes to the id it is filed under, has its directory index, and lists parents that resolve.
///
/// A commit that is present but cannot be read is reported, and its ancestry is not walked.
async fn check_commits(
    repo: &LocalRepository,
    report: &mut VerifyReport,
) -> Result<Vec<Commit>, OxenError> {
    #[derive(Default)]
    struct CommitFindings {
        commits: Vec<Commit>,
        dangling_branches: Findings<DanglingBranch>,
        dangling_parents: Findings<DanglingParent>,
        misaddressed: Findings<MisaddressedCommit>,
        missing_dir_hash_indexes: Findings<String>,
        unreadable: Vec<(MerkleHash, String)>,
    }

    /// What looking up a commit id found.
    enum CommitLookup {
        /// Read for the first time.
        New(Commit),
        /// Already read, or unreadable and already recorded.
        Handled,
        /// The repository holds no commit under the id.
        Missing,
    }

    let branches = branches::list(repo).await?;
    report.branches_checked = branches.len();

    // Sync core: every commit lookup is a node-store read, one blocking unit.
    let walk_repo = repo.clone();
    let found = tokio::task::spawn_blocking(move || -> Result<CommitFindings, OxenError> {
        let mut found = CommitFindings::default();
        // Commits already read or reported unreadable, so each is looked up once.
        let mut seen = HashSet::new();
        let mut pending = Vec::new();

        let mut resolve = |id: &str| {
            let Ok(hash) = id.parse::<MerkleHash>() else {
                return CommitLookup::Missing;
            };
            if seen.contains(&hash) {
                return CommitLookup::Handled;
            }
            match commits::get_by_hash(&walk_repo, &hash) {
                Ok(Some(commit)) => {
                    seen.insert(hash);
                    CommitLookup::New(commit)
                }
                Ok(None) => CommitLookup::Missing,
                Err(err) => {
                    seen.insert(hash);
                    found.unreadable.push((hash, err.to_string()));
                    CommitLookup::Handled
                }
            }
        };

        for branch in branches {
            match resolve(&branch.commit_id) {
                CommitLookup::New(commit) => pending.push(commit),
                CommitLookup::Handled => {}
                CommitLookup::Missing => found.dangling_branches.record(DanglingBranch {
                    branch: branch.name,
                    commit_id: branch.commit_id,
                }),
            }
        }

        while let Some(commit) = pending.pop() {
            if let Some(finding) = misaddressed_commit(&commit)? {
                found.misaddressed.record(finding);
            }
            if !dir_hash_db_path(&walk_repo, &commit).exists() {
                found.missing_dir_hash_indexes.record(commit.id.clone());
            }
            for parent_id in &commit.parent_ids {
                match resolve(parent_id) {
                    CommitLookup::New(parent) => pending.push(parent),
                    CommitLookup::Handled => {}
                    CommitLookup::Missing => found.dangling_parents.record(DanglingParent {
                        commit_id: commit.id.clone(),
                        parent_id: parent_id.clone(),
                    }),
                }
            }
            found.commits.push(commit);
        }
        Ok(found)
    })
    .await??;

    report.commits_checked = found.commits.len();
    report.dangling_branches = found.dangling_branches;
    report.dangling_parents = found.dangling_parents;
    report.misaddressed_commits = found.misaddressed;
    report.missing_dir_hash_indexes = found.missing_dir_hash_indexes;
    report.record_unreadable(found.unreadable);
    Ok(found.commits)
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
    commits: Vec<Commit>,
    report: &mut VerifyReport,
) -> Result<HashMap<String, DeclaredSizes>, OxenError> {
    type TreeFindings = (
        HashMap<String, DeclaredSizes>,
        Findings<String>,
        Vec<(MerkleHash, String)>,
    );

    // Sync core: the merkle walk and the node-existence probes are sync DB IO, one blocking unit.
    let walk_repo = repo.clone();
    let (versions, missing_nodes, unreadable) =
        tokio::task::spawn_blocking(move || -> Result<TreeFindings, OxenError> {
            let mut by_hash: HashMap<MerkleHash, &Commit> = HashMap::new();
            for commit in &commits {
                by_hash.insert(commit.hash()?, commit);
            }

            let store = walk_repo.merkle_node_store();
            let mut versions: HashMap<String, DeclaredSizes> = HashMap::new();
            let mut seen_nodes: HashSet<MerkleHash> = HashSet::new();
            let mut missing_nodes = Findings::default();
            let mut unreadable = Vec::new();

            for commit in &commits {
                // Parents resolve by the rule the dangling-parent check uses, so a parent that
                // check reports as present is one this walk takes a delta against.
                let base = commit
                    .parent_ids
                    .first()
                    .and_then(|id| id.parse::<MerkleHash>().ok())
                    .and_then(|hash| by_hash.get(&hash).copied());
                let added =
                    match added_objects::<HashMap<String, DeclaredSizes>>(&walk_repo, base, commit)
                    {
                        Ok(Some(added)) => added,
                        Ok(None) => {
                            // No root directory node means the commit's tree was never written.
                            missing_nodes.record(commit.hash()?.to_string());
                            continue;
                        }
                        // The commit's children did not parse, so its tree cannot be entered at all.
                        Err(err) => {
                            unreadable.push((commit.hash()?, err.to_string()));
                            continue;
                        }
                    };

                unreadable.extend(added.unreadable);

                for hash in added.nodes {
                    if seen_nodes.insert(hash) && !store.exists(&hash)? {
                        missing_nodes.record(hash.to_string());
                    }
                }
                for (hash, declared) in added.versions {
                    versions.entry(hash).or_default().extend(declared);
                }
            }
            Ok((versions, missing_nodes, unreadable))
        })
        .await??;

    report.versions_checked = versions.len();
    report.missing_nodes = missing_nodes;
    report.record_unreadable(unreadable);
    Ok(versions)
}

/// Version blobs the repository does not hold, blobs whose stored size disagrees with a size a
/// file node declares for them, and blobs whose size could not be established.
///
/// One metadata probe per blob answers all three, so a blob referenced from a thousand paths costs
/// one round trip and its stored size is compared against every size declared for it. A probe that
/// fails for any reason other than the blob being absent is reported rather than passed over, so a
/// pass that could not reach part of the store never reads as a clean one.
async fn check_versions(
    repo: &LocalRepository,
    mut versions: HashMap<String, DeclaredSizes>,
    report: &mut VerifyReport,
) -> Result<(), OxenError> {
    let hashes = versions.keys().cloned().collect();
    let sizes = repo.version_store().get_version_sizes(hashes).await?;

    for (hash, stored) in sizes {
        let declared = versions.remove(&hash).unwrap_or_default();
        let stored_bytes = match stored {
            Ok(stored_bytes) => stored_bytes,
            Err(OxenError::VersionStoreBlobMissing { .. }) => {
                report.missing_versions.record(hash);
                continue;
            }
            Err(err) => {
                report.unchecked_versions.record(UncheckedVersion {
                    hash,
                    error: err.to_string(),
                });
                continue;
            }
        };

        for (declared_bytes, path) in declared {
            if stored_bytes != declared_bytes {
                report.size_mismatches.record(SizeMismatch {
                    hash: hash.clone(),
                    path,
                    declared_bytes,
                    stored_bytes,
                });
            }
        }
    }

    // A blob the store answered nothing for was never checked.
    for hash in versions.into_keys() {
        report.unchecked_versions.record(UncheckedVersion {
            hash,
            error: "the version store returned no size for this blob".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::db::dir_hashes::dir_hashes_db::remove_from_cache_with_children;
    use crate::core::db::merkle_node::MerkleNodeDB;
    use crate::model::Workspace;
    use crate::model::merkle_tree::node::{CommitNode, EMerkleTreeNode, MerkleTreeNode};
    use crate::repositories;
    use crate::test;
    use crate::util;
    use bytes::Bytes;
    use std::path::Path;

    /// Store bytes under `hash` that do not parse as a merkle node.
    fn write_unparseable_node(repo: &LocalRepository, hash: MerkleHash) -> Result<(), OxenError> {
        repo.merkle_node_store().write_nodes(
            vec![(
                hash,
                Bytes::from_static(b"not a node"),
                Bytes::from_static(b"not children"),
            )],
            true,
        )?;
        Ok(())
    }

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

    #[test]
    fn test_a_report_survives_skew_with_a_server_of_another_version() {
        let mut report = VerifyReport {
            versions_checked: 7,
            ..Default::default()
        };
        report.missing_versions.record("abc123".to_string());

        let mut json = serde_json::to_value(&report).expect("a report serializes");
        let fields = json.as_object_mut().expect("a report is a JSON object");
        // A server older than this client omits a class it never had.
        fields
            .remove("unchecked_versions")
            .expect("the class was there to remove");
        // A server newer than this client sends a class this client has never heard of.
        fields.insert(
            "findings_from_a_later_version".to_string(),
            serde_json::json!({ "count": 1, "sample": [] }),
        );

        let parsed: VerifyReport =
            serde_json::from_value(json).expect("a report from either side still parses");

        assert_eq!(parsed.versions_checked, 7, "known counters survive");
        assert_eq!(parsed.missing_versions.count, 1, "known findings survive");
        assert!(
            parsed.unchecked_versions.is_empty(),
            "an omitted class reads as unreported"
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
            let commit = repositories::commit(&repo, "Add a")?;

            let report = verify_repo(&repo).await?;

            assert!(report.is_healthy(), "a sound repository has no findings");
            assert_eq!(report.branches_checked, 1);
            assert_eq!(report.versions_checked, 1);

            let index = dir_hash_db_path(&repo, &commit);
            remove_from_cache_with_children(&index)?;
            util::fs::remove_dir_all(&index)?;
            let report = verify_repo(&repo).await?;

            assert_eq!(
                report.missing_dir_hash_indexes.sample,
                vec![commit.id.clone()],
                "a commit whose directory index is gone is a finding: {report:?}"
            );
            assert_eq!(report.total_findings(), 1, "{report:?}");

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_a_missing_nested_directory_node_is_reported_not_raised() -> Result<(), OxenError>
    {
        test::run_empty_local_repo_test_async(|repo| async move {
            let nested = repo.path.join("nested");
            util::fs::create_dir_all(&nested)?;
            util::fs::write_to_path(nested.join("a.txt"), "alpha")?;
            repositories::add(&repo, &nested).await?;
            let commit = repositories::commit(&repo, "Add nested")?;

            let dir = repositories::tree::get_dir_with_children(&repo, &commit, "nested", None)?
                .expect("the nested directory has a node");
            repo.merkle_node_store().delete(&dir.hash)?;

            let report = verify_repo(&repo).await?;

            assert_eq!(
                report.missing_nodes.count, 1,
                "the absent directory node is a finding: {report:?}"
            );
            assert_eq!(report.missing_nodes.sample, vec![dir.hash.to_string()]);
            assert!(!report.is_healthy());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_an_unreadable_node_is_reported_and_the_rest_still_checked()
    -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let nested = repo.path.join("nested");
            util::fs::create_dir_all(&nested)?;
            util::fs::write_to_path(nested.join("buried.txt"), "buried")?;
            util::fs::write_to_path(repo.path.join("top.txt"), "top")?;
            repositories::add(&repo, &repo.path).await?;
            let commit = repositories::commit(&repo, "Add both")?;

            let dir = repositories::tree::get_dir_with_children(&repo, &commit, "nested", None)?
                .expect("the nested directory has a node");

            // Present but unparseable, which is the case a missing-node check cannot see.
            write_unparseable_node(&repo, dir.hash)?;

            let report = verify_repo(&repo).await?;

            assert_eq!(
                report.unreadable_nodes.count, 1,
                "the unreadable node is a finding: {report:?}"
            );
            assert_eq!(report.unreadable_nodes.sample[0].hash, dir.hash.to_string());
            assert_eq!(
                report.missing_nodes.count, 0,
                "a node that is present but unreadable is not a missing one"
            );
            assert!(
                report.versions_checked >= 1,
                "only the damaged subtree is skipped, not the whole commit: {report:?}"
            );
            assert!(!report.is_healthy());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_an_unreadable_vnode_is_named_and_its_siblings_still_checked()
    -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let mut repo = repo;
            // One entry per vnode, so a single directory holds several of them.
            repo.set_vnode_size(1);
            test::populate_dir_with_txt_files(repo.path.join("nested"), "file", 4)?;
            repositories::add(&repo, &repo.path).await?;
            let commit = repositories::commit(&repo, "Add four files")?;

            let dir = repositories::tree::get_dir_with_children(&repo, &commit, "nested", None)?
                .expect("the nested directory has a node");
            let mut vnodes = Vec::new();
            for (hash, child) in MerkleTreeNode::read_children_from_hash(&repo, &dir.hash)? {
                if matches!(child.node, EMerkleTreeNode::VNode(_)) {
                    let files = MerkleTreeNode::read_children_from_hash(&repo, &hash)?.len();
                    vnodes.push((hash, files));
                }
            }
            // Entries bucket into vnodes by a hash of their path, so which vnode holds what is
            // fixed by the file names rather than chosen here.
            let (damaged, hidden) = vnodes
                .iter()
                .filter(|(_, files)| *files > 0)
                .min_by_key(|(_, files)| *files)
                .copied()
                .expect("the nested directory has a vnode holding entries");
            let survivors = vnodes.iter().map(|(_, files)| files).sum::<usize>() - hidden;
            assert!(
                survivors >= 1,
                "the fixture needs entries outside the damaged vnode: {vnodes:?}"
            );

            write_unparseable_node(&repo, damaged)?;

            let report = verify_repo(&repo).await?;

            assert_eq!(
                report.unreadable_nodes.count, 1,
                "the unreadable vnode is a finding: {report:?}"
            );
            assert_eq!(
                report.unreadable_nodes.sample[0].hash,
                damaged.to_string(),
                "the finding names the vnode that could not be read, not its parent directory"
            );
            assert_eq!(
                report.versions_checked, survivors,
                "every blob outside the damaged vnode is still checked: {report:?}"
            );
            assert!(!report.is_healthy());

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

    /// Unix only: the induced failure is a non-directory path component, which Windows reports as
    /// not-found and so classifies as an absent blob rather than an unreadable one.
    #[cfg(unix)]
    #[tokio::test]
    async fn test_a_blob_whose_size_cannot_be_read_is_not_called_healthy() -> Result<(), OxenError>
    {
        test::run_empty_local_repo_test_async(|repo| async move {
            let path = repo.path.join("a.txt");
            util::fs::write_to_path(&path, "alpha")?;
            repositories::add(&repo, &path).await?;
            let commit = repositories::commit(&repo, "Add a")?;

            let entries = repositories::entries::list_for_commit(&repo, &commit)?;
            let hash = entries.first().expect("one entry").hash.clone();

            // A regular file where the blob's directory belongs, so probing the blob inside it
            // fails with something other than "absent".
            let blob = find_only_version_blob(&repo.path.join(".oxen").join("versions"))?;
            let blob_dir = blob
                .parent()
                .expect("a blob lives in a directory")
                .to_path_buf();
            std::fs::remove_dir_all(&blob_dir)?;
            std::fs::write(&blob_dir, b"not a directory")?;

            let report = verify_repo(&repo).await?;

            assert_eq!(
                report.unchecked_versions.count, 1,
                "a blob that could not be probed is reported: {report:?}"
            );
            assert_eq!(report.unchecked_versions.sample[0].hash, hash);
            assert_eq!(
                report.missing_versions.count, 0,
                "a probe that failed is not evidence the blob is absent"
            );
            assert_eq!(
                report.size_mismatches.count, 0,
                "a size that was never read cannot disagree"
            );
            assert!(
                !report.is_healthy(),
                "a pass that could not check a blob is not a clean one"
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
    async fn test_every_size_declared_for_one_blob_is_checked() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let path = repo.path.join("a.txt");
            util::fs::write_to_path(&path, "the original contents")?;
            repositories::add(&repo, &path).await?;
            let commit = repositories::commit(&repo, "Add a")?;

            let entries = repositories::entries::list_for_commit(&repo, &commit)?;
            let entry = entries.first().expect("one entry").clone();

            // Two file nodes referencing one blob while disagreeing about its size. Identical
            // content stats to an identical length, so the declarations are built here rather
            // than committed.
            let declared = DeclaredSizes::from([
                (entry.num_bytes, PathBuf::from("a.txt")),
                (entry.num_bytes + 1, PathBuf::from("copy.txt")),
            ]);
            let versions = HashMap::from([(entry.hash.clone(), declared)]);

            let mut report = VerifyReport::default();
            check_versions(&repo, versions, &mut report).await?;

            assert_eq!(
                report.size_mismatches.count, 1,
                "only the declaration disagreeing with the stored blob is reported: {report:?}"
            );
            let mismatch = &report.size_mismatches.sample[0];
            assert_eq!(mismatch.hash, entry.hash);
            assert_eq!(mismatch.declared_bytes, entry.num_bytes + 1);
            assert_eq!(mismatch.stored_bytes, entry.num_bytes);
            assert_eq!(mismatch.path, PathBuf::from("copy.txt"));
            assert!(
                report.missing_versions.is_empty(),
                "the blob is present, so nothing is missing"
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
            let expected = misaddressed_commit(&tampered)?
                .expect("a commit whose fields changed no longer hashes to its id");
            assert!(
                misaddressed_commit(&commit)?.is_none(),
                "an untouched commit hashes to its id"
            );

            let report = verify_repo(&repo).await?;
            assert_eq!(
                report.misaddressed_commits.count, 0,
                "the repository's own commit is sound"
            );

            // Store the tampered fields under the original id, keeping the tree it points at.
            let stored = repositories::tree::get_node_by_id_with_children(&repo, &commit.hash()?)?
                .expect("the commit has a node");
            let mut commit_db = MerkleNodeDB::open_read_write(
                repo.merkle_node_store(),
                &CommitNode::from_commit(tampered),
                stored.parent_id,
            )?;
            for child in &stored.children {
                if let EMerkleTreeNode::Directory(dir) = &child.node {
                    commit_db.add_child(dir)?;
                }
            }
            commit_db.close()?;

            let report = verify_repo(&repo).await?;
            assert_eq!(
                report.misaddressed_commits.count, 1,
                "a stored commit that does not hash to its id is a finding: {report:?}"
            );
            let finding = &report.misaddressed_commits.sample[0];
            assert_eq!(finding.recorded_id, commit.id);
            assert_eq!(finding.computed_id, expected.computed_id);
            assert_eq!(
                report.total_findings(),
                1,
                "only the id is wrong, and the tree it points at is intact: {report:?}"
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

            write_unparseable_node(&repo, commit.hash()?)?;
            let report = verify_repo(&repo).await?;

            assert_eq!(
                report.unreadable_nodes.count, 1,
                "a head commit that will not parse is a finding: {report:?}"
            );
            assert_eq!(report.unreadable_nodes.sample[0].hash, commit.id);
            assert!(
                report.dangling_branches.is_empty(),
                "a head commit that is present but unreadable is not missing"
            );

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

            write_unparseable_node(&repo, first.hash()?)?;
            let report = verify_repo(&repo).await?;

            assert_eq!(
                report.unreadable_nodes.count, 1,
                "a parent commit that will not parse is a finding: {report:?}"
            );
            assert_eq!(report.unreadable_nodes.sample[0].hash, first.id);
            assert!(
                report.dangling_parents.is_empty(),
                "a parent that is present but unreadable is not missing"
            );
            assert_eq!(
                report.commits_checked, 1,
                "the walk still checks the commit whose parent is unreadable"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_a_workspace_whose_pin_resolves_is_counted_and_not_a_finding()
    -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let path = repo.path.join("a.txt");
            util::fs::write_to_path(&path, "alpha")?;
            repositories::add(&repo, &path).await?;
            let commit = repositories::commit(&repo, "Add a")?;

            workspaces::create(&repo, &commit, "ws-sound", true)?;

            let report = verify_repo(&repo).await?;

            assert_eq!(report.workspaces_checked, 1);
            assert!(report.dangling_workspaces.is_empty());
            assert!(report.is_healthy(), "a resolvable pin is not a finding");

            Ok(())
        })
        .await
    }

    /// Nothing but the pin references the commit. The total-findings assertion holds the line
    /// that no other class covers it.
    #[tokio::test]
    async fn test_a_workspace_pinned_to_a_missing_commit_is_reported() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let path = repo.path.join("a.txt");
            util::fs::write_to_path(&path, "alpha")?;
            repositories::add(&repo, &path).await?;
            let commit = repositories::commit(&repo, "Add a")?;

            let workspace = workspaces::create_with_name(
                &repo,
                &commit,
                "ws-stranded",
                Some("history_workspace".to_string()),
                true,
            )
            .await?;
            let absent = "deadbeefdeadbeefdeadbeefdeadbeef";
            workspaces::update_commit(&workspace, absent)?;

            let report = verify_repo(&repo).await?;

            assert_eq!(report.workspaces_checked, 1);
            assert_eq!(report.dangling_workspaces.count, 1, "{report:?}");
            let dangling = &report.dangling_workspaces.sample[0];
            assert_eq!(dangling.workspace, "ws-stranded");
            assert_eq!(dangling.name.as_deref(), Some("history_workspace"));
            assert_eq!(dangling.commit_id, absent);
            assert!(!report.is_healthy());

            assert_eq!(
                report.total_findings(),
                1,
                "the commit walk reaches nothing that names the pinned commit: {report:?}"
            );

            write_unparseable_node(&repo, absent.parse()?)?;
            let report = verify_repo(&repo).await?;

            assert_eq!(
                report.unreadable_nodes.count, 1,
                "a pinned commit that will not parse is a finding: {report:?}"
            );
            assert!(
                report.dangling_workspaces.is_empty(),
                "a pin to a present but unreadable commit is not dangling"
            );

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_a_directory_holding_no_config_is_not_counted_as_a_workspace()
    -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let path = repo.path.join("a.txt");
            util::fs::write_to_path(&path, "alpha")?;
            repositories::add(&repo, &path).await?;
            repositories::commit(&repo, "Add a")?;

            util::fs::create_dir_all(Workspace::workspaces_dir(&repo).join("leftover"))?;

            let report = verify_repo(&repo).await?;

            assert_eq!(report.workspaces_checked, 0);
            assert!(report.dangling_workspaces.is_empty());
            assert!(report.is_healthy());

            Ok(())
        })
        .await
    }

    /// A config written before the id was recorded still identifies its workspace, by the
    /// directory it lives in.
    #[tokio::test]
    async fn test_a_workspace_config_without_an_id_is_named_by_its_directory()
    -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let path = repo.path.join("a.txt");
            util::fs::write_to_path(&path, "alpha")?;
            repositories::add(&repo, &path).await?;
            let commit = repositories::commit(&repo, "Add a")?;

            let workspace = workspaces::create(&repo, &commit, "ws-idless", true)?;
            let dir = workspace.dir();
            let config_path = Workspace::config_path_from_dir(&dir);
            util::fs::write_to_path(
                &config_path,
                "workspace_commit_id = \"deadbeefdeadbeefdeadbeefdeadbeef\"\nis_editable = true\n",
            )?;

            let report = verify_repo(&repo).await?;

            assert_eq!(report.dangling_workspaces.count, 1, "{report:?}");
            let dangling = &report.dangling_workspaces.sample[0];
            let dir_name = dir
                .file_name()
                .expect("the workspace directory has a name")
                .to_string_lossy();
            assert_eq!(dangling.workspace, dir_name);
            assert_eq!(dangling.name, None);

            Ok(())
        })
        .await
    }
}
