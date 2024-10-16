use crate::config::UserConfig;
use crate::core::db;
use crate::core::merge::db_path;
pub use crate::core::merge::entry_merge_conflict_db_reader::EntryMergeConflictDBReader;
use crate::core::merge::entry_merge_conflict_reader::EntryMergeConflictReader;
use crate::core::merge::entry_merge_conflict_writer;
use crate::core::oxenignore;
use crate::core::refs::{RefReader, RefWriter};
use crate::core::v0_10_0::index::{
    CommitEntryReader, CommitEntryWriter, CommitReader, CommitWriter, SchemaReader, Stager,
};
use crate::error::OxenError;
use crate::model::{Branch, Commit, CommitEntry, EntryMergeConflict, LocalRepository};
use crate::repositories;
use crate::repositories::merge::MergeCommits;
use crate::util;

use rocksdb::{DBWithThreadMode, MultiThreaded, DB};
use std::path::{Path, PathBuf};
use std::str;

use super::restore;

pub struct Merger {
    repository: LocalRepository,
    merge_db: DB,
    // files_db: DBWithThreadMode<MultiThreaded>,
}

pub fn list_conflicts(repo: &LocalRepository) -> Result<Vec<EntryMergeConflict>, OxenError> {
    let reader = EntryMergeConflictReader::new(repo)?;
    reader.list_conflicts()
}

impl Merger {
    /// Create a new merger
    pub fn new(repo: &LocalRepository) -> Result<Merger, OxenError> {
        let db_path = db_path(repo);
        log::debug!("Merger::new() DB {:?}", db_path);
        let opts = db::key_val::opts::default();
        Ok(Merger {
            repository: repo.to_owned(),
            merge_db: DB::open(&opts, dunce::simplified(&db_path))?,
            // files_db: DBWithThreadMode::open(&opts, dunce::simplified(&files_db_path))?,
        })
    }

    /// Check if there are conflicts between the branch you are trying to merge and the base branch
    /// Returns true if there are conflicts, false if there are not
    pub fn has_conflicts(
        &self,
        base_branch: &Branch,
        merge_branch: &Branch,
    ) -> Result<bool, OxenError> {
        let commit_reader = CommitReader::new(&self.repository)?;
        let base_commit = Commit::from_branch(&commit_reader, base_branch)?;
        let merge_commit = Commit::from_branch(&commit_reader, merge_branch)?;

        Ok(!self.can_merge_commits(&commit_reader, &base_commit, &merge_commit)?)
    }

    /// Check if there are conflicts between the merge commit and the base commit
    /// Returns true if there are no conflicts, false if there are conflicts
    pub fn can_merge_commits(
        &self,
        commit_reader: &CommitReader,
        base_commit: &Commit,
        merge_commit: &Commit,
    ) -> Result<bool, OxenError> {
        let lca =
            self.lowest_common_ancestor_from_commits(commit_reader, base_commit, merge_commit)?;
        let merge_commits = MergeCommits {
            lca,
            base: base_commit.clone(),
            merge: merge_commit.clone(),
        };

        if merge_commits.is_fast_forward_merge() {
            // If it is fast forward merge, there are no merge conflicts
            return Ok(true);
        }

        let write_to_disk = false;
        let conflicts = self.find_merge_conflicts(&merge_commits, write_to_disk)?;
        Ok(conflicts.is_empty())
    }

    pub fn list_conflicts_between_branches(
        &self,
        commit_reader: &CommitReader,
        base_branch: &Branch,
        merge_branch: &Branch,
    ) -> Result<Vec<PathBuf>, OxenError> {
        let base_commit = Commit::from_branch(commit_reader, base_branch)?;
        let merge_commit = Commit::from_branch(commit_reader, merge_branch)?;

        self.list_conflicts_between_commits(commit_reader, &base_commit, &merge_commit)
    }

    pub fn list_commits_between_branches(
        &self,
        reader: &CommitReader,
        base_branch: &Branch,
        head_branch: &Branch,
    ) -> Result<Vec<Commit>, OxenError> {
        log::debug!(
            "list_commits_between_branches() base: {:?} head: {:?}",
            base_branch,
            head_branch
        );
        let base_commit = Commit::from_branch(reader, base_branch)?;
        let head_commit = Commit::from_branch(reader, head_branch)?;

        let lca = self.lowest_common_ancestor_from_commits(reader, &base_commit, &head_commit)?;

        reader.history_from_base_to_head(&lca.id, &head_commit.id)
    }

    pub fn list_commits_between_commits(
        &self,
        reader: &CommitReader,
        base_commit: &Commit,
        head_commit: &Commit,
    ) -> Result<Vec<Commit>, OxenError> {
        log::debug!(
            "list_commits_between_commits() base: {:?} head: {:?}",
            base_commit,
            head_commit
        );

        let lca = self.lowest_common_ancestor_from_commits(reader, base_commit, head_commit)?;

        log::debug!(
            "For commits {:?} -> {:?} found lca {:?}",
            base_commit,
            head_commit,
            lca
        );

        log::debug!("Reading history from lca to head");
        reader.history_from_base_to_head(&lca.id, &head_commit.id)
    }

    pub fn list_conflicts_between_commits(
        &self,
        commit_reader: &CommitReader,
        base_commit: &Commit,
        merge_commit: &Commit,
    ) -> Result<Vec<PathBuf>, OxenError> {
        let lca =
            self.lowest_common_ancestor_from_commits(commit_reader, base_commit, merge_commit)?;
        let merge_commits = MergeCommits {
            lca,
            base: base_commit.clone(),
            merge: merge_commit.clone(),
        };
        let write_to_disk = false;
        let conflicts = self.find_merge_conflicts(&merge_commits, write_to_disk)?;
        Ok(conflicts
            .iter()
            .map(|c| c.base_entry.path.to_owned())
            .collect())
    }

    /// Merge into the current branch, returns the merge commit if successful, and None if there is conflicts
    pub fn merge(&self, branch_name: impl AsRef<str>) -> Result<Option<Commit>, OxenError> {
        let branch_name = branch_name.as_ref();
        let commit_reader = CommitReader::new(&self.repository)?;

        let merge_branch = repositories::branches::get_by_name(&self.repository, branch_name)?
            .ok_or(OxenError::local_branch_not_found(branch_name))?;

        let base_commit = commit_reader.head_commit()?;
        let merge_commit = Commit::from_branch(&commit_reader, &merge_branch)?;

        let lca =
            self.lowest_common_ancestor_from_commits(&commit_reader, &base_commit, &merge_commit)?;
        let merge_commits = MergeCommits {
            lca,
            base: base_commit,
            merge: merge_commit,
        };

        self.merge_commits(&merge_commits)
    }

    /// Merge a branch into a base branch, returns the merge commit if successful, and None if there is conflicts
    pub fn merge_into_base(
        &self,
        merge_branch: &Branch,
        base_branch: &Branch,
    ) -> Result<Option<Commit>, OxenError> {
        println!(
            "merge_into_base merge {} into {}",
            merge_branch, base_branch
        );

        if merge_branch.commit_id == base_branch.commit_id {
            // If the merge branch is the same as the base branch, there is nothing to merge
            return Ok(None);
        }

        let commit_reader = CommitReader::new(&self.repository)?;
        let base_commit = Commit::from_branch(&commit_reader, base_branch)?;
        let merge_commit = Commit::from_branch(&commit_reader, merge_branch)?;

        let lca =
            self.lowest_common_ancestor_from_commits(&commit_reader, &base_commit, &merge_commit)?;
        let merge_commits = MergeCommits {
            lca,
            base: base_commit,
            merge: merge_commit,
        };

        self.merge_commits(&merge_commits)
    }

    pub fn merge_commit_into_base(
        &self,
        merge_commit: &Commit,
        base_commit: &Commit,
    ) -> Result<Option<Commit>, OxenError> {
        let commit_reader = CommitReader::new(&self.repository)?;

        let lca =
            self.lowest_common_ancestor_from_commits(&commit_reader, base_commit, merge_commit)?;
        log::debug!(
            "merge_commit_into_base has lca {:?} for merge commit {:?} and base {:?}",
            lca,
            merge_commit,
            base_commit
        );
        let merge_commits = MergeCommits {
            lca,
            base: base_commit.to_owned(),
            merge: merge_commit.to_owned(),
        };

        self.merge_commits(&merge_commits)
    }

    pub fn merge_commit_into_base_on_branch(
        &self,
        merge_commit: &Commit,
        base_commit: &Commit,
        branch: &Branch,
    ) -> Result<Option<Commit>, OxenError> {
        let commit_reader = CommitReader::new(&self.repository)?;
        let lca =
            self.lowest_common_ancestor_from_commits(&commit_reader, base_commit, merge_commit)?;

        log::debug!(
            "merge_commit_into_branch has lca {:?} for merge commit {:?} and base {:?}",
            lca,
            merge_commit,
            base_commit
        );

        let merge_commits = MergeCommits {
            lca,
            base: base_commit.to_owned(),
            merge: merge_commit.to_owned(),
        };

        self.merge_commits_on_branch(&merge_commits, branch)
    }

    fn merge_commits(&self, merge_commits: &MergeCommits) -> Result<Option<Commit>, OxenError> {
        // User output
        println!(
            "Updating {} -> {}",
            merge_commits.base.id, merge_commits.merge.id
        );

        log::debug!(
            "FOUND MERGE COMMITS:\nLCA: {} -> {}\nBASE: {} -> {}\nMerge: {} -> {}",
            merge_commits.lca.id,
            merge_commits.lca.message,
            merge_commits.base.id,
            merge_commits.base.message,
            merge_commits.merge.id,
            merge_commits.merge.message,
        );

        // Check which type of merge we need to do
        if merge_commits.is_fast_forward_merge() {
            // User output
            println!("Fast-forward");
            let commit = self.fast_forward_merge(&merge_commits.base, &merge_commits.merge)?;
            Ok(Some(commit))
        } else {
            log::debug!(
                "Three way merge! {} -> {}",
                merge_commits.base.id,
                merge_commits.merge.id
            );

            let write_to_disk = true;
            let conflicts = self.find_merge_conflicts(merge_commits, write_to_disk)?;
            log::debug!("Got {} conflicts", conflicts.len());

            if conflicts.is_empty() {
                let commit = self.create_merge_commit(merge_commits)?;
                Ok(Some(commit))
            } else {
                entry_merge_conflict_writer::write_conflicts_to_disk(
                    &self.repository,
                    &self.merge_db,
                    &merge_commits.merge,
                    &merge_commits.base,
                    &conflicts,
                )?;
                Ok(None)
            }
        }
    }

    fn merge_commits_on_branch(
        &self,
        merge_commits: &MergeCommits,
        branch: &Branch,
    ) -> Result<Option<Commit>, OxenError> {
        // User output
        println!(
            "Updating {} -> {}",
            merge_commits.base.id, merge_commits.merge.id
        );

        log::debug!(
            "FOUND MERGE COMMITS:\nLCA: {} -> {}\nBASE: {} -> {}\nMerge: {} -> {}",
            merge_commits.lca.id,
            merge_commits.lca.message,
            merge_commits.base.id,
            merge_commits.base.message,
            merge_commits.merge.id,
            merge_commits.merge.message,
        );

        // Check which type of merge we need to do
        if merge_commits.is_fast_forward_merge() {
            // User output
            println!("Fast-forward");
            let commit = self.fast_forward_merge(&merge_commits.base, &merge_commits.merge)?;
            Ok(Some(commit))
        } else {
            log::debug!(
                "Three way merge! {} -> {}",
                merge_commits.base.id,
                merge_commits.merge.id
            );

            let write_to_disk = true;
            let conflicts = self.find_merge_conflicts(merge_commits, write_to_disk)?;
            log::debug!("Got {} conflicts", conflicts.len());

            if conflicts.is_empty() {
                log::debug!("creating merge commit on branch {:?}", branch);
                let commit = self.create_merge_commit_on_branch(merge_commits, branch)?;
                Ok(Some(commit))
            } else {
                entry_merge_conflict_writer::write_conflicts_to_disk(
                    &self.repository,
                    &self.merge_db,
                    &merge_commits.merge,
                    &merge_commits.base,
                    &conflicts,
                )?;
                Ok(None)
            }
        }
    }

    pub fn has_file(&self, path: &Path) -> Result<bool, OxenError> {
        EntryMergeConflictDBReader::has_file(&self.merge_db, path)
    }

    pub fn remove_conflict_path(&self, path: &Path) -> Result<(), OxenError> {
        let path_str = path.to_str().unwrap();
        let key = path_str.as_bytes();
        self.merge_db.delete(key)?;
        Ok(())
    }

    fn create_merge_commit(&self, merge_commits: &MergeCommits) -> Result<Commit, OxenError> {
        let repo = &self.repository;

        // Stage changes
        let stager = Stager::new(repo)?;
        let commit = repositories::commits::head_commit(repo)?;
        let reader = CommitEntryReader::new(repo, &commit)?;
        let schema_reader = SchemaReader::new(repo, &commit.id)?;
        let ignore = oxenignore::create(repo);
        stager.add(&repo.path, &reader, &schema_reader, &ignore)?;

        let commit_msg = format!(
            "Merge commit {} into {}",
            merge_commits.merge.id, merge_commits.base.id
        );

        log::debug!("create_merge_commit {}", commit_msg);

        // Create a commit with both parents
        let reader = CommitEntryReader::new_from_head(repo)?;
        let status = stager.status(&reader)?;
        let commit_writer = CommitWriter::new(repo)?;
        let parent_ids: Vec<String> = vec![
            merge_commits.base.id.to_owned(),
            merge_commits.merge.id.to_owned(),
        ];
        let commit = commit_writer.commit_with_parent_ids(&status, parent_ids, &commit_msg)?;
        stager.unstage()?;

        Ok(commit)
    }

    fn create_merge_commit_on_branch(
        &self,
        merge_commits: &MergeCommits,
        branch: &Branch,
    ) -> Result<Commit, OxenError> {
        let repo = &self.repository;

        // Stage changes
        let stager = Stager::new(repo)?;
        let commit = repositories::commits::head_commit(repo)?;
        let reader = CommitEntryReader::new(repo, &commit)?;
        let schema_reader = SchemaReader::new(repo, &commit.id)?;
        let ignore = oxenignore::create(repo);
        stager.add(&repo.path, &reader, &schema_reader, &ignore)?;

        let commit_msg = format!(
            "Merge commit {} into {} on branch {}",
            merge_commits.merge.id, merge_commits.base.id, branch.name
        );

        log::debug!("create_merge_commit_on_branch {}", commit_msg);

        // Create a commit with both parents
        let reader = CommitEntryReader::new_from_head(repo)?;
        let status = stager.status(&reader)?;
        let commit_writer = CommitWriter::new(repo)?;
        let parent_ids: Vec<String> = vec![
            merge_commits.base.id.to_owned(),
            merge_commits.merge.id.to_owned(),
        ];

        // The author in this case is the pusher - the author of the merge commit

        let cfg = UserConfig {
            name: merge_commits.merge.author.clone(),
            email: merge_commits.merge.email.clone(),
        };

        let commit = commit_writer.commit_with_parent_ids_on_branch(
            &status,
            parent_ids,
            &commit_msg,
            branch.clone(),
            cfg,
        )?;

        stager.unstage()?;

        Ok(commit)
    }

    // This will try to find the least common ancestor, and if the least common ancestor is HEAD, then we just
    // fast forward, otherwise we need to three way merge
    pub fn find_merge_commits<S: AsRef<str>>(
        &self,
        branch_name: S,
    ) -> Result<MergeCommits, OxenError> {
        let branch_name = branch_name.as_ref();
        let ref_reader = RefReader::new(&self.repository)?;
        let head_commit_id = ref_reader
            .head_commit_id()?
            .ok_or_else(OxenError::head_not_found)?;
        let merge_commit_id = ref_reader
            .get_commit_id_for_branch(branch_name)?
            .ok_or_else(|| OxenError::commit_db_corrupted(branch_name))?;

        let commit_reader = CommitReader::new(&self.repository)?;
        let base = commit_reader
            .get_commit_by_id(&head_commit_id)?
            .ok_or_else(|| OxenError::commit_db_corrupted(&head_commit_id))?;
        let merge = commit_reader
            .get_commit_by_id(&merge_commit_id)?
            .ok_or_else(|| OxenError::commit_db_corrupted(&merge_commit_id))?;

        let lca = self.lowest_common_ancestor_from_commits(&commit_reader, &base, &merge)?;

        Ok(MergeCommits { lca, base, merge })
    }

    /// It is a fast forward merge if we cannot traverse cleanly back from merge to HEAD
    fn fast_forward_merge(
        &self,
        base_commit: &Commit,
        merge_commit: &Commit,
    ) -> Result<Commit, OxenError> {
        log::debug!("FF merge!");
        let base_commit_entry_reader = CommitEntryReader::new(&self.repository, base_commit)?;
        let merge_commit_entry_reader = CommitEntryReader::new(&self.repository, merge_commit)?;

        let base_entries = base_commit_entry_reader.list_entries_set()?;
        let merge_entries = merge_commit_entry_reader.list_entries_set()?;

        // Make sure files_db is dropped before the merge commit is written
        {
            let opts = db::key_val::opts::default();
            let files_db = CommitEntryWriter::files_db_dir(&self.repository);
            let files_db = DBWithThreadMode::open(&opts, dunce::simplified(&files_db))?;
            // Can just copy over all new versions since it is fast forward
            for merge_entry in merge_entries.iter() {
                log::debug!("Merge entry: {:?}", merge_entry.path);
                // Only copy over if hash is different or it doesn't exist for performance
                if let Some(base_entry) = base_entries.get(merge_entry) {
                    if base_entry.hash != merge_entry.hash {
                        log::debug!("Merge entry has changed, restore: {:?}", merge_entry.path);
                        self.update_entry(merge_entry, &files_db)?;
                    }
                } else {
                    log::debug!("Merge entry is new, restore: {:?}", merge_entry.path);
                    self.update_entry(merge_entry, &files_db)?;
                }
            }

            // Remove all entries that are in HEAD but not in merge entries
            for base_entry in base_entries.iter() {
                log::debug!("Base entry: {:?}", base_entry.path);
                if !merge_entries.contains(base_entry) {
                    log::debug!("Removing Base Entry: {:?}", base_entry.path);

                    let path = self.repository.path.join(&base_entry.path);
                    if path.exists() {
                        util::fs::remove_file(path)?;
                    }
                }
            }
        }

        // Move the HEAD forward to this commit
        let ref_writer = RefWriter::new(&self.repository)?;
        ref_writer.set_head_commit_id(&merge_commit.id)?;

        Ok(merge_commit.clone())
    }

    /// Check if HEAD is in the direct parent chain of the merge commit. If it is a direct parent, we can just fast forward
    pub fn lowest_common_ancestor<S: AsRef<str>>(
        &self,
        branch_name: S,
    ) -> Result<Commit, OxenError> {
        let branch_name = branch_name.as_ref();
        let ref_reader = RefReader::new(&self.repository)?;
        let base_commit_id = ref_reader
            .head_commit_id()?
            .ok_or_else(OxenError::head_not_found)?;
        let merge_commit_id = ref_reader
            .get_commit_id_for_branch(branch_name)?
            .ok_or_else(|| OxenError::commit_db_corrupted(branch_name))?;

        let commit_reader = CommitReader::new(&self.repository)?;
        let base_commit = commit_reader
            .get_commit_by_id(&base_commit_id)?
            .ok_or_else(|| OxenError::commit_db_corrupted(&base_commit_id))?;
        let merge_commit = commit_reader
            .get_commit_by_id(&merge_commit_id)?
            .ok_or_else(|| OxenError::commit_db_corrupted(&merge_commit_id))?;

        self.lowest_common_ancestor_from_commits(&commit_reader, &base_commit, &merge_commit)
    }

    pub fn lowest_common_ancestor_from_commits(
        &self,
        commit_reader: &CommitReader,
        base_commit: &Commit,
        merge_commit: &Commit,
    ) -> Result<Commit, OxenError> {
        // Traverse the base commit back to start, keeping map of Commit -> Depth(int)
        let commit_depths_from_head = commit_reader.history_with_depth_from_commit(base_commit)?;

        // Traverse the merge commit back
        //   check at each step if ID is in the HEAD commit history
        //   The lowest Depth Commit in HEAD should be the LCA
        let commit_depths_from_merge =
            commit_reader.history_with_depth_from_commit(merge_commit)?;

        let mut min_depth = usize::MAX;
        let mut lca: Commit = commit_depths_from_head.keys().next().unwrap().clone();
        for (commit, _) in commit_depths_from_merge.iter() {
            if let Some(depth) = commit_depths_from_head.get(commit) {
                if depth < &min_depth {
                    min_depth = *depth;
                    log::debug!("setting new lca, {:?}", commit);
                    lca = commit.clone();
                }
            }
        }

        Ok(lca)
    }

    /// Will try a three way merge and return conflicts if there are any to indicate that the merge was unsuccessful
    pub fn find_merge_conflicts(
        &self,
        merge_commits: &MergeCommits,
        write_to_disk: bool,
    ) -> Result<Vec<EntryMergeConflict>, OxenError> {
        log::debug!("finding merge conflicts");
        /*
        https://en.wikipedia.org/wiki/Merge_(version_control)#Three-way_merge

        C = LCA
        A = Base
        B = Merge
        D = Resulting merge commit

        C - A - D
          \   /
            B

        The three-way merge looks for sections which are the same in only two of the three files.
        In this case, there are two versions of the section,
            and the version which is in the common ancestor "C" is discarded,
            while the version that differs is preserved in the output.
        If "A" and "B" agree, that is what appears in the output.
        A section that is the same in "A" and "C" outputs the changed version in "B",
        and likewise a section that is the same in "B" and "C" outputs the version in "A".

        Sections that are different in all three files are marked as a conflict situation and left for the user to resolve.
        */

        // We will return conflicts if there are any
        let mut conflicts: Vec<EntryMergeConflict> = vec![];

        // Read all the entries from each commit into sets we can compare to one another
        let lca_entry_reader = CommitEntryReader::new(&self.repository, &merge_commits.lca)?;
        let base_entry_reader = CommitEntryReader::new(&self.repository, &merge_commits.base)?;
        let merge_entry_reader = CommitEntryReader::new(&self.repository, &merge_commits.merge)?;

        let lca_entries = lca_entry_reader.list_entries_set()?;
        let base_entries = base_entry_reader.list_entries_set()?;
        let merge_entries = merge_entry_reader.list_entries_set()?;

        log::debug!("lca_entries.len() {}", lca_entries.len());
        log::debug!("base_entries.len() {}", base_entries.len());
        log::debug!("merge_entries.len() {}", merge_entries.len());

        let opts = db::key_val::opts::default();
        let files_db = CommitEntryWriter::files_db_dir(&self.repository);
        let files_db = DBWithThreadMode::open(&opts, dunce::simplified(&files_db))?;

        // Check all the entries in the candidate merge
        for merge_entry in merge_entries.iter() {
            // log::debug!("Considering entry {}", merge_entries.len());

            // Check if the entry exists in all 3 commits
            if let Some(base_entry) = base_entries.get(merge_entry) {
                if let Some(lca_entry) = lca_entries.get(merge_entry) {
                    // If Base and LCA are the same but Merge is different, take merge
                    // log::debug!(
                    //     "Comparing hashes merge_entry {:?} BASE {} LCA {} MERGE {}",
                    //     merge_entry.path,
                    //     base_entry.hash,
                    //     lca_entry.hash,
                    //     merge_entry.hash
                    // );
                    if base_entry.hash == lca_entry.hash
                        && base_entry.hash != merge_entry.hash
                        && write_to_disk
                    {
                        log::debug!("top update entry");
                        self.update_entry(merge_entry, &files_db)?;
                    }

                    // If all three are different, mark as conflict
                    if base_entry.hash != lca_entry.hash
                        && lca_entry.hash != merge_entry.hash
                        && base_entry.hash != merge_entry.hash
                    {
                        conflicts.push(EntryMergeConflict {
                            lca_entry: lca_entry.to_owned(),
                            base_entry: base_entry.to_owned(),
                            merge_entry: merge_entry.to_owned(),
                        });
                    }
                } else {
                    // merge entry doesn't exist in LCA, so just check if it's different from base
                    if base_entry.hash != merge_entry.hash {
                        conflicts.push(EntryMergeConflict {
                            lca_entry: base_entry.to_owned(),
                            base_entry: base_entry.to_owned(),
                            merge_entry: merge_entry.to_owned(),
                        });
                    }
                }
            } else if write_to_disk {
                // merge entry does not exist in base, so create it
                log::debug!("bottom update entry");
                self.update_entry(merge_entry, &files_db)?;
            }
        }
        log::debug!("three_way_merge conflicts.len() {}", conflicts.len());

        Ok(conflicts)
    }

    fn update_entry(
        &self,
        merge_entry: &CommitEntry,
        files_db: &DBWithThreadMode<MultiThreaded>,
    ) -> Result<(), OxenError> {
        restore::restore_file(
            &self.repository,
            &merge_entry.path,
            &merge_entry.commit_id,
            merge_entry,
            files_db,
        )?;
        Ok(())
    }
}
