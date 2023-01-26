use crate::constants::{HEAD_FILE, REFS_DIR};
use crate::db;
use crate::error::OxenError;
use crate::index::CommitReader;
use crate::model::{Branch, LocalRepository};
use crate::util;

use rocksdb::{IteratorMode, DB};
use std::path::PathBuf;
use std::str;

pub struct RefReader {
    refs_db: DB,
    head_file: PathBuf,
    repository: LocalRepository,
}

impl RefReader {
    pub fn new(repository: &LocalRepository) -> Result<RefReader, OxenError> {
        let refs_dir = util::fs::oxen_hidden_dir(&repository.path).join(REFS_DIR);
        let head_filename = util::fs::oxen_hidden_dir(&repository.path).join(HEAD_FILE);
        let error_if_log_file_exist = false;
        let opts = db::opts::default();

        if !refs_dir.exists() {
            std::fs::create_dir_all(&refs_dir)?;
            // open it then lose scope to close it
            // so that we can read an empty one if it doesn't exist
            let _db = DB::open(&opts, &refs_dir)?;
        }

        Ok(RefReader {
            refs_db: DB::open_for_read_only(&opts, &refs_dir, error_if_log_file_exist)?,
            head_file: head_filename,
            repository: repository.clone(),
        })
    }

    pub fn has_branch(&self, name: &str) -> bool {
        let bytes = name.as_bytes();
        match self.refs_db.get(bytes) {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(_) => false,
        }
    }

    pub fn get_current_branch(&self) -> Result<Option<Branch>, OxenError> {
        let ref_name = self.read_head_ref()?;
        if ref_name.is_none() {
            return Ok(None);
        }

        let ref_name = ref_name.unwrap();
        if let Some(id) = self.get_commit_id_for_branch(&ref_name)? {
            Ok(Some(Branch {
                name: ref_name,
                commit_id: id,
                is_head: true,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn get_commit_id_for_branch(&self, name: &str) -> Result<Option<String>, OxenError> {
        let bytes = name.as_bytes();
        match self.refs_db.get(bytes) {
            Ok(Some(value)) => Ok(Some(String::from(str::from_utf8(&value)?))),
            Ok(None) => {
                log::debug!(
                    "get_commit_id_for_branch could not find commit id for branch {}",
                    name
                );
                Ok(None)
            }
            Err(err) => {
                log::error!(
                    "get_commit_id_for_branch error finding commit id for branch {}",
                    name
                );
                let err = format!("{err}");
                Err(OxenError::basic_str(err))
            }
        }
    }

    pub fn head_commit_id(&self) -> Result<Option<String>, OxenError> {
        let head_ref = self.read_head_ref()?;
        log::debug!("Got HEAD ref {:?}", head_ref);

        if let Some(head_ref) = head_ref {
            if let Some(commit_id) = self.get_commit_id_for_branch(&head_ref)? {
                log::debug!(
                    "RefReader::head_commit_id got commit id {} for branch {}",
                    commit_id,
                    head_ref
                );
                Ok(Some(commit_id))
            } else {
                log::debug!(
                    "RefReader::head_commit_id looking for head_ref {}",
                    head_ref
                );
                let commit_reader = CommitReader::new(&self.repository)?;
                if commit_reader.commit_id_exists(&head_ref) {
                    Ok(Some(head_ref))
                } else {
                    log::debug!("Commit id does not exist {:?}", head_ref);
                    Ok(None)
                }
            }
        } else {
            log::debug!("Head ref is none {:?}", head_ref);
            Ok(None)
        }
    }

    pub fn read_head_ref(&self) -> Result<Option<String>, OxenError> {
        // Should probably lock before reading...
        // but not a lot of parallel action going on here
        log::debug!("Looking for HEAD at {:?}", self.head_file);
        if self.head_file.exists() {
            Ok(Some(util::fs::read_from_path(&self.head_file)?))
        } else {
            log::debug!("HEAD not found at {:?}", self.head_file);
            Ok(None)
        }
    }

    pub fn list_branches(&self) -> Result<Vec<Branch>, OxenError> {
        let mut branch_names: Vec<Branch> = vec![];
        let maybe_head_ref = self.read_head_ref()?;
        let iter = self.refs_db.iterator(IteratorMode::Start);
        for (key, value) in iter {
            match (str::from_utf8(&key), str::from_utf8(&value)) {
                (Ok(key_str), Ok(value)) => {
                    if let Some(head_ref) = &maybe_head_ref {
                        let ref_name = String::from(key_str);
                        let id = String::from(value);
                        branch_names.push(Branch {
                            name: ref_name.clone(),
                            commit_id: id.clone(),
                            is_head: (ref_name == head_ref.clone()),
                        });
                    }
                }
                _ => {
                    eprintln!("Could not read utf8 val...")
                }
            }
        }
        Ok(branch_names)
    }

    pub fn get_branch_by_name(&self, name: &str) -> Result<Option<Branch>, OxenError> {
        log::debug!("get_branch_by_name {name}");
        let maybe_head_id = self.head_commit_id()?;
        if maybe_head_id.is_none() {
            return Ok(None);
        }

        let head_commit_id = maybe_head_id.unwrap();
        log::debug!("get_branch_by_name got head_commit_id {}", head_commit_id);
        match self.get_commit_id_for_branch(name) {
            Ok(Some(commit_id)) => Ok(Some(Branch {
                name: name.to_string(),
                commit_id: commit_id.to_string(),
                is_head: commit_id == head_commit_id,
            })),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::index::RefReader;
    use crate::test;

    #[test]
    fn test_ref_reader_list_branches() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            command::create_branch_from_head(&repo, "feature/add-something")?;
            command::create_branch_from_head(&repo, "bug/something-is-broken")?;

            let ref_reader = RefReader::new(&repo)?;
            let branches = ref_reader.list_branches()?;

            // We start with the main branch, then added these two
            assert_eq!(branches.len(), 3);

            assert!(branches.iter().any(|b| b.name == "feature/add-something"));
            assert!(branches.iter().any(|b| b.name == "bug/something-is-broken"));
            assert!(branches.iter().any(|b| b.name == "main"));

            Ok(())
        })
    }
}
