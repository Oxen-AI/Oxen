//! This module goes through a repo and caches values on commits that will never change
//! but are expensive to compute at runtime

use crate::constants::{CACHE_DIR, HISTORY_DIR};
use crate::core::db;
use crate::core::db::key_val::str_json_db;
use crate::core::v0_10_0::cache::cacher_status::{CacherStatus, CacherStatusType};
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::util;

use super::cachers::{content_stats, content_validator, df_size, repo_size};
use lazy_static::lazy_static;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::PathBuf;

type CommitCacher = fn(&LocalRepository, &Commit) -> Result<(), OxenError>;

lazy_static! {
    /// These are all the cachers we are going to run in `run_all`
    /// TODO: make this a config file that users can extend or run their own cachers
    static ref CACHERS: Vec<(String, CommitCacher)> = vec![
        (String::from("COMMIT_CONTENT_IS_VALID"), content_validator::compute as CommitCacher),
        (String::from("REPO_SIZE"), repo_size::compute as CommitCacher),
        (String::from("COMMIT_STATS"), content_stats::compute as CommitCacher),
        (String::from("DF_SIZE"), df_size::compute as CommitCacher),
        // (String::from("ARROW_CONVERSION"), convert_to_arrow::convert_to_arrow as CommitCacher),
    ];
}

fn cached_status_db_path(repo: &LocalRepository, commit: &Commit) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join("status.db")
}

fn cached_status_lock_path(repo: &LocalRepository, commit: &Commit) -> PathBuf {
    util::fs::oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join("LOCK")
}

/// Pick most appropriate status to return given the status's in the db
pub fn get_status(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Option<CacherStatusType>, OxenError> {
    let vals = get_all_statuses(repo, commit)?;
    if vals.iter().any(|v| CacherStatusType::Pending == v.status) {
        return Ok(Some(CacherStatusType::Pending));
    }

    if vals.iter().any(|v| CacherStatusType::Failed == v.status) {
        return Ok(Some(CacherStatusType::Failed));
    }

    Ok(vals
        .into_iter()
        .find(|v| CacherStatusType::Success == v.status)
        .map(|v| v.status))
}

/// Return all the statuses from cacher processes that were run
pub fn get_failures(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Vec<CacherStatus>, OxenError> {
    let db_path = cached_status_db_path(repo, commit);
    let opts = db::key_val::opts::default();
    let db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;

    let vals = str_json_db::list_vals::<MultiThreaded, CacherStatus>(&db)?
        .into_iter()
        .filter(|v| v.status == CacherStatusType::Failed)
        .collect();
    Ok(vals)
}

/// Return all the statuses from cacher processes that were run
pub fn get_all_statuses(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<Vec<CacherStatus>, OxenError> {
    let db_path = cached_status_db_path(repo, commit);
    let lock_path = cached_status_lock_path(repo, commit);

    // Return if db path !exists
    if !db_path.exists() {
        return Ok(vec![]);
    }

    // Return if we are locked because it is processing
    if lock_path.exists() {
        return Ok(vec![CacherStatus::pending()]);
    }

    log::warn!("get_all_statuses Opening db connection {:?}", db_path);
    let opts = db::key_val::opts::default();
    let error_if_log_file_exist = false;
    let db = DBWithThreadMode::open_for_read_only(
        &opts,
        dunce::simplified(&db_path),
        error_if_log_file_exist,
    );
    match db {
        Ok(db) => {
            let vals = str_json_db::list_vals::<MultiThreaded, CacherStatus>(&db)?;
            Ok(vals)
        }
        Err(_) => {
            // If db path exists, and error, it's probably because we are trying to open from different processes
            log::debug!("Could not open db....still processing");
            Ok(vec![CacherStatus::pending()])
        }
    }
}

// DB connection might be already LOCK'd from get_all_statuses
fn get_db_connection(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<DBWithThreadMode<MultiThreaded>, OxenError> {
    let db_path = cached_status_db_path(repo, commit);
    let opts = db::key_val::opts::default();
    let sleep_time = 100;
    let mut num_attempts = 5;

    while num_attempts >= 1 {
        match DBWithThreadMode::open(&opts, dunce::simplified(&db_path)) {
            Ok(db) => return Ok(db),
            Err(err) => {
                // sleep
                let time = sleep_time * num_attempts;
                log::warn!(
                    "Could not open db connection sleeping {time}s attempt {num_attempts} {err:?}"
                );
                std::thread::sleep(std::time::Duration::from_millis(time));
                num_attempts -= 1;
            }
        }
    }
    Err(OxenError::basic_str("Could not open db"))
}

pub fn force_remove_lock(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    let lock_path = cached_status_lock_path(repo, commit);
    if lock_path.exists() {
        log::warn!("force_remove_lock Deleting lock file {:?}", lock_path);
        util::fs::remove_file(lock_path)?;
    }
    Ok(())
}

pub fn set_all_cachers_status(
    repo: &LocalRepository,
    commit: &Commit,
    status: CacherStatus,
) -> Result<(), OxenError> {
    let db: DBWithThreadMode<MultiThreaded> = get_db_connection(repo, commit)?;
    // TODO: can we tell which ones actually failed so we don't have to rerun all?
    force_remove_lock(repo, commit)?;
    for (name, _cacher) in CACHERS.iter() {
        str_json_db::put(&db, name, &status)?;
    }
    Ok(())
}

/// Run all the cachers and update their status's as you go
pub fn run_all(repo: &LocalRepository, commit: &Commit, force: bool) -> Result<(), OxenError> {
    // Write the LOCK file and delete when we are done processing
    let lock_path = cached_status_lock_path(repo, commit);
    log::warn!("run_all called on commit {} force? {}", commit, force);

    // If the LOCK exists we should not be running this again
    if lock_path.exists() {
        log::warn!("run_all LOCK file exists...skipping {:?}", lock_path);
        return Err(OxenError::basic_str("Already processing"));
    }

    // Create parent dir if not exists
    if let Some(parent) = lock_path.parent() {
        if !parent.exists() {
            util::fs::create_dir_all(parent)?;
        }
    }
    log::debug!("run_all Creating lock file {:?}", lock_path);
    util::fs::write_to_path(&lock_path, "LOCK")?;

    // Create kvdb of NAME -> STATUS
    let db: DBWithThreadMode<MultiThreaded> = get_db_connection(repo, commit)?;

    for (name, cacher) in CACHERS.iter() {
        log::debug!("run_all running {:?}", name);
        // Skip ones that are already cached successfully
        if let Some(val) = str_json_db::get::<MultiThreaded, &str, CacherStatus>(&db, name)? {
            if CacherStatusType::Success == val.status && !force {
                log::debug!("run_all skipping {:?}", name);
                continue;
            }
        }

        // set as pending
        let pending_status = CacherStatus::pending();
        str_json_db::put(&db, name, &pending_status)?;

        // run and set appropriate status
        match cacher(repo, commit) {
            Ok(_) => {
                let status_success = CacherStatus::success();
                str_json_db::put(&db, name, &status_success)?;
                log::debug!("run_all done running {:?}", name);
            }
            Err(err) => {
                let err = format!("{err}");
                log::error!("{}", err);
                let status_failed = CacherStatus::failed(&err);
                str_json_db::put(&db, name, &status_failed)?;
            }
        }
    }
    // Delete the LOCK file
    log::debug!("run_all Deleting lock file {:?}", lock_path);
    util::fs::remove_file(lock_path)?;

    Ok(())
}
