//! # oxen clean (v_latest)
//!
//! Remove untracked files and directories from the working tree. See the `clean` module
//! docs in `crates/lib/src/repositories/clean.rs` for behavior and the user-facing contract.

use std::path::{Path, PathBuf};

use futures::{StreamExt, TryStreamExt, stream};

use crate::constants::OXEN_HIDDEN_DIR;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::model::staged_data::StagedDataOpts;
use crate::opts::CleanOpts;
use crate::repositories;
use crate::util;

/// Outcome of a `clean` invocation.
///
/// In dry-run mode (`applied == false`) the `files` and `dirs` lists describe what *would*
/// be removed. In apply mode (`applied == true`) they describe what actually was removed.
#[derive(Debug, Clone, Default)]
pub struct CleanResult {
    pub files: Vec<PathBuf>,
    pub dirs: Vec<PathBuf>,
    pub total_bytes: u64,
    pub applied: bool,
}

pub async fn clean(repo: &LocalRepository, opts: &CleanOpts) -> Result<CleanResult, OxenError> {
    // Resolve scoping paths to repo-relative. Empty scope == whole working tree.
    let scope = opts
        .paths
        .iter()
        .map(|p| util::fs::path_relative_to_dir(p, &repo.path))
        .collect::<Result<Vec<PathBuf>, _>>()?;

    // Trust `status` to filter `.oxenignore` and `.oxen/` — see core::v_latest::status. The
    // explicit `.oxen/` guard below is defense-in-depth in case that ever regresses.
    let status = repositories::status::status_from_opts(repo, &StagedDataOpts::default())?;

    let mut files: Vec<PathBuf> = status
        .untracked_files
        .iter()
        .filter(|p| !is_inside_oxen(p))
        .filter(|p| path_in_scope(p, &scope))
        .cloned()
        .collect();
    files.sort();

    let mut dirs: Vec<PathBuf> = status
        .untracked_dirs
        .iter()
        .map(|(p, _)| p.clone())
        .filter(|p| !is_inside_oxen(p))
        .filter(|p| path_in_scope(p, &scope))
        .collect();
    dirs.sort();

    // Calculate total bytes of files and directories to be cleaned
    let mut total_bytes = 0u64;
    for f in &files {
        if let Ok(meta) = util::fs::metadata(repo.path.join(f)) {
            total_bytes = total_bytes.saturating_add(meta.len());
        }
    }
    for d in &dirs {
        let abs = repo.path.join(d);
        for entry in jwalk::WalkDir::new(&abs).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file()
                && let Ok(meta) = entry.metadata()
            {
                total_bytes = total_bytes.saturating_add(meta.len());
            }
        }
    }

    // Clean up files and directories
    if opts.force {
        stream::iter(files.clone())
            .map(|f| async move {
                println!("Removing {}", f.display());
                tokio::fs::remove_file(repo.path.join(&f)).await
            })
            .buffer_unordered(10)
            .try_collect::<Vec<()>>()
            .await?;
        stream::iter(dirs.clone())
            .map(|d| async move {
                println!("Removing {}/", d.display());
                tokio::fs::remove_dir_all(repo.path.join(&d)).await
            })
            .buffer_unordered(10)
            .try_collect::<Vec<()>>()
            .await?;
    } else {
        for f in &files {
            println!("Would remove {}", f.display());
        }
        for d in &dirs {
            println!("Would remove {}/", d.display());
        }
    }

    Ok(CleanResult {
        files,
        dirs,
        total_bytes,
        applied: opts.force,
    })
}

fn is_inside_oxen(p: &Path) -> bool {
    p.starts_with(OXEN_HIDDEN_DIR)
}

fn path_in_scope(candidate: &Path, scope: &[PathBuf]) -> bool {
    if scope.is_empty() {
        return true;
    }
    scope
        .iter()
        .any(|s| s.as_os_str().is_empty() || candidate == s.as_path() || candidate.starts_with(s))
}
