use crate::core::v_latest::index;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::{GlobOpts, RestoreOpts};
use crate::util;

// TODO: Deprecate this module
// Not sure why we have seperate core and core::index modules for restore
pub async fn restore(repo: &LocalRepository, restore_opts: RestoreOpts) -> Result<(), OxenError> {
    let paths = restore_opts.paths.iter().map(|p| p.to_owned()).collect();

    let glob_opts = GlobOpts {
        paths,
        staged_db: restore_opts.staged,
        merkle_tree: !restore_opts.staged,
        working_dir: false,
        walk_dirs: false,
    };

    let expanded_paths = util::glob::parse_glob_paths(&glob_opts, Some(repo))?;

    let mut restore_opts = restore_opts.clone();
    restore_opts.paths = expanded_paths;

    index::restore::restore(repo, restore_opts).await?;

    Ok(())
}
