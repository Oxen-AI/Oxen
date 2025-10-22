use crate::core::v_latest::index;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::{GlobOpts, RestoreOpts};
use crate::util;

pub async fn restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    let path = &opts.path;

    let glob_opts = GlobOpts {
        paths: vec![path.to_path_buf()],
        staged_db: opts.staged,
        merkle_tree: !opts.staged,
        working_dir: false,
        walk_dirs: false,
    };

    let expanded_paths = util::glob::parse_glob_paths(&glob_opts, Some(repo))?;

    for path in expanded_paths {
        let mut opts = opts.clone();
        opts.path = path;
        index::restore::restore(repo, opts).await?;
    }

    Ok(())
}
