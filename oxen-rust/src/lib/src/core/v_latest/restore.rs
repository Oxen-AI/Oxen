use crate::core::v_latest::index;
use crate::error::OxenError;
use crate::model::{LocalRepository, MerkleHash};
use crate::opts::{GlobOpts, RestoreOpts};
use crate::{repositories, util};

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

/// Combine the version chunks for large files and restore the completed file to the working dir
/// This can be used to recover files if this step fails during a fetch
pub async fn combine_chunks(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    let path = util::fs::path_relative_to_dir(&opts.path, &repo.path)?;
    let commit = repositories::commits::get_commit_or_head(repo, opts.source_ref.clone())?;

    // Find the file in the merkle tree
    let Some(file_node) = repositories::tree::get_file_by_path(repo, &commit, &path)? else {
        log::debug!(
            "path {:?}not found in tree for commit {:?}",
            path,
            commit.id
        );
        return Ok(());
    };

    // Recombine the version chunks for this file
    let hash = file_node.hash();
    combine_chunks_for_hash(repo, hash).await?;

    // Restore the completed file to the working directory
    index::restore::restore(repo, opts).await?;

    Ok(())
}

// Recombine file chunks for hash and delete the chunks dir
async fn combine_chunks_for_hash(
    repo: &LocalRepository,
    hash: &MerkleHash,
) -> Result<(), OxenError> {
    let version_store = repo.version_store()?;
    let hash_str = format!("{hash:?}");

    version_store
        .combine_version_chunks(&hash_str, true)
        .await?;

    Ok(())
}
