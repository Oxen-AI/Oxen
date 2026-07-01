use std::{fs::File, path::Path};

use bytesize::ByteSize;
use flate2::Compression;
use flate2::write::GzEncoder;
use std::io::Write;

use crate::constants::{NODES_LMDB_DIR, OXEN_HIDDEN_DIR, TREE_DIR};
use crate::core;
use crate::{error::OxenError, model::LocalRepository, util};

/// Takes `repo` by value and drops it before archiving so the repo's Merkle node store handle is
/// released first: under the LMDB backend this closes the env (unmapping `data.mdb` / `lock.mdb`),
/// so the archive captures a quiescent, consistent store rather than a live mmap being written
/// underneath the tar reader.
pub fn save(repo: LocalRepository, dst_path: &Path) -> Result<(), OxenError> {
    let repo_path = repo.path.clone();
    let output_path = if !dst_path.exists() {
        dst_path.to_path_buf()
    } else {
        match (dst_path.is_file(), dst_path.is_dir()) {
            (true, false) => dst_path.to_path_buf(),
            (false, true) => dst_path.join("oxen-archive.tar.gz"),
            _ => return Err(OxenError::basic_str(dst_path.to_str().unwrap())),
        }
    };

    // Close DB instances before we tar it.
    core::staged::remove_from_cache_with_children(&repo_path)?;
    core::refs::remove_from_cache(&repo_path)?;

    // Drop the repo to release its Merkle node store handle. Under the LMDB backend this is the
    // last strong reference, so the env closes here and its files stop being written.
    drop(repo);

    // Guard: the archive must not capture a live LMDB env. If some other handle still holds the
    // env open (a cached repo elsewhere in the process), refuse rather than tar an inconsistent,
    // actively-mmapped store.
    let lmdb_env_dir = repo_path
        .join(OXEN_HIDDEN_DIR)
        .join(TREE_DIR)
        .join(NODES_LMDB_DIR);
    if crate::lmdb::shared_env_is_live(&lmdb_env_dir) {
        return Err(OxenError::basic_str(
            "Cannot save: the repository's Merkle node store is still open elsewhere in this process",
        ));
    }

    let oxen_dir = util::fs::oxen_hidden_dir(&repo_path);
    let tar_subdir = Path::new(OXEN_HIDDEN_DIR);

    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    log::debug!("command::save compressing oxen dir at {oxen_dir:?} into tarball");

    println!("🐂 Compressing oxen repo at {repo_path:?}");

    tar.append_dir_all(tar_subdir, &oxen_dir)?;
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!("command::save tarball size is {}", ByteSize(total_size));

    let mut file = File::create(output_path.clone())?;
    file.write_all(&buffer)?;

    println!("\n\n✅ Saved oxen repo to {output_path:?}\n\n");

    Ok(())
}
