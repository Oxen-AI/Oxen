use std::{fs::File, path::Path};

use bytesize::ByteSize;
use flate2::Compression;
use flate2::write::GzEncoder;
use std::io::Write;
use tempfile::TempDir;
use walkdir::WalkDir;

use crate::constants::{NODES_LMDB_DIR, OXEN_HIDDEN_DIR, TREE_DIR};
use crate::core;
use crate::{error::OxenError, model::LocalRepository, util};

pub fn save(repo: &LocalRepository, dst_path: &Path) -> Result<(), OxenError> {
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
    core::staged::remove_from_cache_with_children(&repo.path)?;
    core::refs::remove_from_cache(&repo.path)?;

    let oxen_dir = util::fs::oxen_hidden_dir(&repo.path);
    let tar_subdir = Path::new(OXEN_HIDDEN_DIR);

    // Some backends can't be archived by copying their live on-disk files (an LMDB env's
    // `data.mdb` is mmapped and its bytes can shift under the archiver). Those return a
    // point-in-time-consistent snapshot to splice in instead; the filesystem backend returns
    // `None` and its node files are archived directly. The temp dir lives on the repo's filesystem
    // so the snapshot copy never crosses a device boundary.
    let snapshot_tmp = TempDir::new_in(&repo.path)?;
    let snapshot_file = repo
        .merkle_node_store()
        .snapshot_for_archive(snapshot_tmp.path())?;

    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    log::debug!("command::save compressing oxen dir at {oxen_dir:?} into tarball");

    println!("🐂 Compressing oxen repo at {:?}", repo.path);

    match snapshot_file {
        // Filesystem backend: the node files live under `.oxen`, so archive it wholesale.
        None => tar.append_dir_all(tar_subdir, &oxen_dir)?,
        // LMDB backend: archive everything under `.oxen` except the live env directory (whose
        // mmapped files can shift under the archiver), then splice the consistent snapshot in at
        // its location so load reconstructs the store from it. Symlinks aren't tracked by Oxen, so
        // skip rather than follow them.
        Some(data_file) => {
            let lmdb_env_dir = oxen_dir.join(TREE_DIR).join(NODES_LMDB_DIR);
            for entry in WalkDir::new(&oxen_dir)
                .into_iter()
                .filter_entry(|e| e.path() != lmdb_env_dir)
            {
                let entry =
                    entry.map_err(|e| OxenError::basic_str(format!("save: walk error: {e}")))?;
                let path = entry.path();
                let metadata = entry
                    .metadata()
                    .map_err(|e| OxenError::basic_str(format!("save: metadata error: {e}")))?;
                if metadata.is_symlink() {
                    continue;
                }
                let rel = path.strip_prefix(&oxen_dir).map_err(|e| {
                    OxenError::basic_str(format!("save: failed to relativize {path:?}: {e}"))
                })?;
                let name = tar_subdir.join(rel);
                if metadata.is_dir() {
                    tar.append_dir(&name, path)?;
                } else if metadata.is_file() {
                    tar.append_path_with_name(path, &name)?;
                }
            }

            let file_name = data_file.file_name().ok_or_else(|| {
                OxenError::basic_str(format!("save: snapshot file has no name: {data_file:?}"))
            })?;
            let name = tar_subdir
                .join(TREE_DIR)
                .join(NODES_LMDB_DIR)
                .join(file_name);
            tar.append_path_with_name(&data_file, &name)?;
        }
    }

    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!("command::save tarball size is {}", ByteSize(total_size));

    let mut file = File::create(output_path.clone())?;
    file.write_all(&buffer)?;

    println!("\n\n✅ Saved oxen repo to {output_path:?}\n\n");

    Ok(())
}
