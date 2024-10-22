use std::{fs::File, path::Path};

use bytesize::ByteSize;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;

use crate::{constants::OXEN_HIDDEN_DIR, error::OxenError, model::LocalRepository, util};

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

    let oxen_dir = util::fs::oxen_hidden_dir(&repo.path);
    let tar_subdir = Path::new(OXEN_HIDDEN_DIR);

    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    log::debug!(
        "command::save compressing oxen dir at {:?} into tarball",
        oxen_dir
    );

    println!("🐂 Compressing oxen repo at {:?}", repo.path);

    tar.append_dir_all(tar_subdir, &oxen_dir)?;
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!("command::save tarball size is {}", ByteSize(total_size));

    let mut file = File::create(output_path.clone())?;
    file.write_all(&buffer)?;

    println!("\n\n✅ Saved oxen repo to {:?}\n\n", output_path);

    Ok(())
}
