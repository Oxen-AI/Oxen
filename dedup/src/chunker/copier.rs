use std::{
    fs::{self, File},
    io::{self, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    time::Duration,
};
use crate::chunker::{Chunker, ArchiveDedupStats};

/*
Super simple chunker that just copies the file to a new location.
Our current baseline implemented in Oxen.
*/
pub struct Copier {}

impl Chunker for Copier {
    fn name(&self) -> &'static str {
        "mover"
    }

    fn pack(
        &self,
        input_files: &Vec<PathBuf>,
        output_dir: &Path,
        _ignored_dirs: &Vec<PathBuf>,
    ) -> Result<PathBuf, io::Error> {
        if input_files.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "No input files provided for pack operation",
            ));
        }

        fs::create_dir_all(output_dir)?;
        println!("Packing to directory: {:?}", output_dir.display());

        for input_file_path in input_files {
            let file_name = input_file_path.file_name().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "Input path does not have a file name: {:?}",
                        input_file_path
                    ),
                )
            })?;

            let output_file_path = output_dir.join(file_name);

            println!(
                "Packing file: {:?} to {:?}",
                input_file_path.display(),
                output_file_path.display()
            );

            let mut input_stream = BufReader::new(File::open(input_file_path)?);
            let mut output_stream = BufWriter::new(File::create(&output_file_path)?);

            io::copy(&mut input_stream, &mut output_stream)?;
            output_stream.flush()?; // Ensure data is written before proceeding
        }

        println!("All files packed to {:?}", output_dir.display());
        Ok(output_dir.to_path_buf())
    }

    fn unpack(&self, input_dir: &Path, output_path: &Path) -> Result<PathBuf, io::Error> {
        // output_path is the directory where files will be unpacked.
        fs::create_dir_all(output_path)?;
        println!(
            "Unpacking from directory: {:?} to {:?}",
            input_dir.display(),
            output_path.display()
        );

        for entry in fs::read_dir(input_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                let file_name = path.file_name().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::Other,format!("Packed file path does not have a file name: {:?}", path))
                })?;
                let dest_path = output_path.join(file_name);
                println!("Unpacking {:?} to {:?}", path.display(), dest_path.display());
                fs::copy(&path, &dest_path)?;
            }
        }

        println!("All files unpacked to {:?}", output_path.display());
        Ok(output_path.to_path_buf())
    }

    fn get_chunk_hashes(&self, _input_dir: &Path) -> Result<Vec<String>, io::Error> {
        Ok(Vec::new())
    }

    fn get_archive_stats(&self, _archive_dir: &Path) -> Result<Option<ArchiveDedupStats>, io::Error> {
        if !_archive_dir.exists() {
            return Ok(None);
        }
        if !_archive_dir.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                format!("Archive path is not a directory: {:?}", _archive_dir),
            ));
        }

        let mut num_files_copied: u64 = 0;
        let mut total_bytes_copied: u64 = 0;

        for entry in fs::read_dir(_archive_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                num_files_copied += 1;
                let metadata = fs::metadata(&path)?;
                total_bytes_copied += metadata.len();
            }
        }

        Ok(Some(ArchiveDedupStats {
            overlapping_unique_chunks_between_files: 0,
            total_intra_file_space_saved_bytes: 0,
            total_space_saved_bytes: 0,
            total_logical_chunks_referenced: num_files_copied as usize,
            unique_chunks_physically_stored: num_files_copied as usize,
            total_logical_size_bytes: total_bytes_copied,
            total_physical_size_bytes: total_bytes_copied,
            time_to_pack: Duration::ZERO, // Copier's pack time isn't stored in archive metadata
        }))
    }
}