use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};

use liboxen::model::{ LocalRepository};
use liboxen::repositories::{self, commits};
use serde::{Serialize, Deserialize};
use crate::chunker::{get_chunker};

use super::Algorithm;
 
pub struct PackCmd;

pub struct OxenStats {
    pub pack_time: f64,
    pub unpack_time: f64,
    pub pack_cpu_usage: f32,
    pub pack_memory_usage_bytes: u64,
    pub unpack_cpu_usage: f32,
    pub unpack_memory_usage_bytes: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OxenChunker {
    chunk_size: usize,
    chunk_algorithm: String,
    root_path: PathBuf,
}

impl OxenChunker {
    pub fn new(chunk_size: usize, chunk_algorithm: String, root_path: PathBuf) -> Result<Self, Error> {
        Ok(Self { chunk_size, chunk_algorithm, root_path })
    }

    pub fn pack(&self, algo: Algorithm, chunk_size: usize, input_path: &Path, output_dir: &Path, ignored_dirs: &Vec<PathBuf> ) -> Result<PathBuf, Error> {
        
        std::fs::create_dir_all(output_dir)?;

        let repo = LocalRepository::from_current_dir().map_err(|e| Error::new(ErrorKind::NotFound, format!("error loading repository: {}", e) ))?;
        let chunker = get_chunker(&algo, chunk_size).map_err(|e| Error::new(ErrorKind::NotFound, format!("error fetching chunker:  {}", e)))?;

        let head_commit = commits::head_commit(&repo).map_err(|e| Error::new(ErrorKind::Other, format!("Error getting HEAD commit: {}", e)))?;

       let commit_tree = repositories::tree::get_root_with_children(&repo, &head_commit)

            .map_err(|oxen_error| {
                Error::new(
                    ErrorKind::Other,
                    format!(
                        "Failed to retrieve tree for commit {}: {}",
                        head_commit.id, oxen_error
                    ),
                )
            })? 

            .ok_or_else(|| {
                Error::new(
                    ErrorKind::NotFound,
                    format!("Root tree not found for commit {}", head_commit.id),
                )
            })?;
        let commit_id = head_commit.id.clone();
        let commit_output_dir = output_dir.join(&commit_id);

        let paths = commit_tree.list_file_paths().unwrap();
        let mut path_list = Vec::new();

        for path in paths {
            println!("path:{}", path.display());
            path_list.push(path);
        }
        std::fs::create_dir_all(&commit_output_dir)?;

        chunker.pack(&path_list, &commit_output_dir, ignored_dirs)?;
        
        let actual_stats: super::ArchiveDedupStats = chunker.get_archive_stats(&commit_output_dir)?
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "Archive deduplication statistics not found or not supported by this chunker."))?;

        println!("Successfully retrieved archive stats: {:?}", actual_stats);

        println!("Packing operation for HEAD commit finished.");
        Ok(commit_output_dir.to_path_buf())
    }

    // Not implemented/used because we're more worried about the overlaps right now.
    // pub fn unpack(&self, input_dir: &Path, output_file: &Path) -> Result<PathBuf, std::io::Error> {
    //     println!("Unpacking repository data from {} to {}...", input_dir.display(), output_file.display());
    //     if input_dir.exists() {

    //         println!("Conceptual unpack: If this were a real unpack, files from {} would be reconstructed to {}.", input_dir.display(), output_file.display());
    //         Ok(output_file.to_path_buf())
    //     } else {
    //         Err(Error::new(ErrorKind::NotFound, format!("Input directory {} for unpack not found.", input_dir.display())))
    //     }
    // }
}