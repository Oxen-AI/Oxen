use std::fs::File;
use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::time::{Instant};

use liboxen::model::{ LocalRepository};
use liboxen::repositories::{self, commits};
use serde::{Serialize, Deserialize};

use crate::chunker::{get_chunker};

use super::Algorithm;
 
const VERSION_FILE_NAME: &str = "data";
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

    fn version_dir(&self, hash: &str) -> PathBuf {
        let topdir = &hash[..2];
        let subdir = &hash[2..];
        self.root_path.join(topdir).join(subdir)
    }

    fn version_path(&self, hash: &str) -> PathBuf {
        self.version_dir(hash).join(VERSION_FILE_NAME)
    }

    fn count_commit_overlap(commit1: &Vec<String>, commit2: &Vec<String>) -> Result<usize, Error> {
        // let commit1_map: std::collections::HashSet<_> = commit1.iter().cloned().collect();
        // let commit2_map: std::collections::HashSet<_> = commit2.iter().cloned().collect();

        // let overlap = commit1_map.intersection(&commit2_map).count();

        let mut overlap = 0;
        for hash in commit1 {
            if commit2.contains(hash) {
                overlap += 1;
            }
        }

        Ok(overlap)
    }


    pub fn pack(&self, algo: Algorithm, chunk_size: usize, input_file: &Path, output_dir: &Path, n: u8 ) -> Result<PathBuf, Error> {
        
        std::fs::create_dir_all(output_dir)?;

        let input = File::open(input_file)?;
        let _metadata = input.metadata()?;
        let _paths = vec![input_file];

        let repo = LocalRepository::from_current_dir().map_err(|e| Error::new(ErrorKind::NotFound, format!("error loading repository: {}", e) ))?;

        let chunker = get_chunker(&algo, chunk_size).map_err(|e| Error::new(ErrorKind::NotFound, format!("error fetching chunker:  {}", e)))?;

        let commits = commits::list(&repo).map_err(|e| Error::new(ErrorKind::NotFound, format!("error listing commit: {}", e)))?;

        let latest_n_commits = commits.iter().take(n as usize).collect::<Vec<_>>();

        let mut commit_count = 0;
        let mut previous_commit = latest_n_commits[0].clone();
        let mut previous_commit_hash = latest_n_commits[0].hash().map_err(|e| Error::new(ErrorKind::NotFound, format!("error fetching commit: {}", e)))?;

        for commit in latest_n_commits {

            let commit_hash = commit.hash().map_err(|e| Error::new(ErrorKind::NotFound, format!("error fetching commit: {}", e)))?;
            let commit_output_dir = output_dir.join(&commit_hash.to_string());
            
            std::fs::create_dir_all(&commit_output_dir)?;

            let file_location = repositories::tree::get_file_by_path(&repo, commit, input_file);

            match file_location {
                Ok(Some(file)) => {
                    // println!("Found file: {:?}", file);
                    let hash = file.hash().to_string();
                    let file_path = self.version_path(&hash);
                    // println!("File path : {:?}", file_path.display());

                    let pack_start_time = Instant::now();

                    chunker.pack(&file_path, &commit_output_dir)?;

                    let pack_duration = pack_start_time.elapsed();
                    println!("Packed commit {:?} in {:?}", hash, pack_duration);
                }
                Ok(None) => {
                    println!("No file found for path: {:?}", input_file);
                }
                Err(e) => {
                    eprintln!("Error getting file for path {:?}: {:?}", input_file, e);
                }
            }            

            if commit_count >= 1 {
                
                let commit1 = previous_commit;
                let commit2 = commit;

                let commit1_hashes = chunker.get_chunk_hashes(&output_dir.join(previous_commit_hash.to_string())).map_err(|e| Error::new(ErrorKind::NotFound, format!("error getting chunk hashes for hash {} : {}", &commit1.to_string(), e)))?;
                let commit2_hashes = chunker.get_chunk_hashes(&commit_output_dir).map_err(|e| Error::new(ErrorKind::NotFound, format!("error getting chunk hashes: {}", e)))?;
                
                println!("Commit {} hash_count: {:?}", commit1, commit1_hashes.len());
                // println!("commits {:?}", commit1_hashes);
                // Save the hashes to a file
                let commit1_hashes_path = commit_output_dir.join(format!("{}_hashes.txt", commit1));
                std::fs::write(&commit1_hashes_path, format!("{:?}", commit1_hashes))?;
                println!("Commit {} hash_count: {:?}", commit2, commit2_hashes.len());
                let commit2_hashes_path = commit_output_dir.join(format!("{}_hashes.txt", commit2));
                std::fs::write(&commit2_hashes_path, format!("{:?}", commit2_hashes))?;
                // println!("commits {:?}", commit2_hashes);

                let overlap = Self::count_commit_overlap(&commit1_hashes, &commit2_hashes).map_err(|e| Error::new(ErrorKind::NotFound, format!("error counting commit overlap: {}", e)))?;
                
                if overlap > 0 {
                    println!("Commit {} and {} have {} overlapping chunks out of {}", commit1, commit2, overlap, commit1_hashes.len());
                } else {
                    println!("Commit {} and {} have no overlapping chunks", commit1, commit2);
                }
            }
            commit_count += 1;
            previous_commit = commit.clone();
            previous_commit_hash = commit.hash().map_err(|e| Error::new(ErrorKind::NotFound, format!("error fetching commit: {}", e)))?;

        }

        println!("Packing repository data with content-defined chunking...");

        Ok(output_dir.to_path_buf())
    }

    pub fn unpack(&self, input_dir: &Path, output_file: &Path) -> Result<PathBuf, std::io::Error> {

        println!("Unpacking repository data with content-defined chunking... {}", input_dir.display());
        Ok(output_file.to_path_buf())
    }
}