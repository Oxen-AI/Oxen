use crate::{api, util};

use std::path::{Path, PathBuf};
use crate::repositories::LocalRepository;
use crate::repositories::OxenError;
use glob::glob;

pub async fn restore(repo: &LocalRepository, path: &PathBuf, revision: &String) -> Result<(), OxenError> {
    
    let repo_path = repo.path.clone();
    let remote_repo = api::client::repositories::get_default_remote(&repo).await?;

    let path_str = &path
            .to_str()
            .ok_or_else(|| OxenError::basic_str("Invalid path string"))?;

    let mut paths: Vec<PathBuf> =  vec![];
    
    if util::fs::is_glob_path(path_str) {
        for entry in glob(&path_str)? {
            let entry_path = entry?;
            let relative_path = util::fs::path_relative_to_dir(&entry_path, &repo_path)?;
            paths.push(relative_path);
        }
    } else {
        let relative_path = util::fs::path_relative_to_dir(&path, &repo_path)?;
        paths.push(relative_path);
    }

    for path in paths {
        let dst = if path.is_dir() {
            path.clone()
        } else {
            if let Some(parent) = path.parent() {
                if parent.is_file() {
                    return Err(OxenError::basic_str(format!(
                        "{:?} is not a directory",
                        parent
                    )));
                }

                // * if the dst parent does not exist, we error because cannot copy a directory to a non-existent location
                if !parent.exists() && parent != Path::new("") {
                    return Err(OxenError::basic_str(format!("{:?} does not exist", parent)));
                }

                parent.to_path_buf()
            } else {
                return Err(OxenError::basic_str(format!("file {path:?} has no parent")))
            }
        };

        api::client::entries::download_entry(&remote_repo, &path, &dst, &revision).await?

    }


    Ok(())
}