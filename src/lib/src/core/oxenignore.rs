use ignore::gitignore::Gitignore;
use std::path::Path;

use crate::constants;
use crate::constants::OXEN_HIDDEN_DIR;
use crate::model::LocalRepository;

/// Create will load the .oxenignore if it exists. If it does not exist, it will return None.
pub fn create(repo: &LocalRepository) -> Option<Gitignore> {
    let path = repo.path.join(constants::OXEN_IGNORE_FILE);
    match Gitignore::new(path) {
        (gitignore, None) => {
            // log::debug!("loaded .oxenignore file from {}", path.display());
            Some(gitignore)
        }
        (_, Some(err)) => {
            log::debug!("Could not open .oxenignore file. Reason: {}", err);
            None
        }
    }
}

/// Check if a path should be ignored based on .oxenignore rules
pub fn is_ignored(path: &Path, gitignore: &Option<Gitignore>, is_dir: bool) -> bool {
    // Skip hidden .oxen files
    if path.starts_with(OXEN_HIDDEN_DIR) {
        return true;
    }
    if let Some(gitignore) = gitignore {
        if gitignore
            .matched_path_or_any_parents(path, is_dir)
            .is_ignore()
        {
            return true;
        }
    }
    false
}
