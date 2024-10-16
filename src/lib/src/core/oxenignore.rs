use ignore::gitignore::Gitignore;

use crate::constants;
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
