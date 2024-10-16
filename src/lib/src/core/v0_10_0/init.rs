use std::path::Path;

use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::{constants, util};

pub fn init(path: impl AsRef<Path>) -> Result<LocalRepository, OxenError> {
    let path = path.as_ref();
    let hidden_dir = util::fs::oxen_hidden_dir(path);
    if hidden_dir.exists() {
        let err = format!("Oxen repository already exists: {path:?}");
        return Err(OxenError::basic_str(err));
    }

    // Cleanup the .oxen dir if init fails
    match p_init(path) {
        Ok(result) => Ok(result),
        Err(error) => {
            util::fs::remove_dir_all(hidden_dir)?;
            Err(error)
        }
    }
}

fn p_init(path: impl AsRef<Path>) -> Result<LocalRepository, OxenError> {
    let path = path.as_ref();
    let hidden_dir = util::fs::oxen_hidden_dir(path);

    std::fs::create_dir_all(hidden_dir)?;
    let config_path = util::fs::config_filepath(path);

    // Instantiate this one from the older v0.10.0 min version
    let repo = LocalRepository::new_from_version(path, "0.10.0")?;
    repo.save(&config_path)?;

    // In older versions we make the initial commit for the users
    super::commits::commit_with_no_files(&repo, constants::INITIAL_COMMIT_MSG)?;

    Ok(repo)
}
