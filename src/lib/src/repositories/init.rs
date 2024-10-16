//! # oxen init
//!
//! Initialize a local oxen repository
//!

use std::path::Path;

use crate::constants::MIN_OXEN_VERSION;
use crate::core;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;

/// # Initialize an Empty Oxen Repository
/// ```
/// # use liboxen::command;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # use liboxen::test;
/// # fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
/// let base_dir = Path::new("repo_dir_init");
/// command::repositori(base_dir)?;
/// assert!(base_dir.join(".oxen").exists());
/// # util::fs::remove_dir_all(base_dir)?;
/// # Ok(())
/// # }
/// ```
pub fn init(path: impl AsRef<Path>) -> Result<LocalRepository, OxenError> {
    init_with_version(path, MIN_OXEN_VERSION)
}

pub fn init_with_version(
    path: impl AsRef<Path>,
    version: MinOxenVersion,
) -> Result<LocalRepository, OxenError> {
    let path = path.as_ref();
    match version {
        MinOxenVersion::V0_10_0 => core::v0_10_0::init(path),
        MinOxenVersion::V0_19_0 => core::v0_19_0::init(path),
    }
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;
    use crate::util;

    #[test]
    fn test_command_init() -> Result<(), OxenError> {
        test::run_empty_dir_test(|repo_dir| {
            // Init repo
            repositories::init(repo_dir)?;

            // Init should create the .oxen directory
            let hidden_dir = util::fs::oxen_hidden_dir(repo_dir);
            let config_file = util::fs::config_filepath(repo_dir);
            assert!(hidden_dir.exists());
            assert!(config_file.exists());

            Ok(())
        })
    }
}
