use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::{LocalStorageOpts, S3Opts};
use crate::storage::StorageConfig;
use crate::{constants, util};

use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Default)]
pub struct StorageOpts {
    pub type_: String,
    pub local_storage_opts: Option<LocalStorageOpts>,
    pub s3_opts: Option<S3Opts>,
}

impl StorageOpts {
    // Defaults to local storage
    pub fn new() -> StorageOpts {
        let local_storage_opts = LocalStorageOpts { path: None };

        StorageOpts {
            type_: "local".to_string(),
            local_storage_opts: Some(local_storage_opts),
            s3_opts: None,
        }
    }

    pub fn from_repo_config(
        repo: &LocalRepository,
        config: &StorageConfig,
    ) -> Result<StorageOpts, OxenError> {
        match config.type_.as_str() {
            "local" => {
                // Take the version store path from the config if specified
                // Otherwise, default to the repo hidden dir
                let version_path = if let Some(path) = config.settings.get("path") {
                    PathBuf::from(path)
                } else {
                    let repo_path = util::fs::oxen_hidden_dir(&repo.path);
                    repo_path
                        .join(constants::VERSIONS_DIR)
                        .join(constants::FILES_DIR)
                };

                let local_storage_opts = LocalStorageOpts {
                    path: Some(version_path),
                };

                Ok(StorageOpts {
                    type_: "local".to_string(),
                    local_storage_opts: Some(local_storage_opts),
                    s3_opts: None,
                })
            }
            "s3" => {
                let bucket = config
                    .settings
                    .get("bucket")
                    .ok_or_else(|| OxenError::basic_str("S3 bucket not specified"))?;
                let prefix = config
                    .settings
                    .get("prefix")
                    .cloned()
                    .unwrap_or_else(|| String::from("versions"));

                let s3_opts = S3Opts {
                    bucket: bucket.to_string(),
                    prefix: Some(prefix),
                };

                Ok(StorageOpts {
                    type_: "s3".to_string(),
                    local_storage_opts: None,
                    s3_opts: Some(s3_opts),
                })
            }
            _ => Err(OxenError::basic_str(format!(
                "Unsupported async storage type: {}",
                config.type_
            ))),
        }
    }

    pub fn from_path(path: &Path, is_repo_dir: bool) -> StorageOpts {
        let version_path = if is_repo_dir {
            let repo_path = util::fs::oxen_hidden_dir(path);

            repo_path
                .join(constants::VERSIONS_DIR)
                .join(constants::FILES_DIR)
        } else {
            path.to_path_buf()
        };

        let local_storage_opts = LocalStorageOpts {
            path: Some(version_path),
        };

        StorageOpts {
            type_: "local".to_string(),
            local_storage_opts: Some(local_storage_opts),
            s3_opts: None,
        }
    }
}
