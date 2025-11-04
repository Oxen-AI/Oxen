use crate::opts::{LocalStorageOpts, S3Opts}
use crate::storage::StorageConfig;

#[derive(Clone, Debug)]
pub struct StorageOpts {
    pub local_storage_opts: Option<LocalStorageOpts>,
    pub s3_opts: Option<S3Opts>,
}

impl StorageOpts {
    pub fn from_repo_config(repo: &LocalRepository, config: &StorageConfig) -> Result<StorageOpts, OxenError> {
        match config.type_ {
            "local" => {
                let local_storage_opts = LocalStorageOpts {
                    path: Some(repo.path)
                };

                StorageOpts {
                    local_storage_opts,
                    s3_opts: None,
                }
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
                    bucket,
                    prefix: Some(prefix),
                };

                StorageOpts {
                    local_storage_opts: None,
                    s3_opts,
                }
            }
            _ => {
                Err(OxenError::basic_str(format!(
                    "Unsupported async storage type: {}",
                    config.type_
                )))
            }
        }
    } 

    pub fn from_path(path: &PathBuf) -> StorageOpts {
        let local_storage_opts = LocalStorage {
            path: Some(path)
        };

        StorageOpts {
            local_storage_opts,
            s3_opts: None,
        }
    }
}
