use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::{LocalStorageOpts, S3Opts};
use crate::storage::StorageConfig;
use crate::{constants, util};

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use utoipa::ToSchema;

#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct StorageOpts {
    pub type_: String,
    pub local_storage_opts: Option<LocalStorageOpts>,
    pub s3_opts: Option<S3Opts>,
}

impl StorageOpts {
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

                log::debug!("Storage backend is in S3 Bucket: {bucket}, Prefix: {prefix}",);

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

    pub fn from_args(
        storage_backend: Option<String>,
        storage_backend_path: Option<String>,
        storage_backend_bucket: Option<String>,
    ) -> Result<Option<StorageOpts>, OxenError> {
        // parse storage backend options
        if let Some(backend) = storage_backend {
            match backend.as_str() {
                "local" => {
                    if storage_backend_bucket.is_some() {
                        return Err(OxenError::basic_str("Error: storage-backend-bucket should not be set when storage-backend is local"));
                    }
                    if storage_backend_path.is_some() {
                        let local_storage_opts = Some(LocalStorageOpts {
                            path: Some(PathBuf::from(storage_backend_path.unwrap())),
                        });
                        Ok(Some(StorageOpts {
                            type_: "local".to_string(),
                            local_storage_opts,
                            s3_opts: None,
                        }))
                    } else {
                        Ok(None)
                    }
                }
                "s3" => {
                    let bucket = storage_backend_bucket.ok_or(OxenError::basic_str(
                        "storage-backend-bucket is required when storage-backend is s3",
                    ))?;

                    let prefix = storage_backend_path.ok_or(OxenError::basic_str(
                        "storage-backend-path is required when storage-backend is s3",
                    ))?;

                    Ok(Some(StorageOpts {
                        type_: "s3".to_string(),
                        local_storage_opts: None,
                        s3_opts: Some(S3Opts {
                            bucket: bucket.clone(),
                            prefix: Some(prefix.clone()),
                        }),
                    }))
                }
                _ => Err(OxenError::basic_str(
                    "storage-backend can only be local or s3",
                )),
            }
        } else {
            Ok(None)
        }
    }
}
