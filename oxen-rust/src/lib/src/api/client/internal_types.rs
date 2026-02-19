use std::path::PathBuf;

use crate::{error::OxenError, model::LocalRepository};

#[derive(Debug, Clone)]
pub(crate) enum LocalOrBase {
    Local(LocalRepository),
    Base(PathBuf),
}

impl LocalOrBase {
    pub(crate) fn local_repo(lr: LocalRepository) -> Self {
        Self::Local(lr)
    }

    pub(crate) fn base_dir(base_dir: PathBuf) -> Result<Self, OxenError> {
        if base_dir.is_dir() {
            Ok(Self::Base(base_dir))
        } else {
            Err(OxenError::basic_str(format!(
                "base directory is not a directory: {}",
                base_dir.display()
            )))
        }
    }
}
