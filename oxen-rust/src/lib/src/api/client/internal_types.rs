use std::path::PathBuf;

use crate::model::LocalRepository;

#[derive(Debug, Clone)]
pub(crate) enum LocalOrBase {
    Local(LocalRepository),
    Base(PathBuf),
}
