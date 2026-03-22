//! Oxen User Functions
//!

use crate::error::PyOxenError;
use crate::py_user::PyUser;
use liboxen::config::user_config::UserConfig;
use liboxen::model::User;
use pyo3::prelude::*;
use std::path::Path;

#[pyfunction]
pub fn config_user(name: String, email: String, path: String) -> Result<PyUser, PyOxenError> {
    let final_path = Path::new(&path);
    let user = User { name, email };
    let config = UserConfig::from_user(&user);
    config.save(final_path)?;
    Ok(user.into())
}

#[pyfunction]
pub fn current_user(path: String) -> Result<PyUser, PyOxenError> {
    let path = Path::new(&path);
    let config = UserConfig::new(path);
    Ok(config.to_user().into())
}
