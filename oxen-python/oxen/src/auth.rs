//! Oxen Auth tools
//!

use crate::error::PyOxenError;
use liboxen::config::auth_config::AuthConfig;
use pyo3::prelude::*;
use std::path::Path;

#[pyfunction]
pub fn config_auth(host: String, token: String, path: String) -> Result<(), PyOxenError> {
    let final_path = Path::new(&path);
    // Create parent dir if not exists
    if let Some(parent) = final_path.parent() {
        match std::fs::create_dir_all(parent) {
            Ok(_) => (),
            Err(e) => {
                log::error!("Failed to create parent dir: {}", e);
            }
        }
    }

    let mut config = AuthConfig::get_or_create()?;
    config.add_host_auth_token(host, token);
    config.save(final_path)?;
    Ok(())
}
