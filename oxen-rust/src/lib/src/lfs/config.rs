use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::OxenError;

const LFS_CONFIG_FILENAME: &str = "lfs.toml";

/// Configuration stored in `.oxen/lfs.toml` within a Git repository.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LfsConfig {
    /// Optional Oxen remote URL for push/pull of large file content.
    pub remote_url: Option<String>,
}

impl LfsConfig {
    /// Load from `<oxen_dir>/lfs.toml`. Returns defaults if the file does not exist.
    pub fn load(oxen_dir: &Path) -> Result<Self, OxenError> {
        let path = oxen_dir.join(LFS_CONFIG_FILENAME);
        if !path.exists() {
            return Ok(Self::default());
        }
        let text = std::fs::read_to_string(&path)?;
        let config: LfsConfig = toml::from_str(&text).map_err(OxenError::TomlDe)?;
        Ok(config)
    }

    /// Persist to `<oxen_dir>/lfs.toml`.
    pub fn save(&self, oxen_dir: &Path) -> Result<(), OxenError> {
        let path = oxen_dir.join(LFS_CONFIG_FILENAME);
        let text = toml::to_string_pretty(self).map_err(OxenError::TomlSer)?;
        std::fs::write(&path, text)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_config_save_and_load() {
        let tmp = TempDir::new().unwrap();
        let cfg = LfsConfig {
            remote_url: Some("https://hub.oxen.ai/user/repo".to_string()),
        };
        cfg.save(tmp.path()).unwrap();

        let loaded = LfsConfig::load(tmp.path()).unwrap();
        assert_eq!(loaded.remote_url, cfg.remote_url);
    }

    #[test]
    fn test_config_load_defaults_when_missing() {
        let tmp = TempDir::new().unwrap();
        let cfg = LfsConfig::load(tmp.path()).unwrap();
        assert!(cfg.remote_url.is_none());
    }
}
