use crate::constants::{CONFIG_DIR, OXEN};
use crate::error::OxenError;
use crate::model::User;
use crate::util;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

pub const USER_CONFIG_FILENAME: &str = "user_config.toml";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserConfig {
    pub name: String,
    pub email: String,
    #[serde(default)]
    pub editor: Option<String>,
}

impl UserConfig {
    pub fn new(path: &Path) -> UserConfig {
        let contents = util::fs::read_from_path(path).unwrap();
        toml::from_str(&contents).unwrap()
    }

    pub fn from_user(user: &User) -> UserConfig {
        UserConfig {
            name: user.name.to_owned(),
            email: user.email.to_owned(),
            editor: None,
        }
    }

    pub fn to_user(&self) -> User {
        User {
            name: self.name.to_owned(),
            email: self.email.to_owned(),
        }
    }

    fn new_empty() -> UserConfig {
        UserConfig {
            name: String::from(""),
            email: String::from(""),
            editor: None,
        }
    }

    pub fn get() -> Result<UserConfig, OxenError> {
        let config_dir = util::fs::oxen_config_dir()?;
        let mut config_file = config_dir.join(Path::new(USER_CONFIG_FILENAME));
        if std::env::var("TEST").is_ok() {
            config_file = PathBuf::from("data/test/config/user_config.toml");
        }
        log::debug!("looking for config file in...{config_file:?}");
        if config_file.exists() {
            Ok(UserConfig::new(&config_file))
        } else {
            log::debug!(
                "unable to find config file at {:?}. Current working directory is {:?}",
                config_file,
                std::env::current_dir().unwrap()
            );
            Err(OxenError::email_and_name_not_set())
        }
    }

    pub fn identifier() -> Result<String, OxenError> {
        Ok(util::hasher::hash_str_sha256(
            UserConfig::get()?.to_user().email,
        ))
    }

    pub fn get_or_create() -> Result<UserConfig, OxenError> {
        match Self::get() {
            Ok(config) => Ok(config),
            Err(_err) => {
                let config = Self::new_empty();
                config.save_default()?;
                println!("🐂 created a new config file in \"$HOME/{CONFIG_DIR}/{OXEN}/{USER_CONFIG_FILENAME}");
                Ok(config)
            }
        }
    }

    pub fn save_default(&self) -> Result<(), OxenError> {
        let config_dir = util::fs::oxen_config_dir()?;
        let config_file = config_dir.join(Path::new(USER_CONFIG_FILENAME));
        log::debug!("Saving config to {config_file:?}");
        if !config_dir.exists() {
            fs::create_dir_all(config_dir)?;
        }
        self.save(&config_file)?;
        Ok(())
    }

    pub fn save(&self, path: &Path) -> Result<(), OxenError> {
        let toml = toml::to_string(&self)?;
        util::fs::write_to_path(path, toml)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::config::UserConfig;
    use crate::error::OxenError;
    use crate::test;
    use std::path::Path;

    #[test]
    fn test_read() {
        let config = UserConfig::new(&test::user_cfg_file());
        assert!(!config.name.is_empty());
        assert!(!config.email.is_empty());
    }

    #[test]
    fn test_save() -> Result<(), OxenError> {
        let final_path = Path::new("test_save_config.toml");
        let orig_config = UserConfig::new(&test::user_cfg_file());

        orig_config.save(final_path)?;

        let config = UserConfig::new(final_path);
        assert!(!config.name.is_empty());

        std::fs::remove_file(final_path)?;
        Ok(())
    }
}
