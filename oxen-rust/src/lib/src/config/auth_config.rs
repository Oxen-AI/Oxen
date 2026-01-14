use crate::constants::{CONFIG_DIR, DEFAULT_HOST, OXEN};
use crate::error::OxenError;
use crate::util;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

pub const AUTH_CONFIG_FILENAME: &str = "auth_config.toml";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HostConfig {
    pub host: String,
    pub auth_token: Option<String>,
}

impl HostConfig {
    pub fn from_host(host: &str) -> HostConfig {
        HostConfig {
            host: String::from(host),
            auth_token: None,
        }
    }
}

// Hash on the id field so we can quickly look up
impl PartialEq for HostConfig {
    fn eq(&self, other: &HostConfig) -> bool {
        self.host == other.host
    }
}
impl Eq for HostConfig {}
impl Hash for HostConfig {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.host.hash(state);
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AuthConfig {
    pub default_host: Option<String>,
    pub host_configs: HashSet<HostConfig>,
}

impl AuthConfig {
    pub fn new(path: &Path) -> AuthConfig {
        let contents = util::fs::read_from_path(path).unwrap();
        toml::from_str(&contents).unwrap()
    }

    pub fn new_empty() -> AuthConfig {
        AuthConfig {
            default_host: DEFAULT_HOST.to_string().into(),
            host_configs: HashSet::new(),
        }
    }

    pub fn get() -> Result<AuthConfig, OxenError> {
        let config_dir = util::fs::oxen_config_dir()?;
        let mut config_file = config_dir.join(Path::new(AUTH_CONFIG_FILENAME));
        if std::env::var("TEST").is_ok() {
            config_file = PathBuf::from("data/test/config/auth_config.toml");
        }
        log::trace!("looking for config file in...{config_file:?}");
        if config_file.exists() {
            Ok(AuthConfig::new(&config_file))
        } else {
            log::debug!(
                "unable to find authconfig file at {:?}. Current working directory is {:?}",
                config_file,
                std::env::current_dir().unwrap()
            );
            Err(OxenError::must_supply_valid_api_key())
        }
    }

    pub fn get_or_create() -> Result<AuthConfig, OxenError> {
        match Self::get() {
            Ok(config) => Ok(config),
            Err(_err) => {
                let config = Self::new_empty();
                config.save_default()?;
                println!("🐂 created a new config file in \"$HOME/{CONFIG_DIR}/{OXEN}/{AUTH_CONFIG_FILENAME}");
                Ok(config)
            }
        }
    }

    pub fn save_default(&self) -> Result<(), OxenError> {
        let config_dir = util::fs::oxen_config_dir()?;
        let config_file = config_dir.join(Path::new(AUTH_CONFIG_FILENAME));
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

    pub fn add_host_auth_token<S: AsRef<str>>(&mut self, host: S, token: S) {
        let host = host.as_ref();
        self.host_configs.replace(HostConfig {
            host: String::from(host),
            auth_token: Some(String::from(token.as_ref())),
        });
    }

    pub fn auth_token_for_host<S: AsRef<str>>(&self, host: S) -> Option<String> {
        let host = host.as_ref();
        if let Some(token) = self.host_configs.get(&HostConfig::from_host(host)) {
            if token.auth_token.is_none() {
                log::trace!("no auth_token found for host \"{}\"", token.host);
            }
            token.auth_token.clone()
        } else {
            log::trace!("no host configuration found for {host}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::AuthConfig;
    use crate::error::OxenError;
    use crate::test;
    #[test]
    fn test_second_auth_should_overwrite_first() -> Result<(), OxenError> {
        let mut auth_config = AuthConfig::new(&test::auth_cfg_file());
        let og_num_configs = auth_config.host_configs.len();

        let host = "hub.oxen.ai";
        let token_1 = "1234";
        let token_2 = "5678";
        auth_config.add_host_auth_token(host, token_1);
        auth_config.add_host_auth_token(host, token_2);

        assert_eq!(auth_config.host_configs.len(), og_num_configs + 1);
        assert_eq!(
            auth_config.auth_token_for_host(host),
            Some(token_2.to_string())
        );

        Ok(())
    }
}
