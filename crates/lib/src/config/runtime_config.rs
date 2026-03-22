pub mod platform;
pub mod runtime;

use crate::config::runtime_config::platform::Platform;
use crate::config::runtime_config::runtime::Runtime;
use crate::error::OxenError;
use std::sync::OnceLock;
use std::sync::RwLock;

const VERSION: &str = crate::constants::OXEN_VERSION;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub runtime_name: Runtime,
    pub runtime_version: String,
    pub host_platform: Platform,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        RuntimeConfig {
            runtime_name: Runtime::CLI,
            runtime_version: String::from(VERSION),
            host_platform: Platform::from_os_identifier(std::env::consts::OS),
        }
    }
}

static RUNTIME_CONFIG: OnceLock<RwLock<RuntimeConfig>> = OnceLock::new();

fn get_runtime_config() -> &'static RwLock<RuntimeConfig> {
    RUNTIME_CONFIG.get_or_init(|| RwLock::new(RuntimeConfig::default()))
}

impl RuntimeConfig {
    pub fn set(runtime_name: String, runtime_version: String) -> Result<(), OxenError> {
        let config = RuntimeConfig {
            runtime_name: Runtime::from_runtime_name(runtime_name.as_str()),
            runtime_version,
            host_platform: Platform::from_os_identifier(std::env::consts::OS),
        };

        Self::update(config)
    }

    pub fn get() -> Result<RuntimeConfig, OxenError> {
        get_runtime_config()
            .read()
            .map(|config| config.clone())
            .map_err(|_| OxenError::basic_str("Failed to read global configuration"))
    }

    pub fn update(config: RuntimeConfig) -> Result<(), OxenError> {
        if let Ok(mut runtime_config) = get_runtime_config().write() {
            *runtime_config = config;
            Ok(())
        } else {
            Err(OxenError::basic_str(
                "Failed to update global configuration",
            ))
        }
    }
}
