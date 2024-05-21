//! Configuration for Oxen, including user configuration and remote host configuration
//!

pub mod auth_config;
pub mod endpoint;
pub mod remote_config;
pub mod user_config;

pub use crate::config::remote_config::RemoteConfig;

pub use crate::config::user_config::UserConfig;
pub use crate::config::user_config::USER_CONFIG_FILENAME;

pub use crate::config::auth_config::AuthConfig;
pub use crate::config::auth_config::AUTH_CONFIG_FILENAME;
