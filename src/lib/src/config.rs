//! Configuration for Oxen, including user configuration and remote host configuration
//!

pub mod auth_config;
pub mod embedding_config;
pub mod endpoint;
pub mod repository_config;
pub mod runtime_config;
pub mod user_config;

pub use crate::config::auth_config::AuthConfig;
pub use crate::config::auth_config::AUTH_CONFIG_FILENAME;

pub use crate::config::embedding_config::EmbeddingConfig;
pub use crate::config::embedding_config::EMBEDDING_CONFIG_FILENAME;

pub use crate::config::user_config::UserConfig;
pub use crate::config::user_config::USER_CONFIG_FILENAME;

pub use crate::config::repository_config::RepositoryConfig;

pub use crate::config::runtime_config::RuntimeConfig;
