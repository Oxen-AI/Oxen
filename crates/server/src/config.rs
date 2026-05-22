//! Server-side configuration.
//!
//! - [`Config`] — the TOML deserialize target for `--config`.
//! - [`storage::StoragePolicy`] — the version store storage policy for the server.

pub mod storage_policy;

use crate::config::storage_policy::StoragePolicy;
use serde::Deserialize;

/// TOML deserialization target for the server's `--config` flag.
///
/// Future top-level keys (auth options, telemetry settings, etc.) land here without restructuring.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub storage: StoragePolicy,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_file_uses_default_storage() {
        let f: Config = toml::from_str("").unwrap();
        assert_eq!(f.storage, StoragePolicy::default());
    }

    #[test]
    fn parse_minimal_valid_file_round_trips() {
        let toml_str = r#"
            [storage]
            backends = ["local", "s3"]
            s3_bucket = "my-bucket"
        "#;
        let f: Config = toml::from_str(toml_str).unwrap();
        let expected: StoragePolicy = toml::from_str(
            r#"
            backends = ["local", "s3"]
            s3_bucket = "my-bucket"
        "#,
        )
        .unwrap();
        assert_eq!(f.storage, expected);
    }

    #[test]
    fn parse_storage_section_validates() {
        // S3 in backends without a bucket — the nested `StoragePolicy` deserializer's
        // `TryFrom` impl fires and the error bubbles up through the parent.
        let err = toml::from_str::<Config>(
            r#"
            [storage]
            backends = ["s3"]
        "#,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("s3 bucket cannot be empty"),
            "expected EmptyS3Bucket message, got: {err}",
        );
    }
}
