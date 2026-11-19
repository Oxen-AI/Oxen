//! Server-side configuration.
//!
//! - [`Config`] — the TOML deserialize target for `--config`.
//! - [`StoragePolicy`] — the version store storage policy for the server.
//! - [`IdentityPolicy`] — who assigns the UUIDs repositories are identified by.

pub mod identity_policy;
pub mod storage_policy;

use crate::config::identity_policy::IdentityPolicy;
use crate::config::storage_policy::StoragePolicy;
use serde::Deserialize;

/// TOML deserialization target for the server's `--config` flag.
///
/// Future top-level keys (auth options, telemetry settings, etc.) land here without restructuring.
/// Tables and keys this build doesn't know are ignored.
/// A value it can't parse for a key it knows still fails startup.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub storage: StoragePolicy,
    #[serde(default)]
    pub identity: IdentityPolicy,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::identity_policy::IdentitySource;

    #[test]
    fn parse_empty_file_uses_defaults() {
        let f: Config = toml::from_str("").expect("an empty file parses");
        assert_eq!(f.storage, StoragePolicy::default());
        assert_eq!(f.identity, IdentityPolicy::default());
    }

    #[test]
    fn parse_identity_section() {
        let f: Config = toml::from_str(
            r#"
            [identity]
            repo_uuids_assigned_by = "auth-provider"
        "#,
        )
        .unwrap();
        assert_eq!(
            f.identity.repo_uuids_assigned_by(),
            IdentitySource::AuthProvider
        );
    }

    #[test]
    fn parse_minimal_valid_file_round_trips() {
        let toml_str = r#"
            [storage]
            backends = ["local", "s3"]
            s3_bucket = "my-bucket"
            s3_region = "us-west-1"
        "#;
        let f: Config = toml::from_str(toml_str).unwrap();
        let expected: StoragePolicy = toml::from_str(
            r#"
            backends = ["local", "s3"]
            s3_bucket = "my-bucket"
            s3_region = "us-west-1"
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

    #[test]
    fn ignores_an_unrecognized_section() {
        let f: Config = toml::from_str(
            "[storaje]\nbackends = [\"s3\"]\ns3_bucket = \"b\"\ns3_region = \"r\"\n",
        )
        .expect("an unrecognized section is ignored");
        assert_eq!(
            f.storage,
            StoragePolicy::default(),
            "a misspelled [storage] leaves the default policy in place of the one it names"
        );
    }
}
