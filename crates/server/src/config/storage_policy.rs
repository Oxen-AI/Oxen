//! Server-side storage policy.
//!
//! [`StoragePolicy`] is the admin's "what backends will this server host?" answer.
//! Its methods validate caller-supplied storage requests against that answer: empty
//! requests fall back to the server's default, disallowed requests yield a 400
//! BadRequest whose body lists the allowed backends.
//!
//! Single source of truth: this type is the TOML deserialize target, the validated
//! in-memory representation, and the value carried on
//! [`crate::app_data::OxenAppData`]. Construction goes through either
//! [`Default::default`] or the serde path (which funnels through [`TryFrom`] on the
//! private [`StoragePolicyRaw`] helper for validation).

use std::collections::HashSet;

use liboxen::storage::StorageKind;
use serde::Deserialize;

use crate::errors::OxenHttpError;

/// Admin-only S3 configuration. The bucket is server-wide; per-repo prefixes are derived from
/// `{namespace}/{name}` when the version store is constructed (not yet wired up).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Opts {
    pub bucket: String,
}

/// Server-side storage policy.
///
/// The default kind is the first element of `backends = [...]` in the TOML.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "StoragePolicyRaw")]
pub struct StoragePolicy {
    default: StorageKind,
    local: bool,
    s3: Option<S3Opts>,
}

#[derive(Deserialize)]
struct StoragePolicyRaw {
    backends: Vec<StorageKind>,
    #[serde(default)]
    s3_bucket: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoragePolicyError {
    /// `StoragePolicy` rejected an admin config with an empty `[storage] backends` list.
    #[error("Storage policy: at least one backend must be configured under [storage] backends")]
    NoBackends,

    /// `StoragePolicy` rejected an admin config that lists the same backend twice under
    /// `[storage] backends`.
    #[error("Storage policy: backend '{0}' appears multiple times in [storage] backends")]
    DuplicateBackend(StorageKind),

    /// `StoragePolicy` rejected an admin config that includes the S3 backend without a
    /// bucket name (or with an empty one).
    #[error("Storage policy: s3 bucket cannot be empty when the s3 backend is allowed")]
    EmptyS3Bucket,
}

impl TryFrom<StoragePolicyRaw> for StoragePolicy {
    type Error = StoragePolicyError;

    fn try_from(raw: StoragePolicyRaw) -> Result<Self, StoragePolicyError> {
        if raw.backends.is_empty() {
            return Err(StoragePolicyError::NoBackends);
        }
        let mut seen = HashSet::new();
        for kind in &raw.backends {
            if !seen.insert(*kind) {
                return Err(StoragePolicyError::DuplicateBackend(*kind));
            }
        }
        let default = raw.backends[0];
        let local = raw.backends.contains(&StorageKind::Local);
        let s3 = if raw.backends.contains(&StorageKind::S3) {
            if raw.s3_bucket.is_empty() {
                return Err(StoragePolicyError::EmptyS3Bucket);
            }
            Some(S3Opts {
                bucket: raw.s3_bucket,
            })
        } else {
            // Orphan bucket (set when S3 isn't in backends) is silently dropped so
            // admins toggling backends on/off during config iteration don't trip a
            // validation error.
            None
        };
        Ok(Self { default, local, s3 })
    }
}

impl Default for StoragePolicy {
    fn default() -> Self {
        Self {
            default: StorageKind::Local,
            local: true,
            s3: None,
        }
    }
}

impl StoragePolicy {
    fn default_kind(&self) -> StorageKind {
        self.default
    }

    fn is_allowed(&self, kind: StorageKind) -> bool {
        match kind {
            StorageKind::Local => self.local,
            StorageKind::S3 => self.s3.is_some(),
        }
    }

    /// Resolve the storage backend kind to use for a new repo.
    ///
    /// - `requested = None`: returns the server's default.
    /// - `requested = Some(k)`: returns `k` iff allowed; otherwise `BadRequest` (400).
    ///
    /// Callers writing the result back to a `RepoNew` should treat the caller's
    /// `storage_kind` as a *request* — the server's policy decides what actually gets
    /// persisted (defense in depth).
    pub fn resolve(&self, requested: Option<StorageKind>) -> Result<StorageKind, OxenHttpError> {
        let kind = requested.unwrap_or_else(|| self.default_kind());
        if self.is_allowed(kind) {
            Ok(kind)
        } else {
            let mut allowed = Vec::with_capacity(2);
            if self.local {
                allowed.push(StorageKind::Local.to_string());
            }
            if self.s3.is_some() {
                allowed.push(StorageKind::S3.to_string());
            }
            Err(OxenHttpError::BadRequest(
                format!(
                    "Storage backend '{kind}' is not enabled on this server. Allowed backends: [{}]",
                    allowed.join(", "),
                )
                .into(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s3_opts(bucket: &str) -> S3Opts {
        S3Opts {
            bucket: bucket.to_string(),
        }
    }

    fn raw(backends: Vec<StorageKind>, s3_bucket: &str) -> StoragePolicyRaw {
        StoragePolicyRaw {
            backends,
            s3_bucket: s3_bucket.to_string(),
        }
    }

    fn cfg(backends: Vec<StorageKind>, s3_bucket: &str) -> StoragePolicy {
        StoragePolicy::try_from(raw(backends, s3_bucket)).unwrap()
    }

    #[test]
    fn default_is_local_only() {
        let c = StoragePolicy::default();
        assert!(c.local);
        assert!(c.s3.is_none());
        assert_eq!(c.default_kind(), StorageKind::Local);
    }

    #[test]
    fn try_from_local_only() {
        let c = cfg(vec![StorageKind::Local], "");
        assert!(c.local);
        assert!(c.s3.is_none());
        assert_eq!(c.default_kind(), StorageKind::Local);
    }

    #[test]
    fn try_from_s3_only() {
        let c = cfg(vec![StorageKind::S3], "oxen-test");
        assert!(!c.local);
        assert_eq!(c.s3, Some(s3_opts("oxen-test")));
        assert_eq!(c.default_kind(), StorageKind::S3);
    }

    #[test]
    fn try_from_both_local_default() {
        let c = cfg(vec![StorageKind::Local, StorageKind::S3], "oxen-test");
        assert!(c.local);
        assert_eq!(c.s3, Some(s3_opts("oxen-test")));
        assert_eq!(c.default_kind(), StorageKind::Local);
    }

    #[test]
    fn try_from_both_s3_default() {
        let c = cfg(vec![StorageKind::S3, StorageKind::Local], "oxen-test");
        assert!(c.local);
        assert_eq!(c.s3, Some(s3_opts("oxen-test")));
        assert_eq!(c.default_kind(), StorageKind::S3);
    }

    #[test]
    fn try_from_rejects_empty_backends() {
        let err = StoragePolicy::try_from(raw(vec![], "")).unwrap_err();
        assert!(matches!(err, StoragePolicyError::NoBackends));
    }

    #[test]
    fn try_from_rejects_duplicate_backend() {
        let err = StoragePolicy::try_from(raw(vec![StorageKind::Local, StorageKind::Local], ""))
            .unwrap_err();
        assert!(matches!(
            err,
            StoragePolicyError::DuplicateBackend(StorageKind::Local)
        ));
    }

    #[test]
    fn try_from_rejects_s3_in_backends_with_empty_bucket() {
        let err = StoragePolicy::try_from(raw(vec![StorageKind::S3], "")).unwrap_err();
        assert!(matches!(err, StoragePolicyError::EmptyS3Bucket));
    }

    #[test]
    fn try_from_silently_drops_orphan_s3_bucket() {
        // s3_bucket set, but S3 not in backends → bucket value is dropped.
        let c = cfg(vec![StorageKind::Local], "orphan-bucket");
        assert!(c.local);
        assert!(c.s3.is_none());
    }

    #[test]
    fn allow_matrix() {
        let local_only = cfg(vec![StorageKind::Local], "");
        assert!(local_only.is_allowed(StorageKind::Local));
        assert!(!local_only.is_allowed(StorageKind::S3));

        let s3_only = cfg(vec![StorageKind::S3], "b");
        assert!(!s3_only.is_allowed(StorageKind::Local));
        assert!(s3_only.is_allowed(StorageKind::S3));

        let both = cfg(vec![StorageKind::Local, StorageKind::S3], "b");
        assert!(both.is_allowed(StorageKind::Local));
        assert!(both.is_allowed(StorageKind::S3));
    }

    #[test]
    fn resolve_unspecified_returns_default_kind() {
        assert_eq!(
            cfg(vec![StorageKind::Local], "").resolve(None).unwrap(),
            StorageKind::Local
        );
        assert_eq!(
            cfg(vec![StorageKind::S3], "b").resolve(None).unwrap(),
            StorageKind::S3
        );
        // Both enabled → first-in-list wins as default.
        assert_eq!(
            cfg(vec![StorageKind::Local, StorageKind::S3], "b")
                .resolve(None)
                .unwrap(),
            StorageKind::Local
        );
        assert_eq!(
            cfg(vec![StorageKind::S3, StorageKind::Local], "b")
                .resolve(None)
                .unwrap(),
            StorageKind::S3
        );
    }

    #[test]
    fn resolve_returns_requested_kind_when_allowed() {
        let both = cfg(vec![StorageKind::Local, StorageKind::S3], "b");
        assert_eq!(
            both.resolve(Some(StorageKind::S3)).unwrap(),
            StorageKind::S3
        );
        assert_eq!(
            both.resolve(Some(StorageKind::Local)).unwrap(),
            StorageKind::Local
        );
    }

    #[test]
    fn resolve_disallowed_request_is_bad_request_listing_allowed_backends() {
        let cases = [
            (
                StorageKind::S3,
                cfg(vec![StorageKind::Local], ""),
                "s3",
                "local",
            ),
            (
                StorageKind::Local,
                cfg(vec![StorageKind::S3], "b"),
                "local",
                "s3",
            ),
        ];
        for (requested, c, rejected, allowed) in cases {
            let err = c.resolve(Some(requested)).unwrap_err();
            let OxenHttpError::BadRequest(msg) = err else {
                panic!("expected BadRequest, got {err:?}");
            };
            let msg = msg.to_string();
            assert!(msg.contains(rejected), "missing rejected kind: {msg}");
            assert!(msg.contains(allowed), "missing allowed list: {msg}");
        }
    }

    #[test]
    fn deserialize_minimum_valid() {
        let toml_str = r#"backends = ["local"]"#;
        let c: StoragePolicy = toml::from_str(toml_str).unwrap();
        assert_eq!(c, StoragePolicy::default());
    }

    #[test]
    fn deserialize_both_backends_with_bucket() {
        // Local first → local default.
        let toml_str = r#"
            backends = ["local", "s3"]
            s3_bucket = "my-bucket"
        "#;
        let c: StoragePolicy = toml::from_str(toml_str).unwrap();
        assert!(c.local);
        assert_eq!(c.s3, Some(s3_opts("my-bucket")));
        assert_eq!(c.default_kind(), StorageKind::Local);

        // S3 first → S3 default.
        let toml_str = r#"
            backends = ["s3", "local"]
            s3_bucket = "my-bucket"
        "#;
        let c: StoragePolicy = toml::from_str(toml_str).unwrap();
        assert!(c.local);
        assert_eq!(c.s3, Some(s3_opts("my-bucket")));
        assert_eq!(c.default_kind(), StorageKind::S3);
    }

    #[test]
    fn deserialize_s3_only() {
        let toml_str = r#"
            backends = ["s3"]
            s3_bucket = "my-bucket"
        "#;
        let c: StoragePolicy = toml::from_str(toml_str).unwrap();
        assert!(!c.local);
        assert_eq!(c.s3, Some(s3_opts("my-bucket")));
        assert_eq!(c.default_kind(), StorageKind::S3);
    }

    #[test]
    fn deserialize_rejects_validation_errors() {
        let err = toml::from_str::<StoragePolicy>(r#"backends = []"#).unwrap_err();
        assert!(
            err.to_string().contains("at least one backend"),
            "expected NoBackends message, got: {err}",
        );

        let err = toml::from_str::<StoragePolicy>(r#"backends = ["s3"]"#).unwrap_err();
        assert!(
            err.to_string().contains("s3 bucket cannot be empty"),
            "expected EmptyS3Bucket message, got: {err}",
        );

        let err = toml::from_str::<StoragePolicy>(r#"backends = ["local", "local"]"#).unwrap_err();
        assert!(
            err.to_string().contains("appears multiple times"),
            "expected DuplicateBackend message, got: {err}",
        );
    }
}
