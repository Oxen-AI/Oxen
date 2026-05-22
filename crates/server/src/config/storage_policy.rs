//! Server-side storage policy.
//!
//! [`StoragePolicy`] is the admin's "what backends will this server host?" answer.
//! Its methods validate caller-supplied storage requests against that answer: empty
//! requests fall back to the server's default (first in `backends`), disallowed requests
//! yield a 400 BadRequest whose body lists the allowed backends.
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

/// Server-side storage policy.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "StoragePolicyRaw")]
pub struct StoragePolicy {
    /// Non-empty; index 0 is the server's default. No duplicate kinds.
    backends: Vec<StorageKind>,
    /// Must be non-empty when `StorageKind::S3 ∈ backends`. When S3 is not enabled,
    /// any value here is ignored — `""` is the canonical "unset" form.
    s3_bucket: String,
}

#[derive(Deserialize)]
struct StoragePolicyRaw {
    backends: Vec<StorageKind>,
    #[serde(default)]
    s3_bucket: String,
}

#[derive(Debug, thiserror::Error)]
pub enum StoragePolicyError {
    /// `StoragePolicy` rejected an admin config with an empty `[storage] backends` list.
    #[error("Storage policy: at least one backend must be configured under [storage] backends")]
    StoragePolicyNoBackends,

    /// `StoragePolicy` rejected an admin config that lists the same backend twice under
    /// `[storage] backends`.
    #[error("Storage policy: backend '{0}' appears multiple times in [storage] backends")]
    StoragePolicyDuplicateBackend(liboxen::storage::StorageKind),

    /// `StoragePolicy` rejected an admin config that includes the S3 backend without a
    /// bucket name (or with an empty one).
    #[error("Storage policy: s3 bucket cannot be empty when the s3 backend is allowed")]
    StoragePolicyEmptyS3Bucket,
}

impl TryFrom<StoragePolicyRaw> for StoragePolicy {
    type Error = StoragePolicyError;

    fn try_from(raw: StoragePolicyRaw) -> Result<Self, StoragePolicyError> {
        if raw.backends.is_empty() {
            return Err(StoragePolicyError::StoragePolicyNoBackends);
        }
        let mut seen = HashSet::new();
        for kind in &raw.backends {
            if !seen.insert(*kind) {
                return Err(StoragePolicyError::StoragePolicyDuplicateBackend(*kind));
            }
        }
        // Orphan bucket (set when S3 isn't in backends) is silently allowed so admins
        // toggling backends on/off during config iteration don't trip a validation error.
        if raw.backends.contains(&StorageKind::S3) && raw.s3_bucket.is_empty() {
            return Err(StoragePolicyError::StoragePolicyEmptyS3Bucket);
        }
        Ok(Self {
            backends: raw.backends,
            s3_bucket: raw.s3_bucket,
        })
    }
}

impl Default for StoragePolicy {
    fn default() -> Self {
        Self {
            backends: vec![StorageKind::Local],
            s3_bucket: String::new(),
        }
    }
}

impl StoragePolicy {
    fn default_kind(&self) -> StorageKind {
        self.backends[0]
    }

    fn is_allowed(&self, kind: StorageKind) -> bool {
        self.backends.contains(&kind)
    }

    /// Resolve the storage backend kind to use for a new repo.
    ///
    /// - `requested = None`: returns the server's default (first in `backends`).
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
            let allowed = self
                .backends
                .iter()
                .map(StorageKind::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            Err(OxenHttpError::BadRequest(
                format!("Storage backend '{kind}' is not enabled on this server. Allowed backends: [{allowed}]").into(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(c.backends, vec![StorageKind::Local]);
        assert_eq!(c.s3_bucket, "");
        assert_eq!(c.default_kind(), StorageKind::Local);
    }

    #[test]
    fn try_from_minimum_valid_local() {
        let c = cfg(vec![StorageKind::Local], "");
        assert_eq!(c.default_kind(), StorageKind::Local);
    }

    #[test]
    fn try_from_minimum_valid_s3_only() {
        let c = cfg(vec![StorageKind::S3], "oxen-test");
        assert_eq!(c.default_kind(), StorageKind::S3);
        assert_eq!(c.s3_bucket, "oxen-test");
    }

    #[test]
    fn try_from_both_local_default() {
        let c = cfg(vec![StorageKind::Local, StorageKind::S3], "oxen-test");
        assert_eq!(c.default_kind(), StorageKind::Local);
    }

    #[test]
    fn try_from_both_s3_default() {
        let c = cfg(vec![StorageKind::S3, StorageKind::Local], "oxen-test");
        assert_eq!(c.default_kind(), StorageKind::S3);
    }

    #[test]
    fn try_from_rejects_empty_backends() {
        let err = StoragePolicy::try_from(raw(vec![], "")).unwrap_err();
        assert!(matches!(err, StoragePolicyError::StoragePolicyNoBackends));
    }

    #[test]
    fn try_from_rejects_duplicate_backend() {
        let err = StoragePolicy::try_from(raw(vec![StorageKind::Local, StorageKind::Local], ""))
            .unwrap_err();
        assert!(matches!(
            err,
            StoragePolicyError::StoragePolicyDuplicateBackend(StorageKind::Local)
        ));
    }

    #[test]
    fn try_from_rejects_s3_in_backends_with_empty_bucket() {
        let err = StoragePolicy::try_from(raw(vec![StorageKind::S3], "")).unwrap_err();
        assert!(matches!(
            err,
            StoragePolicyError::StoragePolicyEmptyS3Bucket
        ));
    }

    #[test]
    fn try_from_allows_orphan_s3_bucket() {
        // S3 not in backends, but bucket is set. The bucket value is dead config and is
        // silently ignored; no validation error.
        let c = cfg(vec![StorageKind::Local], "orphan-bucket");
        assert_eq!(c.s3_bucket, "orphan-bucket");
        assert!(!c.is_allowed(StorageKind::S3));
    }

    /// Covers `is_allowed` across the three (Local-only, S3-only, both) shapes.
    #[test]
    fn allow_matrix() {
        let cases = [
            (cfg(vec![StorageKind::Local], ""), vec![StorageKind::Local]),
            (cfg(vec![StorageKind::S3], "b"), vec![StorageKind::S3]),
            (
                cfg(vec![StorageKind::Local, StorageKind::S3], "b"),
                vec![StorageKind::Local, StorageKind::S3],
            ),
        ];
        for (c, expected) in cases {
            assert_eq!(c.backends, expected, "backends mismatch for {c:?}");
            assert_eq!(
                c.is_allowed(StorageKind::Local),
                expected.contains(&StorageKind::Local),
            );
            assert_eq!(
                c.is_allowed(StorageKind::S3),
                expected.contains(&StorageKind::S3),
            );
        }
    }

    #[test]
    fn resolve_unspecified_returns_default_kind() {
        assert_eq!(
            cfg(vec![StorageKind::Local], "").resolve(None).unwrap(),
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
        let local = cfg(vec![StorageKind::Local], "");
        assert_eq!(
            local.resolve(Some(StorageKind::Local)).unwrap(),
            StorageKind::Local
        );

        let s3 = cfg(vec![StorageKind::S3], "b");
        assert_eq!(s3.resolve(Some(StorageKind::S3)).unwrap(), StorageKind::S3);

        let both = cfg(vec![StorageKind::Local, StorageKind::S3], "b");
        assert_eq!(
            both.resolve(Some(StorageKind::S3)).unwrap(),
            StorageKind::S3
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
        let toml_str = r#"
            backends = ["local"]
        "#;
        let c: StoragePolicy = toml::from_str(toml_str).unwrap();
        assert_eq!(c, StoragePolicy::default());
    }

    #[test]
    fn deserialize_both_backends_with_bucket() {
        let toml_str = r#"
            backends = ["local", "s3"]
            s3_bucket = "my-bucket"
        "#;
        let c: StoragePolicy = toml::from_str(toml_str).unwrap();
        assert_eq!(c.default_kind(), StorageKind::Local);
        assert_eq!(c.s3_bucket, "my-bucket");
    }

    #[test]
    fn deserialize_s3_only() {
        let toml_str = r#"
            backends = ["s3"]
            s3_bucket = "my-bucket"
        "#;
        let c: StoragePolicy = toml::from_str(toml_str).unwrap();
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
