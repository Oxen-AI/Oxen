//! Server-side repository identity policy.
//!
//! [`IdentityPolicy`] is the admin's "does anything above this server own namespaces?" answer,
//! which decides whether the server assigns repository UUIDs or takes them from above. It is read
//! once at startup and carried on [`crate::app_data::OxenAppData`], so that no request can select
//! the source it is handled under.
//!
//! Transitional; see `docs/deprecations.md`.

use serde::Deserialize;

/// What assigns the UUIDs a deployment identifies namespaces and repositories by, and so whether a
/// request's namespace and name positions carry names or UUIDs.
///
/// Resolved once at server startup and never per request, so that a request can neither select
/// its own source nor promote a server into assigning.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentitySource {
    /// oxen-server owns namespaces. The standalone default: with nothing above it to ask, the
    /// server assigns repository UUIDs itself, and both positions carry names.
    #[default]
    OxenServer,
    /// An auth provider owns namespaces and identifies them by UUIDs it assigns, passing them down
    /// in the namespace position. Right now, this also means the auth provider assigns repository
    /// UUIDs and passes them down in the repo name position. This will evolve quickly in the near
    /// future.
    AuthProvider,
}

impl IdentitySource {
    /// Whether a request's namespace and name positions carry human-readable names.
    pub fn supplies_names(&self) -> bool {
        matches!(self, IdentitySource::OxenServer)
    }
}

/// Server-side repository identity policy.
///
/// The default is [`IdentitySource::OxenServer`]: a server with nothing configured above it
/// assigns UUIDs itself.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdentityPolicy {
    #[serde(default)]
    repo_uuids_assigned_by: IdentitySource,
}

impl IdentityPolicy {
    /// What assigns repository UUIDs on this server.
    pub fn repo_uuids_assigned_by(&self) -> IdentitySource {
        self.repo_uuids_assigned_by
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A server with no `[identity]` section is standalone, so it assigns its own UUIDs.
    #[test]
    fn defaults_to_the_server_assigning_uuids() {
        assert_eq!(
            IdentityPolicy::default().repo_uuids_assigned_by(),
            IdentitySource::OxenServer
        );
    }

    #[test]
    fn parses_both_sources() {
        let auth_provider: IdentityPolicy =
            toml::from_str(r#"repo_uuids_assigned_by = "auth-provider""#)
                .expect("a known source parses");
        let oxen_server: IdentityPolicy =
            toml::from_str(r#"repo_uuids_assigned_by = "oxen-server""#)
                .expect("a known source parses");

        assert_eq!(
            auth_provider.repo_uuids_assigned_by(),
            IdentitySource::AuthProvider
        );
        assert_eq!(
            oxen_server.repo_uuids_assigned_by(),
            IdentitySource::OxenServer
        );
    }

    /// A typo must not silently leave the server assigning UUIDs an auth provider owns.
    #[test]
    fn rejects_an_unknown_source() {
        assert!(toml::from_str::<IdentityPolicy>(r#"repo_uuids_assigned_by = "hub""#).is_err());
    }

    #[test]
    fn rejects_an_unknown_key() {
        assert!(
            toml::from_str::<IdentityPolicy>(r#"repo_uuids_assigned_byy = "oxen-server""#).is_err()
        );
    }
}
