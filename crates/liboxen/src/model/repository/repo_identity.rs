//! A repository's identity: the UUID it is addressed by, and the names recorded beside it.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A repository's identity, as recorded in its `config.toml`.
///
/// `repo_uuid` is immutable and addresses the repo's storage. `namespace` and `name` are hints:
/// they track the human-readable names, may be stale, and nothing may be addressed by them.
///
/// A repository either has a whole identity or none at all, which is why the UUID is not optional:
/// an `[identity]` table without one fails to parse rather than loading as an identity that cannot
/// address anything.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepoIdentity {
    /// Immutable identity of this repository.
    pub repo_uuid: Uuid,
    /// Human-readable namespace name. Absent when the server has not been told one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Human-readable repository name. Absent when the server has not been told one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

impl RepoIdentity {
    /// The identity for a repository this server is creating in `namespace` under `name`.
    ///
    /// Takes no caller-supplied UUID: a server that owns identity assigns its own, and honoring a
    /// supplied one would let a request name its own storage.
    pub fn minted(namespace: &str, name: &str) -> Self {
        RepoIdentity {
            repo_uuid: Uuid::new_v4(),
            namespace: Some(namespace.to_string()),
            name: Some(name.to_string()),
        }
    }

    /// The identity for a repository whose UUID an auth provider assigned, taken from
    /// `supplied_repo_uuid` or read off the `name` position, which the provider sets to the UUID.
    ///
    /// `None` when neither carries one, leaving the repository with no identity rather than one
    /// that disagrees with the directory it was created in. Name hints stay unset, because neither
    /// position holds a name.
    pub fn from_supplied(supplied_repo_uuid: Option<Uuid>, name: &str) -> Option<Self> {
        supplied_repo_uuid
            .or_else(|| Uuid::parse_str(name).ok())
            .map(|repo_uuid| RepoIdentity {
                repo_uuid,
                namespace: None,
                name: None,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn server_identity(namespace: &str, name: &str) -> RepoIdentity {
        RepoIdentity::minted(namespace, name)
    }

    #[test]
    fn oxen_server_assigns_a_repo_uuid_and_records_both_names() {
        let identity = server_identity("ox", "cats");
        assert_eq!(identity.namespace.as_deref(), Some("ox"));
        assert_eq!(identity.name.as_deref(), Some("cats"));
    }

    /// The auth provider passes the repo UUID in the name position, so that is the identity and
    /// neither position is a name.
    #[test]
    fn auth_provider_reads_the_repo_uuid_out_of_the_name_position() {
        let repo_uuid = Uuid::new_v4();
        let identity = RepoIdentity::from_supplied(None, &repo_uuid.to_string())
            .expect("the name position carries the identity");

        assert_eq!(identity.repo_uuid, repo_uuid);
        assert_eq!(identity.namespace, None);
        assert_eq!(identity.name, None);
    }

    #[test]
    fn auth_provider_prefers_a_supplied_uuid_over_the_name_position() {
        let supplied = Uuid::new_v4();
        let in_name_position = Uuid::new_v4();
        let identity = RepoIdentity::from_supplied(Some(supplied), &in_name_position.to_string())
            .expect("a supplied UUID is an identity");

        assert_eq!(identity.repo_uuid, supplied);
    }

    /// Assigning here would write a UUID that disagrees with the directory the repo lives in, and
    /// every later step derives from that config. Recording nothing leaves identity
    /// all-or-nothing, so the backfill can treat its presence as a binary.
    #[test]
    fn auth_provider_records_nothing_rather_than_assigning() {
        assert_eq!(RepoIdentity::from_supplied(None, "cats"), None);
    }

    /// Storage, index keys, and directory names all compare these as strings, so the stored form
    /// has to be the canonical lowercase hyphenated one no matter which form arrived.
    #[test]
    fn uuids_serialize_canonically_whatever_form_arrived() {
        let identity = RepoIdentity::from_supplied(None, "5abd211ee25c494bba0f44ad542443d7")
            .expect("an unhyphenated UUID is still a UUID");
        let toml = toml::to_string(&identity).expect("serialize");
        assert!(
            toml.contains("5abd211e-e25c-494b-ba0f-44ad542443d7"),
            "expected canonical hyphenated form in:\n{toml}"
        );
    }

    #[test]
    fn absent_name_hints_are_not_serialized() {
        let repo_uuid = Uuid::new_v4();
        let toml = toml::to_string(&RepoIdentity {
            repo_uuid,
            namespace: None,
            name: None,
        })
        .expect("serialize");

        assert_eq!(toml, format!("repo_uuid = \"{repo_uuid}\"\n"));
    }

    /// An identity that cannot address storage is worse than none, so it fails to load rather than
    /// arriving as a repo that claims an identity and has no UUID.
    #[test]
    fn an_identity_without_a_repo_uuid_is_rejected() {
        assert!(toml::from_str::<RepoIdentity>("namespace = \"ox\"\n").is_err());
    }
}
