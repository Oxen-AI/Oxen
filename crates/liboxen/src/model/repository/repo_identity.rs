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

    /// The identity for a repository a control plane placed and addresses by UUID, whose names
    /// this server has not been told.
    pub fn hintless(repo_uuid: Uuid) -> Self {
        RepoIdentity {
            repo_uuid,
            namespace: None,
            name: None,
        }
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

    /// Storage, index keys, and directory names all compare these as strings, so the stored form
    /// has to be the canonical lowercase hyphenated one no matter which form arrived.
    #[test]
    fn uuids_serialize_canonically_whatever_form_arrived() {
        let identity = RepoIdentity::hintless(
            Uuid::parse_str("5abd211ee25c494bba0f44ad542443d7").expect("a valid UUID"),
        );
        let toml = toml::to_string(&identity).expect("serialize");
        assert!(
            toml.contains("5abd211e-e25c-494b-ba0f-44ad542443d7"),
            "expected canonical hyphenated form in:\n{toml}"
        );
    }

    #[test]
    fn absent_name_hints_are_not_serialized() {
        let repo_uuid = Uuid::new_v4();
        let toml = toml::to_string(&RepoIdentity::hintless(repo_uuid)).expect("serialize");

        assert_eq!(toml, format!("repo_uuid = \"{repo_uuid}\"\n"));
    }

    /// An identity that cannot address storage is worse than none, so it fails to load rather than
    /// arriving as a repo that claims an identity and has no UUID.
    #[test]
    fn an_identity_without_a_repo_uuid_is_rejected() {
        assert!(toml::from_str::<RepoIdentity>("namespace = \"ox\"\n").is_err());
    }
}
