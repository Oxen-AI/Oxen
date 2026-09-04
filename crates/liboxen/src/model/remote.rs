use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;
#[derive(Serialize, Deserialize, Debug, Clone, ToSchema)]
pub struct Remote {
    pub name: String,
    pub url: String,
    /// The UUID the remote addresses this repository's storage by. Absent on a remote written
    /// before clients recorded it, and on one pointing at a repository that reports no identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repo_uuid: Option<Uuid>,
}

impl Remote {
    /// A remote naming where a repository lives, with no UUID recorded for it.
    pub fn new(name: &str, url: &str) -> Remote {
        Remote {
            name: name.to_string(),
            url: url.to_string(),
            repo_uuid: None,
        }
    }

    /// The same remote, taking `repo_uuid` only where it has none recorded.
    ///
    pub(crate) fn with_repo_uuid_if_absent(self, repo_uuid: Option<Uuid>) -> Remote {
        Remote {
            repo_uuid: self.repo_uuid.or(repo_uuid),
            ..self
        }
    }
}

impl std::fmt::Display for Remote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] '{}'", self.name, self.url)
    }
}

impl std::error::Error for Remote {}

#[cfg(test)]
mod tests {
    use super::*;

    /// The client config is TOML, and a remote written before clients recorded a UUID has no key
    /// for it, so it has to load as absent rather than failing the whole config.
    #[test]
    fn a_remote_without_a_uuid_loads_with_none() {
        let remote: Remote =
            toml::from_str("name = \"origin\"\nurl = \"http://localhost:3000/ox/cats\"\n")
                .expect("a remote predating the field parses");

        assert_eq!(remote.repo_uuid, None);
    }

    /// Guards the `skip_serializing_if`, so a config written for a repository with no identity
    /// carries no key an older client would have to tolerate.
    #[test]
    fn an_absent_uuid_is_not_written_at_all() {
        let toml = toml::to_string(&Remote::new("origin", "http://localhost:3000/ox/cats"))
            .expect("serialize");

        assert!(!toml.contains("repo_uuid"), "unexpected key in:\n{toml}");
    }

    #[test]
    fn a_recorded_uuid_round_trips() {
        let repo_uuid = Uuid::new_v4();
        let remote = Remote::new("origin", "http://localhost:3000/ox/cats")
            .with_repo_uuid_if_absent(Some(repo_uuid));

        let loaded: Remote =
            toml::from_str(&toml::to_string(&remote).expect("serialize")).expect("deserialize");

        assert_eq!(loaded.repo_uuid, Some(repo_uuid));
    }
}
