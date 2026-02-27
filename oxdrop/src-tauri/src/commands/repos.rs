use crate::error::OxDropError;
use crate::state::AppState;
use liboxen::api;
use liboxen::model::RepoNew;
use serde::{Deserialize, Serialize};
use tauri::State;

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RepoInfo {
    pub namespace: String,
    pub name: String,
    pub display_name: String,
}

// --- Hub API response types ---
// These are OxenHub-specific. liboxen doesn't implement Hub user endpoints,
// so we define the response shapes here.
// TODO(oxen-server): When adding bare oxen-server support, implement an
// alternative code path using the /api/namespaces + /api/repos/{ns} endpoints
// (see liboxen::view::ListNamespacesResponse, ListRepositoryResponse).

#[derive(Deserialize)]
struct HubRepoEntry {
    namespace: String,
    name: String,
    // Hub returns more fields (description, data_types, created_by, etc.)
    // but we only need these for the repo picker.
}

/// Paginated response from GET /api/user/repos
#[derive(Deserialize)]
struct HubUserReposResponse {
    repositories: Vec<HubRepoEntry>,
    page_number: u64,
    total_pages: u64,
    #[allow(dead_code)]
    page_size: u64,
    #[allow(dead_code)]
    total_entries: u64,
}

#[tauri::command]
pub async fn list_repos(state: State<'_, AppState>) -> Result<Vec<RepoInfo>, OxDropError> {
    let host = &state.host;
    let scheme = &state.scheme;
    let url = format!("{}://{}", scheme, host);

    // The reqwest client reads the bearer token from ~/.config/oxen/auth_config.toml
    // for this host, so the request is authenticated automatically.
    let client = api::client::new_for_url(&url).map_err(OxDropError::from)?;

    let mut all_repos = Vec::new();
    let mut page = 1u64;

    loop {
        // OxenHub-specific: fetch repos the authenticated user has access to.
        // Results are sorted by most recently updated.
        // TODO(oxen-server): bare oxen-server doesn't have /api/user/repos.
        // For oxen-server support, fall back to /api/namespaces → /api/repos/{ns}.
        let user_repos_url = format!("{}/api/user/repos?page={}&page_size=100", url, page);
        let res = client
            .get(&user_repos_url)
            .send()
            .await
            .map_err(|e| OxDropError {
                message: format!("Failed to fetch your repositories: {}", e),
                code: "CONNECTION_ERROR".to_string(),
            })?;

        let body = api::client::parse_json_body(&user_repos_url, res)
            .await
            .map_err(OxDropError::from)?;

        let hub_response: HubUserReposResponse =
            serde_json::from_str(&body).map_err(|e| OxDropError {
                message: format!("Failed to parse repositories response: {}", e),
                code: "PARSE_ERROR".to_string(),
            })?;

        // Preserve the ordering from the API (most recently updated first).
        for repo in hub_response.repositories {
            all_repos.push(RepoInfo {
                display_name: format!("{}/{}", repo.namespace, repo.name),
                namespace: repo.namespace,
                name: repo.name,
            });
        }

        if hub_response.page_number >= hub_response.total_pages {
            break;
        }
        page += 1;
    }

    Ok(all_repos)
}

#[tauri::command]
pub async fn create_repo(
    state: State<'_, AppState>,
    namespace: String,
    name: String,
) -> Result<RepoInfo, OxDropError> {
    let host = &state.host;
    let scheme = &state.scheme;

    let mut repo_new = RepoNew::from_namespace_name_host(&namespace, &name, host, None);
    repo_new.scheme = Some(scheme.clone());

    // create_empty works against both OxenHub and bare oxen-server.
    let _remote_repo = api::client::repositories::create_empty(repo_new)
        .await
        .map_err(OxDropError::from)?;

    Ok(RepoInfo {
        display_name: format!("{}/{}", namespace, name),
        namespace,
        name,
    })
}
