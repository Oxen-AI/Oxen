//! Integration tests for the OxDrop app flow.
//!
//! These tests exercise the same liboxen API calls that our Tauri commands use,
//! without depending on the Tauri runtime. They require:
//!   - A running OxenHub at localhost:3001
//!   - An auth token configured in ~/.config/oxen/auth_config.toml for localhost:3001
//!   - A repo "josh/test1" (or adjust TEST_REPO below)
//!
//! Run with:
//!   cargo test -- --ignored

use liboxen::api;
use liboxen::config::{AuthConfig, UserConfig};
use liboxen::model::{NewCommitBody, Remote};

const TEST_HOST: &str = "localhost:3001";
const TEST_SCHEME: &str = "http";
const TEST_REPO: &str = "josh/test1";
const TEST_BRANCH: &str = "main";

/// Helper: build a base URL from scheme + host
fn base_url() -> String {
    format!("{}://{}", TEST_SCHEME, TEST_HOST)
}

/// Helper: build a RemoteRepository for the test repo
async fn get_test_remote_repo() -> liboxen::model::RemoteRepository {
    let parts: Vec<&str> = TEST_REPO.splitn(2, '/').collect();
    let url = format!("{}/{}/{}", base_url(), parts[0], parts[1]);
    let remote = Remote {
        name: "origin".to_string(),
        url,
    };
    api::client::repositories::get_by_remote(&remote)
        .await
        .expect("Failed to call get_by_remote")
        .expect("Test repo not found — make sure it exists on the local OxenHub")
}

// ─── Step 0: Login ───────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_step0_login() {
    let url = format!("{}/api/login", base_url());
    let client = reqwest::Client::new();
    let res = client
        .post(&url)
        .json(&serde_json::json!({
            "identifier": "josh",
            "password": "testing123",
        }))
        .send()
        .await
        .expect("Failed to send login request");

    assert!(
        res.status().is_success(),
        "Login returned status {}",
        res.status()
    );

    let body: serde_json::Value = res.json().await.expect("Failed to parse login response");
    assert_eq!(body["status"], "success", "Login status was not 'success'");
    assert!(
        body["user"]["api_key"].as_str().map_or(false, |k| !k.is_empty()),
        "Login response missing api_key"
    );
    println!("Login succeeded, api_key present");
}

// ─── Step 1: Auth ────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn test_step1_auth_token_exists() {
    let auth_config = AuthConfig::get().expect("Could not read auth config");
    let token = auth_config.auth_token_for_host(TEST_HOST);
    assert!(
        token.is_some(),
        "No auth token for {} in ~/.config/oxen/auth_config.toml",
        TEST_HOST
    );
    assert!(
        !token.unwrap().is_empty(),
        "Auth token for {} is empty",
        TEST_HOST
    );
}

// ─── Step 2: List repos (Hub API) ────────────────────────────────────────────

/// Mirrors the logic in commands/repos.rs — calls GET /api/user/repos
#[tokio::test]
#[ignore]
async fn test_step2_list_hub_repos() {
    let url = base_url();
    let client = api::client::new_for_url(&url).expect("Failed to create client");

    let user_repos_url = format!("{}/api/user/repos?page=1&page_size=10", url);
    let res = client
        .get(&user_repos_url)
        .send()
        .await
        .expect("Failed to send request");

    let body = api::client::parse_json_body(&user_repos_url, res)
        .await
        .expect("Failed to parse response body");

    // Verify we can parse the Hub response shape
    let parsed: serde_json::Value =
        serde_json::from_str(&body).expect("Response is not valid JSON");

    assert!(
        parsed.get("repositories").is_some(),
        "Response missing 'repositories' key: {}",
        body
    );
    assert!(
        parsed.get("total_entries").is_some(),
        "Response missing 'total_entries' key"
    );

    let repos = parsed["repositories"].as_array().unwrap();
    assert!(
        !repos.is_empty(),
        "No repositories returned — expected at least one"
    );

    // Each repo should have namespace and name
    let first = &repos[0];
    assert!(first.get("namespace").is_some(), "Repo missing 'namespace'");
    assert!(first.get("name").is_some(), "Repo missing 'name'");
}

// ─── Step 3: File scanning (local only, no server) ───────────────────────────

#[tokio::test]
#[ignore]
async fn test_step3_scan_files() {
    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let dir_path = dir.path();

    // Create some test files
    std::fs::write(dir_path.join("file1.txt"), "hello").unwrap();
    std::fs::write(dir_path.join("file2.txt"), "world").unwrap();
    std::fs::create_dir(dir_path.join("sub")).unwrap();
    std::fs::write(dir_path.join("sub").join("file3.txt"), "nested").unwrap();

    // Replicate the file walking logic from commands/files.rs
    let mut total_files: u64 = 0;
    let mut total_size: u64 = 0;
    for entry in jwalk::WalkDir::new(dir_path).into_iter().flatten() {
        let entry_path = entry.path();
        if entry_path.is_file() {
            total_files += 1;
            if let Ok(meta) = std::fs::metadata(&entry_path) {
                total_size += meta.len();
            }
        }
    }

    assert_eq!(total_files, 3, "Expected 3 files");
    assert_eq!(total_size, 5 + 5 + 6, "Expected 16 bytes total");
}

// ─── Step 4: Full upload flow ────────────────────────────────────────────────

/// Exercises the exact same API sequence as run_upload() in commands/upload.rs:
///   1. Resolve RemoteRepository from repo string
///   2. Create workspace
///   3. Upload files in a batch
///   4. Commit the workspace
#[tokio::test]
#[ignore]
async fn test_step4_upload_flow() {
    let remote_repo = get_test_remote_repo().await;

    // Create a temp file to upload (unique content per run to avoid "no changes")
    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let test_file = dir.path().join("test_upload.txt");
    std::fs::write(
        &test_file,
        format!("oxdrop integration test {}", uuid::Uuid::new_v4()),
    )
    .unwrap();

    let workspace_id = uuid::Uuid::new_v4().to_string();

    // Step 4a: Create workspace (matches upload.rs line 163)
    let ws = api::client::workspaces::create(&remote_repo, TEST_BRANCH, &workspace_id)
        .await
        .expect("Failed to create workspace");
    println!(
        "Created workspace: requested_id={}, returned_id={}",
        workspace_id, ws.id
    );

    // BUG FINDING: OxenHub may return a different ID than what we requested.
    // We must use the returned ID for all subsequent operations.
    let actual_workspace_id = &ws.id;

    // Step 4b: Add files (matches upload.rs upload_batch())
    let paths = vec![test_file.clone()];
    api::client::workspaces::files::add(&remote_repo, actual_workspace_id, "", paths, &None)
        .await
        .expect("Failed to add files to workspace");
    println!("Added files to workspace");

    // Step 4c: Commit (matches upload.rs line 288)
    let user_config = UserConfig::get_or_create().expect("Failed to get user config");
    let commit_body = NewCommitBody {
        message: "OxDrop integration test upload".to_string(),
        author: user_config.name,
        email: user_config.email,
    };

    let commit = api::client::workspaces::commits::commit(
        &remote_repo,
        TEST_BRANCH,
        actual_workspace_id,
        &commit_body,
    )
    .await;

    match &commit {
        Ok(c) => println!("Commit succeeded: {}", c.id),
        Err(e) => println!("Commit failed: {}", e),
    }

    // Clean up workspace on failure (it stays open if commit didn't succeed)
    if commit.is_err() {
        let _ = api::client::workspaces::delete(&remote_repo, actual_workspace_id).await;
    }

    commit.expect("Workspace commit failed");
}

/// Same as test_step4 but uses create_with_name and passes the name to commit,
/// matching how the CLI does it (oxen workspace create -n <name>).
#[tokio::test]
#[ignore]
async fn test_step4b_upload_flow_with_workspace_name() {
    let remote_repo = get_test_remote_repo().await;

    let dir = tempfile::tempdir().expect("Failed to create temp dir");
    let test_file = dir.path().join("test_upload_named.txt");
    std::fs::write(
        &test_file,
        format!("oxdrop named workspace test {}", uuid::Uuid::new_v4()),
    )
    .unwrap();

    let workspace_id = uuid::Uuid::new_v4().to_string();
    let workspace_name = format!("oxdrop-test-{}", &workspace_id[..8]);

    // Create workspace WITH a name
    let ws = api::client::workspaces::create_with_name(
        &remote_repo,
        TEST_BRANCH,
        &workspace_id,
        &workspace_name,
    )
    .await
    .expect("Failed to create named workspace");
    println!(
        "Created named workspace: id={}, name={:?}",
        ws.id, ws.name
    );

    // Add files using the workspace NAME (like the CLI does)
    let paths = vec![test_file.clone()];
    api::client::workspaces::files::add(&remote_repo, &workspace_name, "", paths, &None)
        .await
        .expect("Failed to add files to named workspace");
    println!("Added files to named workspace");

    // Commit using the workspace NAME (like the CLI does)
    let user_config = UserConfig::get_or_create().expect("Failed to get user config");
    let commit_body = NewCommitBody {
        message: "OxDrop named workspace test".to_string(),
        author: user_config.name,
        email: user_config.email,
    };

    let commit = api::client::workspaces::commits::commit(
        &remote_repo,
        TEST_BRANCH,
        &workspace_name,
        &commit_body,
    )
    .await;

    match &commit {
        Ok(c) => println!("Named workspace commit succeeded: {}", c.id),
        Err(e) => println!("Named workspace commit failed: {}", e),
    }

    if commit.is_err() {
        let _ = api::client::workspaces::delete(&remote_repo, &workspace_name).await;
    }

    commit.expect("Named workspace commit failed");
}
