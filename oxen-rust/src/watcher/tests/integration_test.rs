use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use tokio::process::Command;
use tokio::time;
use oxen_watcher::ipc::send_request;
use oxen_watcher::protocol::{WatcherRequest, WatcherResponse};

/// Helper to get the watcher binary path
fn get_watcher_path() -> PathBuf {
    // The test binary is typically in target/{profile}/deps/
    // while the actual binary is in target/{profile}/
    let mut path = std::env::current_exe().unwrap();

    // Go up from deps directory if we're in it
    path.pop(); // Remove test binary name
    if path.ends_with("deps") {
        path.pop(); // Remove "deps"
    }

    // Now we should be in target/{profile}/
    let watcher_path = path.join("oxen-watcher");

    if !watcher_path.exists() {
        panic!(
            "oxen-watcher binary not found at {:?}. Run 'cargo build --package oxen-watcher --bin oxen-watcher' first",
            watcher_path
        );
    }

    watcher_path
}

#[tokio::test]
#[ignore] // Run with: cargo test --package oxen-watcher -- --ignored
async fn test_watcher_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let repo_path = temp_dir.path();

    // Initialize an oxen repository
    liboxen::repositories::init::init(repo_path).unwrap();

    let watcher_path = get_watcher_path();

    // Start the watcher
    let mut start_cmd = Command::new(&watcher_path)
        .arg("start")
        .arg("--repo")
        .arg(repo_path)
        .spawn()
        .expect("Failed to start watcher");

    // Give it time to start
    time::sleep(Duration::from_secs(2)).await;

    // Check status
    let status_output = Command::new(&watcher_path)
        .arg("status")
        .arg("--repo")
        .arg(repo_path)
        .output()
        .await
        .expect("Failed to check status");

    let status_str = String::from_utf8_lossy(&status_output.stdout);
    assert!(status_str.contains("running"), "Watcher should be running");

    // Stop the watcher
    let stop_output = Command::new(&watcher_path)
        .arg("stop")
        .arg("--repo")
        .arg(repo_path)
        .output()
        .await
        .expect("Failed to stop watcher");

    assert!(stop_output.status.success(), "Stop command should succeed");

    // Give it time to stop
    time::sleep(Duration::from_secs(1)).await;

    // Check status again
    let status_output2 = Command::new(&watcher_path)
        .arg("status")
        .arg("--repo")
        .arg(repo_path)
        .output()
        .await
        .expect("Failed to check status");

    let status_str2 = String::from_utf8_lossy(&status_output2.stdout);
    assert!(
        status_str2.contains("not running"),
        "Watcher should not be running"
    );

    // Clean up - ensure process is terminated
    let _ = start_cmd.kill().await;
}

#[tokio::test]
#[ignore]
async fn test_watcher_file_detection() {
    let temp_dir = TempDir::new().unwrap();
    let repo_path = temp_dir.path();

    // Initialize an oxen repository
    liboxen::repositories::init::init(repo_path).unwrap();

    let watcher_path = get_watcher_path();

    // Start the watcher
    let mut watcher_process = Command::new(&watcher_path)
        .arg("start")
        .arg("--repo")
        .arg(repo_path)
        .spawn()
        .expect("Failed to start watcher");

    // Give it time to start and do initial scan
    time::sleep(Duration::from_secs(3)).await;

    // Create a new file
    let test_file = repo_path.join("test.txt");
    std::fs::write(&test_file, "test content").unwrap();

    // Give watcher time to detect the change
    time::sleep(Duration::from_secs(1)).await;

    // TODO: Once CLI integration is complete (try_watcher_status() in status.rs),
    // we should test that `oxen status` actually detects the new file via the watcher.
    // For now we just verify the watcher is running.

    let status_output = Command::new(&watcher_path)
        .arg("status")
        .arg("--repo")
        .arg(repo_path)
        .output()
        .await
        .expect("Failed to check status");

    assert!(status_output.status.success());

    // Stop the watcher
    Command::new(&watcher_path)
        .arg("stop")
        .arg("--repo")
        .arg(repo_path)
        .output()
        .await
        .expect("Failed to stop watcher");

    // Clean up
    let _ = watcher_process.kill().await;
}

#[tokio::test]
#[ignore]
async fn test_multiple_watcher_prevention() {
    let temp_dir = TempDir::new().unwrap();
    let repo_path = temp_dir.path();

    // Initialize an oxen repository
    liboxen::repositories::init::init(repo_path).unwrap();

    let watcher_path = get_watcher_path();

    // Start the first watcher
    let mut first_watcher = Command::new(&watcher_path)
        .arg("start")
        .arg("--repo")
        .arg(repo_path)
        .spawn()
        .expect("Failed to start first watcher");

    // Give it time to start
    time::sleep(Duration::from_secs(2)).await;

    // Try to start a second watcher (should not create a new one)
    let second_output = Command::new(&watcher_path)
        .arg("start")
        .arg("--repo")
        .arg(repo_path)
        .output()
        .await
        .expect("Failed to run second start command");

    // The second start should succeed but not create a new watcher
    assert!(second_output.status.success());

    // Stop the watcher
    Command::new(&watcher_path)
        .arg("stop")
        .arg("--repo")
        .arg(repo_path)
        .output()
        .await
        .expect("Failed to stop watcher");

    // Clean up
    let _ = first_watcher.kill().await;
}

#[tokio::test]
#[ignore]
async fn test_watcher_reports_relative_paths() {
    let temp_dir = TempDir::new().unwrap();
    let repo_path = temp_dir.path();

    // Initialize an oxen repository
    liboxen::repositories::init::init(repo_path).unwrap();

    let watcher_path = get_watcher_path();

    // Start the watcher
    let mut watcher_process = Command::new(&watcher_path)
        .arg("start")
        .arg("--repo")
        .arg(repo_path)
        .spawn()
        .expect("Failed to start watcher");

    // Give it time to start and do initial scan
    time::sleep(Duration::from_secs(3)).await;

    // Create test files in different directories
    std::fs::write(repo_path.join("root_file.txt"), "root content").unwrap();
    std::fs::create_dir_all(repo_path.join("subdir")).unwrap();
    std::fs::write(repo_path.join("subdir/nested_file.txt"), "nested content").unwrap();

    // Give watcher time to detect the changes
    time::sleep(Duration::from_millis(500)).await;

    // Query the watcher via IPC
    let socket_path = repo_path.join(".oxen/watcher.sock");
    let request = WatcherRequest::GetStatus { paths: None };
    let response = send_request(&socket_path, request).await.expect("Failed to send request");

    // Verify the response contains relative paths
    if let WatcherResponse::Status(status) = response {
        // Check that all created file paths are relative
        for file_status in &status.created {
            assert!(!file_status.path.is_absolute(), "Path should be relative, got: {:?}", file_status.path);
            assert!(!file_status.path.starts_with("/"), "Path should not start with /, got: {:?}", file_status.path);
        }
        
        // Verify specific files are present with correct relative paths
        let paths: Vec<_> = status.created.iter().map(|f| f.path.to_string_lossy().to_string()).collect();
        assert!(paths.contains(&"root_file.txt".to_string()), "Should contain root_file.txt");
        assert!(paths.contains(&"subdir/nested_file.txt".to_string()), "Should contain subdir/nested_file.txt");
    } else {
        panic!("Expected Status response, got: {:?}", response);
    }

    // Stop the watcher
    Command::new(&watcher_path)
        .arg("stop")
        .arg("--repo")
        .arg(repo_path)
        .output()
        .await
        .expect("Failed to stop watcher");

    // Clean up
    let _ = watcher_process.kill().await;
}
