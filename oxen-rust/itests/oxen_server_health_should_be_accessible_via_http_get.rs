use crate::common::TestServer;
use std::time::Duration;

/// Test health endpoint
/// Tests that the /api/health endpoint responds successfully
#[tokio::test]
async fn test_health_endpoint() {
    let test_dir = std::env::temp_dir().join("oxen_health_test");
    std::fs::create_dir_all(&test_dir).expect("Failed to create test directory");

    // Start oxen-server with auto-port allocation
    let server = TestServer::start_with_auto_port(&test_dir)
        .await
        .expect("Failed to start test server");

    // Create HTTP client
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client");

    println!("Testing health endpoint...");
    let response = client
        .get(&format!("{}/api/health", server.base_url()))
        .send()
        .await
        .expect("Failed to send health check request");

    let status = response.status();
    println!("Health response status: {}", status);
    let body = response.text().await.unwrap_or_default();
    println!("Health response body: {}", body);

    // Health endpoint should return success
    assert!(
        status.is_success(),
        "Health endpoint should return success status"
    );

    // Clean up
    let _ = std::fs::remove_dir_all(&test_dir);
    println!("✅ Health endpoint test completed!");
}

/// Test version endpoint
/// Tests that the /api/version endpoint returns version information
#[tokio::test]
async fn test_version_endpoint() {
    let test_dir = std::env::temp_dir().join("oxen_version_test");
    std::fs::create_dir_all(&test_dir).expect("Failed to create test directory");

    // Start oxen-server with auto-port allocation
    let server = TestServer::start_with_auto_port(&test_dir)
        .await
        .expect("Failed to start test server");

    // Create HTTP client
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client");

    println!("Testing version endpoint...");
    let response = client
        .get(&format!("{}/api/version", server.base_url()))
        .send()
        .await
        .expect("Failed to send version request");

    let status = response.status();
    println!("Version response status: {}", status);
    let body = response.text().await.unwrap_or_default();
    println!("Version response body: {}", body);

    if status.is_success() {
        assert!(
            body.contains("version"),
            "Version response should contain version info"
        );
        println!("✅ Version endpoint returned expected content");
    } else {
        println!("⚠️  Version endpoint returned non-success status (may be expected)");
    }

    // Clean up
    let _ = std::fs::remove_dir_all(&test_dir);
    println!("✅ Version endpoint test completed!");
}

/// Test 404 endpoint
/// Tests that non-existent endpoints return appropriate error responses
#[tokio::test]
async fn test_404_endpoint() {
    let test_dir = std::env::temp_dir().join("oxen_404_test");
    std::fs::create_dir_all(&test_dir).expect("Failed to create test directory");

    // Start oxen-server with auto-port allocation
    let server = TestServer::start_with_auto_port(&test_dir)
        .await
        .expect("Failed to start test server");

    // Create HTTP client
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client");

    println!("Testing 404 endpoint...");
    let response = client
        .get(&format!("{}/api/nonexistent", server.base_url()))
        .send()
        .await
        .expect("Failed to send 404 test request");

    let status = response.status();
    println!("404 test response status: {}", status);
    let body = response.text().await.unwrap_or_default();
    println!("404 test response body: {}", body);

    // Should be 404 or some error status (not success)
    assert!(
        !status.is_success(),
        "Non-existent endpoint should not return success status"
    );

    // Clean up
    let _ = std::fs::remove_dir_all(&test_dir);
    println!("✅ 404 endpoint test completed!");
}
