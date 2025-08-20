use crate::common::{TestRepositoryBuilder, TestServer};

#[tokio::test]
async fn oxen_repo_held_csv_should_be_accessible_via_http_get() {
    // This test focuses specifically on CSV file accessibility via HTTP GET
    // Create a test repository with CSV data using unique directory to avoid lock conflicts
    let unique_id = std::thread::current().id();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let test_dir =
        std::env::temp_dir().join(format!("oxen_csv_test_{:?}_{}", unique_id, timestamp));
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).expect("Failed to create test directory");

    // Create repository with CSV file using TestRepositoryBuilder
    let test_repo = TestRepositoryBuilder::new("test_user", "csv_repo")
        .with_base_dir(&test_dir)
        .with_file("products.csv", "product,price,category\nLaptop,999.99,Electronics\nChair,149.50,Furniture\nBook,19.99,Education")
        .with_commit_message("Add CSV data")
        .build()
        .await
        .expect("Failed to create test repository");

    // Drop the repository to release any locks
    drop(test_repo);

    // Wait a moment for any background processes to complete
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // Clean up any existing lock files to prevent server startup conflicts
    // This addresses the "Resource temporarily unavailable" error
    let lock_file = test_dir.join("test_user/csv_repo/.oxen/refs/LOCK");
    if lock_file.exists() {
        let _ = std::fs::remove_file(&lock_file);
    }

    // Start oxen-server with auto-port allocation
    let server = TestServer::start_with_auto_port(&test_dir)
        .await
        .expect("Failed to start test server");

    // Create HTTP client
    let client = reqwest::Client::new();

    // Test: HTTP GET should work to retrieve the CSV file we created
    let response = client
        .get(&format!(
            "{}/api/repos/test_user/csv_repo/file/main/products.csv",
            server.base_url()
        ))
        .send()
        .await
        .expect("Failed to send HTTP GET request for CSV");

    let status = response.status();
    println!("CSV HTTP GET response status: {}", status);
    let body = response
        .text()
        .await
        .expect("Failed to read CSV response body");
    println!("CSV HTTP GET response body: {}", body);

    // Verify we can make HTTP GET request over TCP/IP (integration test requirement)
    // Accept only successful status codes (200-299 inclusive)
    assert!(
        status.as_u16() >= 200 && status.as_u16() <= 299,
        "HTTP GET over TCP/IP failed - status: {}, body: {}",
        status,
        body
    );

    // If successful, verify the CSV content is accessible
    if status.is_success() {
        assert!(
            body.contains("Laptop") || body.contains("Electronics"),
            "CSV file content not found - body: {}",
            body
        );
        println!("✅ Successfully retrieved CSV file content via HTTP GET!");
    } else {
        println!("⚠️  HTTP GET request succeeded but file access failed (expected in some test scenarios)");
    }
}
