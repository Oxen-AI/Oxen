use crate::common::{TestRepositoryBuilder, TestServer};
use std::time::Duration;

/// Example test demonstrating the fluent TestRepositoryBuilder API
#[tokio::test]
async fn test_repository_builder_fluent_api() {
    // Create a test repository using the fluent builder API
    let test_repo = TestRepositoryBuilder::new("namespace", "repo_name")
        .with_file("data.csv", "id,name\n1,Alice\n2,Bob")
        .with_file("config.json", r#"{"version": "1.0"}"#)
        .with_commit_message("Test data setup")
        .build()
        .await
        .unwrap();

    // Verify the repository was created correctly
    assert_eq!(test_repo.files().len(), 2);
    assert_eq!(
        test_repo.file_content("data.csv").unwrap(),
        "id,name\n1,Alice\n2,Bob"
    );
    assert_eq!(
        test_repo.file_content("config.json").unwrap(),
        r#"{"version": "1.0"}"#
    );

    // The repository directory should exist
    assert!(test_repo.repo_dir().exists());
    assert!(test_repo.repo_dir().join("data.csv").exists());
    assert!(test_repo.repo_dir().join("config.json").exists());

    println!("✅ Repository created successfully with fluent API");
}

/// Example test demonstrating repository with server integration
#[tokio::test]
async fn test_repository_builder_with_server() {
    // Create base directory for test
    let test_dir = std::env::temp_dir().join("oxen_fluent_test");
    let _ = std::fs::remove_dir_all(&test_dir);

    // Create repository with fluent API
    let _test_repo = TestRepositoryBuilder::new("test_user", "fluent_repo")
        .with_base_dir(&test_dir)
        .with_file(
            "products.csv",
            "product,price,category\nLaptop,999.99,Electronics\nChair,149.50,Furniture",
        )
        .with_file(
            "readme.md",
            "# Test Repository\n\nThis is a test repository created with fluent API.",
        )
        .with_user("Fluent User", "fluent@example.com")
        .with_commit_message("Initial setup with fluent API")
        .build()
        .await
        .unwrap();

    // Start server to test HTTP endpoints
    let server = TestServer::start_with_sync_dir(&test_dir, 3021)
        .await
        .expect("Failed to start test server");

    // Test HTTP client
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("Failed to create HTTP client");

    // Test health endpoint
    let response = client
        .get(&format!("{}/api/health", server.base_url()))
        .send()
        .await
        .expect("Failed to send health request");

    assert!(response.status().is_success());
    println!("✅ Server health check passed with fluent API repository");

    // Clean up
    let _ = std::fs::remove_dir_all(&test_dir);
}

/// Example test showing more advanced builder usage
#[tokio::test]
async fn test_repository_builder_advanced() {
    let mut test_repo = TestRepositoryBuilder::new("advanced", "features")
        .with_file("src/main.rs", "fn main() { println!(\"Hello, World!\"); }")
        .with_file(
            "Cargo.toml",
            "[package]\nname = \"test\"\nversion = \"0.1.0\"",
        )
        .with_filesystem_storage() // Use real filesystem instead of in-memory
        .with_user("Advanced User", "advanced@test.com")
        .with_commit_message("Initial Rust project")
        .build()
        .await
        .unwrap();

    // Add more files after creation
    test_repo
        .add_file(
            "src/lib.rs",
            "pub fn hello() { println!(\"Hello from lib!\"); }",
        )
        .await
        .expect("Failed to add lib.rs");

    test_repo
        .add_file(
            "tests/integration.rs",
            "#[test]\nfn test_hello() { assert_eq!(2 + 2, 4); }",
        )
        .await
        .expect("Failed to add integration test");

    // Commit the new files
    test_repo
        .commit("Add library and tests")
        .expect("Failed to commit changes");

    // Verify all files exist
    assert!(test_repo.repo_dir().join("src/main.rs").exists());
    assert!(test_repo.repo_dir().join("src/lib.rs").exists());
    assert!(test_repo.repo_dir().join("tests/integration.rs").exists());
    assert!(test_repo.repo_dir().join("Cargo.toml").exists());

    println!("✅ Advanced repository builder test completed");
}
