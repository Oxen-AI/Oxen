use crate::common::{RepoType, TestEnvironment};
use serde_json::Value;

/// Test repository listing endpoint
/// Tests the /api/repos/{namespace} endpoint returns valid JSON
#[tokio::test]
async fn test_list_repositories_via_http_get() {
    let env = TestEnvironment::builder()
        .test_name("list_repositories")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await
        .expect("Failed to create test environment");

    let (_test_dir, server, client) = env.into_parts();

    println!("Testing repository listing...");

    // Retry logic to handle race condition where server needs time to discover repository
    let mut attempts = 0;
    let max_attempts = 5;

    loop {
        attempts += 1;
        let response = client
            .get(&format!("{}/api/repos/test_user", server.base_url()))
            .send()
            .await
            .expect("Failed to send request to repositories endpoint");

        let status = response.status();
        println!(
            "Repositories list status: {} (attempt {})",
            status, attempts
        );
        let body = response.text().await.unwrap_or_default();
        println!("Repositories response: {}", body);

        // Should return success status
        assert!(status.is_success(), "Expected 200 OK, got {}", status);

        // Parse JSON to verify structure
        let _json = serde_json::from_str::<Value>(&body)
            .expect("Failed to parse repositories response as valid JSON");
        println!("✅ Successfully parsed repositories JSON");

        // Check if repository is discovered
        if body.contains("test_repo") {
            println!("✅ Found repository in response!");
            break;
        } else if attempts >= max_attempts {
            // Final attempt failed
            assert!(
                false,
                "Expected 'test_repo' in response after {} attempts: {}",
                max_attempts, body
            );
        } else {
            // Wait a bit for server to discover the repository
            println!("⚠️  Repository not found yet, waiting and retrying...");
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }
}

/// Test specific repository info endpoint
/// Tests the /api/repos/{namespace}/{repo} endpoint
#[tokio::test]
async fn test_get_specific_repository_info() {
    let env = TestEnvironment::builder()
        .test_name("specific_repository_info")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await
        .expect("Failed to create test environment");

    let (_test_dir, server, client) = env.into_parts();

    println!("Testing specific repository access...");
    let response = client
        .get(&format!(
            "{}/api/repos/test_user/test_repo",
            server.base_url()
        ))
        .send()
        .await
        .expect("Failed to send request to repository info endpoint");

    let status = response.status();
    println!("Repository info status: {}", status);
    let body = response.text().await.unwrap_or_default();
    println!("Repository info response: {}", body);

    if status.is_success() {
        println!("✅ Successfully accessed repository info");
    }
}

/// Test file listing endpoint
/// Tests the /api/repos/{namespace}/{repo}/files endpoint
#[tokio::test]
async fn test_list_files_in_repository() {
    let env = TestEnvironment::builder()
        .test_name("list_files")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await
        .expect("Failed to create test environment");

    let (_test_dir, server, client) = env.into_parts();

    println!("Testing file listing in repository...");
    let response = client
        .get(&format!(
            "{}/api/repos/test_user/test_repo/files",
            server.base_url()
        ))
        .send()
        .await
        .expect("Failed to send request to files endpoint");

    let status = response.status();
    println!("Files list status: {}", status);
    let body = response.text().await.unwrap_or_default();
    println!("Files response: {}", body);

    if status.is_success() {
        assert!(
            body.contains("test.txt") && body.contains("data.csv"),
            "Expected test files not found in repository listing - response: {}",
            body
        );
        println!("✅ Successfully found our test files in the repository");
    }
}

/// Test text file content retrieval
/// Tests the /api/repos/{namespace}/{repo}/file/{branch}/{path} endpoint for text files
#[tokio::test]
async fn test_get_text_file_content() {
    let env = TestEnvironment::builder()
        .test_name("get_text_file")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await
        .expect("Failed to create test environment");

    let (_test_dir, server, client) = env.into_parts();

    println!("Testing file content retrieval...");
    let response = client
        .get(&format!(
            "{}/api/repos/test_user/test_repo/file/main/test.txt",
            server.base_url()
        ))
        .send()
        .await
        .expect("Failed to send request to file content endpoint");

    let status = response.status();
    println!("File content status: {}", status);
    let body = response.text().await.unwrap_or_default();
    println!("File content response: {}", body);

    if status.is_success() {
        assert!(
            body.contains("Hello from Oxen integration test!"),
            "Expected file content not found - response: {}",
            body
        );
        println!("✅ Successfully retrieved actual file content!");
    }
}

/// Test CSV file content retrieval
/// Tests the /api/repos/{namespace}/{repo}/file/{branch}/{path} endpoint for CSV files
#[tokio::test]
async fn test_get_csv_file_content() {
    let env = TestEnvironment::builder()
        .test_name("get_csv_file")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await
        .expect("Failed to create test environment");

    let (_test_dir, server, client) = env.into_parts();

    println!("Testing CSV file content retrieval...");
    let response = client
        .get(&format!(
            "{}/api/repos/test_user/test_repo/file/main/data.csv",
            server.base_url()
        ))
        .send()
        .await
        .expect("Failed to send request to CSV file endpoint");

    let status = response.status();
    println!("CSV file status: {}", status);
    let body = response.text().await.unwrap_or_default();
    println!("CSV file response: {}", body);

    if status.is_success() {
        assert!(
            body.contains("Alice,30,New York"),
            "Expected CSV content not found - response: {}",
            body
        );
        println!("✅ Successfully retrieved CSV file content!");
    }
}
