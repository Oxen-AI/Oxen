use crate::common::{RepoType, TestEnvironment};

/// Test DELETE file with bearer token authentication
/// This test demonstrates deleting a file from an existing repository with proper authentication
#[tokio::test]
async fn test_delete_file_with_bearer_token() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("delete_file_with_bearer_token")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    // Create a valid bearer token for testing
    let bearer_token = env.create_test_bearer_token()?;

    let (_test_dir, server, client) = env.into_parts();

    // Get the current revision to use in oxen-based-on header by fetching the file
    let get_response = client
        .get(&format!(
            "{}/api/repos/test_user/test_repo/file/main/test.txt",
            server.base_url()
        ))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()
        .await?;

    let current_revision = get_response
        .headers()
        .get("oxen-revision-id")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("unknown");

    let response = client
        .delete(&format!(
            "{}/api/repos/test_user/test_repo/file/main/test.txt",
            server.base_url()
        ))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("oxen-based-on", current_revision)
        .send()
        .await?;

    let status = response.status();
    let body = response.text().await?;

    println!("Status: {}", status);
    println!("Response: {}", body);
    
    // Should succeed with authentication
    assert!(status.is_success(), "Expected success with bearer token. Status: {}, Body: {}", status, body);
    
    // Check that the response indicates success
    assert!(body.contains("success") || body.contains("deleted"), 
        "Expected success response, got: {}", body);
    
    println!("✅ File deletion with bearer token succeeded");
    Ok(())
}

/// Test DELETE file without bearer token should fail
/// This test demonstrates that authentication is required for file deletion
#[tokio::test]
async fn test_delete_file_without_bearer_token_should_fail() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("delete_file_without_bearer_token")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    let (_test_dir, server, client) = env.into_parts();
    
    let response = client
        .delete(&format!("{}/api/repos/test_user/test_repo/file/main/test.txt", server.base_url()))
        .send()
        .await?;

    let status = response.status();
    let body = response.text().await?;

    println!("Status: {}", status);
    println!("Response: {}", body);
    
    // Should fail without authentication
    assert!(status.is_client_error(), "Expected client error without bearer token. Status: {}, Body: {}", status, body);
    
    // Check that the response indicates authentication is required
    assert!(body.contains("Bearer token required") || body.contains("unauthorized") || body.contains("authentication"), 
        "Expected authentication error, got: {}", body);
    
    println!("✅ File deletion without bearer token failed as expected");
    Ok(())
}

/// Test DELETE file with invalid bearer token should fail
/// This test demonstrates that a valid bearer token is required for file deletion
#[tokio::test]
async fn test_delete_file_with_invalid_bearer_token_should_fail() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("delete_file_with_invalid_bearer_token")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    let (_test_dir, server, client) = env.into_parts();
    
    // Use an invalid bearer token
    let invalid_bearer_token = "invalid-token-12345";
    
    let response = client
        .delete(&format!("{}/api/repos/test_user/test_repo/file/main/test.txt", server.base_url()))
        .header("Authorization", format!("Bearer {}", invalid_bearer_token))
        .send()
        .await?;

    let status = response.status();
    let body = response.text().await?;

    println!("Status: {}", status);
    println!("Response: {}", body);
    
    // Should fail with invalid token
    assert!(status.is_client_error(), "Expected client error with invalid bearer token. Status: {}, Body: {}", status, body);
    
    // Check that the response indicates authentication failure
    assert!(body.contains("Bearer token required") || body.contains("unauthorized") || body.contains("authentication"), 
        "Expected authentication error, got: {}", body);
    
    println!("✅ File deletion with invalid bearer token failed as expected");
    Ok(())
}

/// Test DELETE file with invalid oxen-based-on header should fail
/// This test demonstrates that a valid oxen-based-on header is required for file deletion
#[tokio::test]
async fn test_delete_file_with_invalid_oxen_based_on_should_fail() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("delete_file_invalid_oxen_based_on")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    // Create a valid bearer token for testing
    let bearer_token = env.create_test_bearer_token()?;
    
    let (_test_dir, server, client) = env.into_parts();
    
    let response = client
        .delete(&format!("{}/api/repos/test_user/test_repo/file/main/data.csv", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("oxen-based-on", "invalid-revision-hash-that-does-not-match")
        .send()
        .await?;

    let status = response.status();
    let body = response.text().await?;

    println!("Status: {}", status);
    println!("Response: {}", body);
    
    // Should fail with invalid oxen-based-on header
    assert!(status.is_client_error(), "Expected client error with invalid oxen-based-on. Status: {}, Body: {}", status, body);
    
    // Check that the response indicates revision conflict
    assert!(body.contains("revision") || body.contains("conflict") || body.contains("based-on"), 
        "Expected revision conflict error, got: {}", body);
    
    println!("✅ File deletion with invalid oxen-based-on failed as expected");
    Ok(())
}