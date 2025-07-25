use crate::common::{TestEnvironment, TestRepoBuilder, RepoType};
use reqwest::multipart::Form;
use std::path::PathBuf;

/// Test PUT file with bearer token authentication
/// This test demonstrates uploading a file to an existing repository with proper authentication
#[tokio::test]
async fn test_update_file_with_bearer_token() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("update_file_with_bearer_token")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    // Create a valid bearer token for testing
    let bearer_token = env.create_test_bearer_token()?;
    
    let (_test_dir, server, client) = env.into_parts();
    
    // Create test file content
    let test_content = b"This is a test file for upload";
    let file_part = reqwest::multipart::Part::bytes(test_content.to_vec())
        .file_name("test_upload.txt")
        .mime_str("text/plain")?;
    
    let form = Form::new()
        .part("file", file_part)
        .text("name", "Test Author")
        .text("email", "test@example.com") 
        .text("message", "Upload test file");
    
    let response = client
        .put(&format!("{}/api/repos/test_user/test_repo/file/main/test_data", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .multipart(form)
        .send()
        .await?;

    let status = response.status();
    let body = response.text().await?;
    
    println!("Status: {}", status);
    println!("Response: {}", body);
    
    // Should succeed with authentication
    assert!(status.is_success(), "Expected success with bearer token. Status: {}, Body: {}", status, body);
    
    // Check that the response indicates success
    assert!(body.contains("success") || body.contains("created"), 
        "Expected success response, got: {}", body);
    
    println!("✅ File upload with bearer token succeeded");
    Ok(())
}

/// Test PUT file on empty repository with bearer token authentication
/// This test demonstrates uploading a file to an empty repository with proper authentication
#[tokio::test]
async fn test_update_file_on_empty_repo_with_bearer_token() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("update_file_empty_repo_with_bearer_token")
        .with_repo(RepoType::Empty)
        .build()
        .await?;

    // Create a valid bearer token for testing
    let bearer_token = env.create_test_bearer_token()?;
    
    let (_test_dir, server, client) = env.into_parts();
    
    // Create test file content
    let test_content = b"This is a test file for upload to empty repo";
    let file_part = reqwest::multipart::Part::bytes(test_content.to_vec())
        .file_name("test_upload.txt")
        .mime_str("text/plain")?;
    
    let form = Form::new()
        .part("file", file_part)
        .text("name", "Test Author")
        .text("email", "test@example.com")
        .text("message", "Upload test file to empty repo");
    
    let response = client
        .put(&format!("{}/api/repos/test_user/empty_repo/file/main/test_data", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .multipart(form)
        .send()
        .await?;

    let status = response.status();
    let body = response.text().await?;
    
    println!("Status: {}", status);
    println!("Response: {}", body);
    
    // Should succeed with authentication
    assert!(status.is_success(), "Expected success with bearer token. Status: {}, Body: {}", status, body);
    
    // Check that the response indicates success
    assert!(body.contains("success") || body.contains("created"), 
        "Expected success response, got: {}", body);
    
    println!("✅ File upload to empty repo with bearer token succeeded");
    Ok(())
}

/// Test PUT file without bearer token should fail
/// This test demonstrates that authentication is required for file uploads
#[tokio::test]
async fn test_update_file_without_bearer_token_should_fail() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("update_file_without_bearer_token")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    let (_test_dir, server, client) = env.into_parts();
    
    // Create test file content
    let test_content = b"This should fail without authentication";
    let file_part = reqwest::multipart::Part::bytes(test_content.to_vec())
        .file_name("test_upload.txt")
        .mime_str("text/plain")?;
    
    let form = Form::new()
        .part("file", file_part)
        .text("name", "Test Author")
        .text("email", "test@example.com")
        .text("message", "This should fail");
    
    let response = client
        .put(&format!("{}/api/repos/test_user/test_repo/file/main/test_data", server.base_url()))
        .multipart(form)
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
    
    println!("✅ File upload without bearer token failed as expected");
    Ok(())
}

/// Test PUT file with invalid bearer token should fail
/// This test demonstrates that a valid bearer token is required for file uploads
#[tokio::test]
async fn test_update_file_with_invalid_bearer_token_should_fail() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("update_file_with_invalid_bearer_token")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    let (_test_dir, server, client) = env.into_parts();
    
    // Use an invalid bearer token
    let invalid_bearer_token = "invalid-token-12345";
    
    // Create test file content
    let test_content = b"This should fail with invalid token";
    let file_part = reqwest::multipart::Part::bytes(test_content.to_vec())
        .file_name("test_upload.txt")
        .mime_str("text/plain")?;
    
    let form = Form::new()
        .part("file", file_part)
        .text("name", "Test Author")
        .text("email", "test@example.com")
        .text("message", "This should fail with invalid token");
    
    let response = client
        .put(&format!("{}/api/repos/test_user/test_repo/file/main/test_data", server.base_url()))
        .header("Authorization", format!("Bearer {}", invalid_bearer_token))
        .multipart(form)
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
    
    println!("✅ File upload with invalid bearer token failed as expected");
    Ok(())
}