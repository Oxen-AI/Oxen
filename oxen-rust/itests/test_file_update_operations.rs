use crate::common::{TestEnvironment, RepoType};
use reqwest::multipart::Form;

/// Test PUT file update operation with bearer token authentication  
/// This test demonstrates updating a file in an existing repository with proper authentication
#[tokio::test]
async fn test_update_file() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("test_update_file")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    // Create a valid bearer token for testing
    let bearer_token = env.create_test_bearer_token()?;
    
    let (_test_dir, server, client) = env.into_parts();
    
    // Create test file content (using a test image path like the original)
    let test_content = b"test image content for update"; // Simulating image content
    let file_part = reqwest::multipart::Part::bytes(test_content.to_vec())
        .file_name("test.jpeg")
        .mime_str("image/jpeg")?;
    
    let form = Form::new()
        .part("file", file_part)
        .text("name", "Test Author")
        .text("email", "test@example.com") 
        .text("message", "Update file test");
    
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
    
    // Check that the response indicates resource was created
    assert!(body.contains("resource_created") || body.contains("success") || body.contains("created"), 
        "Expected resource_created response, got: {}", body);
    
    println!("✅ File update operation succeeded");
    Ok(())
}

/// Test PUT file update operation on empty repository with bearer token authentication
/// This test demonstrates updating a file in an empty repository with proper authentication  
#[tokio::test]
async fn test_update_file_on_empty_repo() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("test_update_file_on_empty_repo")
        .with_repo(RepoType::Empty)
        .build()
        .await?;

    // Create a valid bearer token for testing
    let bearer_token = env.create_test_bearer_token()?;
    
    let (_test_dir, server, client) = env.into_parts();
    
    // Create test file content (using a test image path like the original)
    let test_content = b"test image content for empty repo"; // Simulating image content
    let file_part = reqwest::multipart::Part::bytes(test_content.to_vec())
        .file_name("test.jpeg")
        .mime_str("image/jpeg")?;
    
    let form = Form::new()
        .part("file", file_part)
        .text("name", "Test Author")
        .text("email", "test@example.com")
        .text("message", "Update file test");
    
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
    
    // Check that the response indicates resource was created
    assert!(body.contains("resource_created") || body.contains("success") || body.contains("created"), 
        "Expected resource_created response, got: {}", body);
    
    println!("✅ File update operation on empty repo succeeded");
    Ok(())
}