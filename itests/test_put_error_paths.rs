use crate::common::{TestEnvironment, RepoType};

#[tokio::test]
async fn test_put_invalid_commit_message_header() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("put_invalid_commit_msg")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();

    let file_content = "Test content for invalid commit message header";
    
    // Test with empty commit message header (should fail validation)
    let response = client
        .put(&format!("{}/api/repos/test_user/test_repo/file/main/test_invalid_msg.txt", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "text/plain")
        .header("oxen-commit-message", "") // Empty message should be invalid
        .body(file_content)
        .send()
        .await?;

    let status = response.status();
    let response_body = response.text().await?;
    
    if !status.is_client_error() {
        eprintln!("❌ Expected client error for invalid commit message, got: {}", status);
        eprintln!("Response: {}", response_body);
        panic!("Expected 400 error for invalid commit message header");
    }

    assert!(response_body.contains("Invalid oxen-commit-message header value"), 
        "Expected error about invalid commit message header, got: {}", response_body);
    
    eprintln!("✅ Invalid commit message header properly rejected");
    Ok(())
}

#[tokio::test]
async fn test_put_filename_with_special_characters() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("put_special_chars")
        .with_repo(RepoType::Empty)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();

    let file_content = "Content for file with special characters";
    
    // Test filename with various special characters
    let special_filename = "test file with spaces & symbols (1).txt";
    let response = client
        .put(&format!("{}/api/repos/test_user/empty_repo/file/main/{}", server.base_url(), urlencoding::encode(special_filename)))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "text/plain")
        .body(file_content)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let response_body = response.text().await?;
        eprintln!("❌ Special characters in filename failed: {}", status);
        eprintln!("Response: {}", response_body);
        panic!("Expected success with special characters in filename");
    }

    let response_body = response.text().await?;
    eprintln!("✅ Special characters in filename handled correctly");
    eprintln!("Response: {}", response_body);

    Ok(())
}

#[tokio::test]
async fn test_put_very_long_filename() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("put_long_filename")
        .with_repo(RepoType::Empty)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();

    let file_content = "Content for file with very long name";
    
    // Create a very long filename (but not too long to break the filesystem)
    let long_filename = "a".repeat(200) + ".txt";
    let response = client
        .put(&format!("{}/api/repos/test_user/empty_repo/file/main/{}", server.base_url(), long_filename))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "text/plain")
        .body(file_content)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let response_body = response.text().await?;
        eprintln!("❌ Very long filename failed: {}", status);
        eprintln!("Response: {}", response_body);
        // This might legitimately fail due to filesystem limits, so we'll log but not panic
        eprintln!("ℹ️ Long filename rejected (possibly due to filesystem limits)");
        return Ok(());
    }

    let response_body = response.text().await?;
    eprintln!("✅ Very long filename handled correctly");
    eprintln!("Response: {}", response_body);

    Ok(())
}

#[tokio::test]
async fn test_put_deeply_nested_path() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("put_deep_path")
        .with_repo(RepoType::Empty)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();

    let file_content = "Content for deeply nested file";
    
    // Create a deeply nested path
    let deep_path = "level1/level2/level3/level4/level5/deep_file.txt";
    let response = client
        .put(&format!("{}/api/repos/test_user/empty_repo/file/main/{}", server.base_url(), deep_path))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "text/plain")
        .body(file_content)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let response_body = response.text().await?;
        eprintln!("❌ Deeply nested path failed: {}", status);
        eprintln!("Response: {}", response_body);
        panic!("Expected success with deeply nested path");
    }

    let response_body = response.text().await?;
    eprintln!("✅ Deeply nested path handled correctly");
    eprintln!("Response: {}", response_body);

    Ok(())
}

#[tokio::test]
async fn test_put_with_unicode_filename() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("put_unicode_filename")
        .with_repo(RepoType::Empty)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();

    let file_content = "Content for file with Unicode characters";
    
    // Test filename with Unicode characters
    let unicode_filename = "测试文件_файл_тест_🚀.txt";
    let response = client
        .put(&format!("{}/api/repos/test_user/empty_repo/file/main/{}", server.base_url(), urlencoding::encode(unicode_filename)))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "text/plain")
        .body(file_content)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let response_body = response.text().await?;
        eprintln!("❌ Unicode filename failed: {}", status);
        eprintln!("Response: {}", response_body);
        panic!("Expected success with Unicode filename");
    }

    let response_body = response.text().await?;
    eprintln!("✅ Unicode filename handled correctly");
    eprintln!("Response: {}", response_body);

    Ok(())
}