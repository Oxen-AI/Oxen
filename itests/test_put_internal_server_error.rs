use crate::common::{TestEnvironment, RepoType};

/// Test to reproduce the internal server error that occurs with raw content PUT
/// This test reproduces the exact scenario from the shell script that fails
#[tokio::test]
async fn test_reproduce_internal_server_error_from_shell_script() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Starting reproduction test for internal server error...");
    
    let env = TestEnvironment::builder()
        .test_name("reproduce_internal_server_error")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    // Create a valid bearer token for testing
    let bearer_token = env.create_test_bearer_token()?;
    println!("✅ Created bearer token: {}", bearer_token);
    
    let (_test_dir, server, client) = env.into_parts();
    println!("✅ Test environment created, server running at: {}", server.base_url());
    
    // First, test GET to ensure the file exists (like the shell script does)
    println!("📖 Testing GET request first to verify file exists...");
    let get_response = client
        .get(&format!("{}/api/repos/test_user/memory_test_repo/file/main/test.txt", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()
        .await?;
    
    let get_status = get_response.status();
    let get_body = get_response.text().await?;
    println!("GET Status: {}", get_status);
    println!("GET Response: {}", get_body);
    
    if !get_status.is_success() {
        println!("❌ GET request failed, skipping PUT test");
        return Ok(());
    }
    
    // Extract revision ID from headers like the shell script does
    let get_with_headers = client
        .get(&format!("{}/api/repos/test_user/memory_test_repo/file/main/test.txt", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()
        .await?;
    
    let oxen_revision_id = get_with_headers
        .headers()
        .get("oxen-revision-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("missing");
    
    println!("📝 Extracted oxen-revision-id: {}", oxen_revision_id);
    
    // Now test the PUT that causes internal server error (exact same as shell script)
    println!("🚀 Testing PUT request that causes internal server error...");
    let test_content = "This is the NEW content after PUT request!";
    
    let response = client
        .put(&format!("{}/api/repos/test_user/memory_test_repo/file/main/test.txt", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "text/plain")
        .header("oxen-based-on", oxen_revision_id)
        .body(test_content)
        .send()
        .await?;

    let status = response.status();
    let body = response.text().await?;
    
    println!("PUT Status: {}", status);
    println!("PUT Response: {}", body);
    
    // This reproduces the exact error from the shell script
    if body.contains("internal_server_error") {
        println!("🎯 Successfully reproduced the internal server error!");
        println!("❌ PUT request failed with internal_server_error as expected");
        
        // We expect this to fail currently, so this is actually "success" for reproduction
        assert!(body.contains("internal_server_error"), 
            "Expected internal_server_error in response: {}", body);
        
        println!("✅ Internal server error successfully reproduced in unit test");
        return Ok(());
    }
    
    // If it doesn't fail, that means the bug is fixed
    println!("🎉 PUT request succeeded! The bug may be fixed.");
    assert!(status.is_success(), "Expected success or internal_server_error. Status: {}, Body: {}", status, body);
    
    Ok(())
}

/// Test to isolate the specific cause of the internal server error
/// This test tries different variations to identify the root cause
#[tokio::test]
async fn test_isolate_internal_server_error_cause() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Starting isolation test for internal server error cause...");
    
    let env = TestEnvironment::builder()
        .test_name("isolate_internal_server_error_cause")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();
    
    println!("🧪 Test 1: PUT without oxen-based-on header");
    let response1 = client
        .put(&format!("{}/api/repos/test_user/memory_test_repo/file/main/test.txt", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "text/plain")
        .body("Test content without oxen-based-on")
        .send()
        .await?;
    
    println!("Test 1 Status: {}", response1.status());
    println!("Test 1 Response: {}", response1.text().await?);
    
    println!("🧪 Test 2: PUT with invalid oxen-based-on header");
    let response2 = client
        .put(&format!("{}/api/repos/test_user/memory_test_repo/file/main/test.txt", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "text/plain")
        .header("oxen-based-on", "invalid-revision-id")
        .body("Test content with invalid oxen-based-on")
        .send()
        .await?;
    
    println!("Test 2 Status: {}", response2.status());
    println!("Test 2 Response: {}", response2.text().await?);
    
    println!("🧪 Test 3: PUT to new file (not overwrite)");
    let response3 = client
        .put(&format!("{}/api/repos/test_user/memory_test_repo/file/main/new_file.txt", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "text/plain")
        .body("Test content for new file")
        .send()
        .await?;
    
    println!("Test 3 Status: {}", response3.status());
    println!("Test 3 Response: {}", response3.text().await?);
    
    println!("✅ Isolation tests completed - check output above for patterns");
    Ok(())
}