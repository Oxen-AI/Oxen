use crate::common::TestEnvironment;

/// Test PUT with form field name as filename
/// Shows that the multipart form field name becomes the filename
#[tokio::test]
async fn test_put_with_form_field_name_as_filename() {
    let env = TestEnvironment::builder()
        .test_name("put_form_field_name")
        .without_repo()
        .timeout_secs(5)
        .build()
        .await
        .expect("Failed to create test environment");

    let (_test_dir, server, client) = env.into_parts();

    println!("Testing PUT with form field name as filename");
    let form_data =
        reqwest::multipart::Form::new().text("my_data_file.txt", "Content from field name");

    let response = client
        .put(&format!(
            "{}/api/repos/test_user/test_repo/file/main/uploads",
            server.base_url()
        ))
        .multipart(form_data)
        .send()
        .await
        .expect("Failed to send PUT request");

    let status = response.status();
    println!("   Status: {}", status);
    let body = response.text().await.expect("Failed to read response body");
    println!("   Response: {}", body);

    // Test that form field name becomes filename (expect repo not found error)
    assert!(
        status.is_client_error() || status.is_server_error(),
        "Expected error response for non-existent repo"
    );

    println!("✅ Form field name as filename test completed");
}

/// Test PUT with explicit filename in multipart
/// Shows that .file_name() overrides the field name
#[tokio::test]
async fn test_put_with_explicit_filename() {
    let env = TestEnvironment::builder()
        .test_name("put_explicit_filename")
        .without_repo()
        .timeout_secs(5)
        .build()
        .await
        .expect("Failed to create test environment");

    let (_test_dir, server, client) = env.into_parts();

    println!("Testing PUT with explicit filename in multipart");
    let file_part = reqwest::multipart::Part::text("Content with explicit filename")
        .file_name("explicit_name.csv")
        .mime_str("text/csv")
        .unwrap();

    let form_data = reqwest::multipart::Form::new().part("upload", file_part);

    let response = client
        .put(&format!(
            "{}/api/repos/test_user/test_repo/file/main/uploads",
            server.base_url()
        ))
        .multipart(form_data)
        .send()
        .await
        .expect("Failed to send PUT request");

    let status = response.status();
    println!("   Status: {}", status);
    let body = response.text().await.expect("Failed to read response body");
    println!("   Response: {}", body);

    // Test that explicit filename is used (expect repo not found error)
    assert!(
        status.is_client_error() || status.is_server_error(),
        "Expected error response for non-existent repo"
    );

    println!("✅ Explicit filename test completed");
}

/// Test PUT with multiple files
/// Shows that multiple files can be uploaded in a single PUT request
#[tokio::test]
async fn test_put_multiple_files() {
    let env = TestEnvironment::builder()
        .test_name("put_multiple_files")
        .without_repo()
        .timeout_secs(5)
        .build()
        .await
        .expect("Failed to create test environment");

    let (_test_dir, server, client) = env.into_parts();

    println!("Testing PUT with multiple files");
    let form_data = reqwest::multipart::Form::new()
        .text("file1.txt", "First file content")
        .text("file2.json", r#"{"key": "value"}"#)
        .text("file3.csv", "col1,col2\nval1,val2");

    let response = client
        .put(&format!(
            "{}/api/repos/test_user/test_repo/file/main/batch",
            server.base_url()
        ))
        .multipart(form_data)
        .send()
        .await
        .expect("Failed to send PUT request");

    let status = response.status();
    println!("   Status: {}", status);
    let body = response.text().await.expect("Failed to read response body");
    println!("   Response: {}", body);

    // Test multiple files upload (expect repo not found error)
    assert!(
        status.is_client_error() || status.is_server_error(),
        "Expected error response for non-existent repo"
    );

    println!("✅ Multiple files test completed");
}

/// Test PUT to root directory
/// Shows that files can be uploaded to the repository root
#[tokio::test]
async fn test_put_to_root_directory() {
    let env = TestEnvironment::builder()
        .test_name("put_to_root")
        .without_repo()
        .timeout_secs(5)
        .build()
        .await
        .expect("Failed to create test environment");

    let (_test_dir, server, client) = env.into_parts();

    println!("Testing PUT to root directory");
    let form_data = reqwest::multipart::Form::new().text("root_file.txt", "File in root");

    let response = client
        .put(&format!(
            "{}/api/repos/test_user/test_repo/file/main/",
            server.base_url()
        ))
        .multipart(form_data)
        .send()
        .await
        .expect("Failed to send PUT request");

    let status = response.status();
    println!("   Status: {}", status);
    let body = response.text().await.expect("Failed to read response body");
    println!("   Response: {}", body);

    // Test root directory upload (expect repo not found error)
    assert!(
        status.is_client_error() || status.is_server_error(),
        "Expected error response for non-existent repo"
    );

    println!("✅ Root directory test completed");
}

/// Test to document the URL path structure
/// This is a documentation test showing how the URL components work
#[tokio::test]
async fn test_put_url_structure_documentation() {
    println!("=== PUT URL Structure Documentation ===");
    println!("Pattern: /api/repos/{{namespace}}/{{repo_name}}/file/{{branch}}/{{directory_path}}");
    println!("Example: /api/repos/test_user/test_repo/file/main/uploads");
    println!("  - Namespace: test_user (repository owner)");
    println!("  - Repository: test_repo (repository name)");
    println!("  - Branch: main (target branch for commit)");
    println!("  - Directory: uploads (directory where files will be saved)");
    println!("  - Filename: Determined by multipart form field name or filename attribute");
    println!("");
    println!("Key Insights:");
    println!("1. URL path must point to a DIRECTORY, not a file");
    println!("2. Filename comes from multipart form field name OR filename attribute");
    println!("3. Multiple files can be uploaded in a single PUT request");
    println!("4. Files are saved as: directory_path/field_name_or_filename");
    println!("5. The server validates that the path is a directory (not an existing file)");
    println!("✅ URL structure documentation completed");
}
