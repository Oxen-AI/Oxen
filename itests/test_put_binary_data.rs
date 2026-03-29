use crate::common::{TestEnvironment, RepoType};

#[tokio::test]
async fn test_put_raw_payload_binary_file() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("put_binary_file")
        .with_repo(RepoType::Empty)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();

    // Create some binary data (a simple PNG-like header)
    let binary_data = vec![
        0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, // PNG signature
        0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52, // IHDR chunk
        0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, // 1x1 pixel
        0x08, 0x02, 0x00, 0x00, 0x00, 0x90, 0x77, 0x53, // color type, etc.
    ];

    let response = client
        .put(&format!("{}/api/repos/test_user/empty_repo/file/main/test.png", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "image/png")
        .body(binary_data.clone())
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let response_body = response.text().await?;
        eprintln!("❌ Binary file upload failed: {}", status);
        eprintln!("Response: {}", response_body);
        panic!("Expected success with binary PNG file");
    }

    let response_body = response.text().await?;
    eprintln!("✅ Binary PNG file uploaded successfully");
    eprintln!("Response: {}", response_body);

    Ok(())
}

#[tokio::test]
async fn test_put_raw_payload_pdf_file() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("put_pdf_file")
        .with_repo(RepoType::Empty)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();

    // Create some binary data (PDF header)
    let binary_data = vec![
        0x25, 0x50, 0x44, 0x46, 0x2D, 0x31, 0x2E, 0x34, // %PDF-1.4
        0x0A, 0x25, 0xC7, 0xEC, 0x8F, 0xA2, 0x0A,       // binary comment
        0x31, 0x20, 0x30, 0x20, 0x6F, 0x62, 0x6A,       // "1 0 obj"
        0x0A, 0x3C, 0x3C, 0x0A, 0x2F, 0x54, 0x79,       // "<<\n/Ty"
    ];

    let response = client
        .put(&format!("{}/api/repos/test_user/empty_repo/file/main/document.pdf", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "application/pdf")
        .body(binary_data.clone())
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let response_body = response.text().await?;
        eprintln!("❌ Binary PDF upload failed: {}", status);
        eprintln!("Response: {}", response_body);
        panic!("Expected success with binary PDF file");
    }

    let response_body = response.text().await?;
    eprintln!("✅ Binary PDF file uploaded successfully");
    eprintln!("Response: {}", response_body);

    Ok(())
}

#[tokio::test]
async fn test_put_raw_payload_invalid_utf8() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("put_invalid_utf8")
        .with_repo(RepoType::Empty)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();

    // Create data with invalid UTF-8 sequences but mark as text
    let mut data = "Valid UTF-8 text with ".as_bytes().to_vec();
    data.extend_from_slice(&[0xFF, 0xFE, 0xFD]); // Invalid UTF-8 bytes
    data.extend_from_slice(" and more text".as_bytes());

    let response = client
        .put(&format!("{}/api/repos/test_user/empty_repo/file/main/invalid_utf8.txt", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "text/plain")
        .body(data)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let response_body = response.text().await?;
        eprintln!("❌ Invalid UTF-8 handling failed: {}", status);
        eprintln!("Response: {}", response_body);
        panic!("Expected success with invalid UTF-8 in text file");
    }

    let response_body = response.text().await?;
    eprintln!("✅ Invalid UTF-8 handled with lossy conversion");
    eprintln!("Response: {}", response_body);

    Ok(())
}

#[tokio::test]
async fn test_put_octet_stream_as_binary() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("put_octet_stream")
        .with_repo(RepoType::Empty)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();

    // Create arbitrary binary data
    let binary_data: Vec<u8> = (0..256).map(|i| i as u8).collect();

    let response = client
        .put(&format!("{}/api/repos/test_user/empty_repo/file/main/binary.bin", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "application/octet-stream")
        .body(binary_data.clone())
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let response_body = response.text().await?;
        eprintln!("❌ Octet stream upload failed: {}", status);
        eprintln!("Response: {}", response_body);
        panic!("Expected success with octet-stream");
    }

    let response_body = response.text().await?;
    eprintln!("✅ Octet stream treated as binary correctly");
    eprintln!("Response: {}", response_body);

    Ok(())
}

#[tokio::test]
async fn test_put_empty_payload() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("put_empty_payload")
        .with_repo(RepoType::Empty)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();

    let response = client
        .put(&format!("{}/api/repos/test_user/empty_repo/file/main/empty.txt", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "text/plain")
        .body("")
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let response_body = response.text().await?;
        eprintln!("❌ Empty payload upload failed: {}", status);
        eprintln!("Response: {}", response_body);
        panic!("Expected success with empty payload");
    }

    let response_body = response.text().await?;
    eprintln!("✅ Empty payload handled correctly");
    eprintln!("Response: {}", response_body);

    Ok(())
}