use crate::common::{TestEnvironment, RepoType};

#[tokio::test]
async fn test_put_missing_content_type_header() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("put_missing_content_type")
        .with_repo(RepoType::Empty)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();

    let file_content = "This file has no Content-Type header";
    let response = client
        .put(&format!("{}/api/repos/test_user/empty_repo/file/main/no_content_type.txt", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        // Intentionally no Content-Type header
        .body(file_content)
        .send()
        .await?;

    let status = response.status();
    let response_body = response.text().await?;
    
    if !status.is_success() {
        eprintln!("❌ Missing Content-Type test failed: {}", status);
        eprintln!("Response: {}", response_body);
        panic!("Expected success with missing Content-Type header");
    }

    eprintln!("✅ Missing Content-Type header handled correctly");
    Ok(())
}

#[tokio::test]
async fn test_put_malformed_content_type_header() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("put_malformed_content_type")
        .with_repo(RepoType::Empty)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();

    let file_content = "This file has malformed Content-Type header";
    let response = client
        .put(&format!("{}/api/repos/test_user/empty_repo/file/main/malformed_ct.txt", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "invalid/malformed/type/with/extra/slashes")
        .body(file_content)
        .send()
        .await?;

    let status = response.status();
    let response_body = response.text().await?;
    
    if !status.is_success() {
        eprintln!("❌ Malformed Content-Type test failed: {}", status);
        eprintln!("Response: {}", response_body);
        panic!("Expected success with malformed Content-Type header");
    }

    eprintln!("✅ Malformed Content-Type header handled gracefully");
    Ok(())
}

#[tokio::test]
async fn test_put_json_content_type() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("put_json_content_type")
        .with_repo(RepoType::Empty)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();

    let json_content = r#"{"name": "test", "value": 42}"#;
    let response = client
        .put(&format!("{}/api/repos/test_user/empty_repo/file/main/data.json", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "application/json")
        .body(json_content)
        .send()
        .await?;

    let status = response.status();
    let response_body = response.text().await?;
    
    if !status.is_success() {
        eprintln!("❌ JSON Content-Type test failed: {}", status);
        eprintln!("Response: {}", response_body);
        panic!("Expected success with JSON Content-Type");
    }

    eprintln!("✅ JSON Content-Type treated as text correctly");
    Ok(())
}

#[tokio::test]
async fn test_put_xml_content_type() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("put_xml_content_type")
        .with_repo(RepoType::Empty)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();

    let xml_content = r#"<?xml version="1.0"?><root><item>test</item></root>"#;
    let response = client
        .put(&format!("{}/api/repos/test_user/empty_repo/file/main/data.xml", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "application/xml")
        .body(xml_content)
        .send()
        .await?;

    let status = response.status();
    let response_body = response.text().await?;
    
    if !status.is_success() {
        eprintln!("❌ XML Content-Type test failed: {}", status);
        eprintln!("Response: {}", response_body);
        panic!("Expected success with XML Content-Type");
    }

    eprintln!("✅ XML Content-Type treated as text correctly");
    Ok(())
}

#[tokio::test]
async fn test_put_yaml_content_type() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("put_yaml_content_type")
        .with_repo(RepoType::Empty)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();

    let yaml_content = "name: test\nvalue: 42\nlist:\n  - item1\n  - item2";
    let response = client
        .put(&format!("{}/api/repos/test_user/empty_repo/file/main/config.yaml", server.base_url()))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .header("Content-Type", "application/yaml")
        .body(yaml_content)
        .send()
        .await?;

    let status = response.status();
    let response_body = response.text().await?;
    
    if !status.is_success() {
        eprintln!("❌ YAML Content-Type test failed: {}", status);
        eprintln!("Response: {}", response_body);
        panic!("Expected success with YAML Content-Type");
    }

    eprintln!("✅ YAML Content-Type treated as text correctly");
    Ok(())
}