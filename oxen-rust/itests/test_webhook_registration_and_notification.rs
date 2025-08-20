use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use actix_web::{web, App, HttpServer, HttpResponse, Result as ActixResult};
use serde_json::{json, Value};

use crate::common::{TestEnvironment, RepoType};

// Helper function to get current commit ID directly from the repository
fn get_current_commit_id_from_repo(repo_path: &std::path::Path) -> Result<String, Box<dyn std::error::Error>> {
    let repo = liboxen::model::LocalRepository::from_dir(repo_path)?;
    let head_commit = liboxen::repositories::commits::head_commit_maybe(&repo)?;
    
    match head_commit {
        Some(commit) => {
            println!("Found HEAD commit: {}", commit.id);
            Ok(commit.id)
        }
        None => {
            println!("No HEAD commit found, using dummy");
            Ok("dummy-current-commit".to_string())
        }
    }
}

/// Mock webhook receiver that captures all webhook notifications
#[derive(Debug, Clone, Default)]
struct MockWebhookReceiver {
    notifications: Arc<Mutex<Vec<Value>>>,
    response_status: Arc<Mutex<u16>>,
}

impl MockWebhookReceiver {
    fn new() -> Self {
        Self::default()
    }

    async fn set_response_status(&self, status: u16) {
        *self.response_status.lock().await = status;
    }

    async fn get_notifications(&self) -> Vec<Value> {
        self.notifications.lock().await.clone()
    }

    #[allow(dead_code)]
    async fn clear_notifications(&self) {
        self.notifications.lock().await.clear();
    }
}

async fn webhook_handler(
    payload: web::Json<Value>,
    data: web::Data<MockWebhookReceiver>,
) -> ActixResult<HttpResponse> {
    // Store the notification
    data.notifications.lock().await.push(payload.into_inner());
    
    // Return configured status
    let status = *data.response_status.lock().await;
    match status {
        200 => Ok(HttpResponse::Ok().json(json!({"status": "received"}))),
        404 => Ok(HttpResponse::NotFound().json(json!({"error": "not found"}))),
        500 => Ok(HttpResponse::InternalServerError().json(json!({"error": "internal error"}))),
        _ => Ok(HttpResponse::Ok().json(json!({"status": "received"}))),
    }
}

async fn start_mock_webhook_server(receiver: MockWebhookReceiver) -> (String, actix_web::dev::ServerHandle) {
    // Find available port
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let webhook_url = format!("http://127.0.0.1:{}/webhook", port);
    
    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(receiver.clone()))
            .route("/webhook", web::post().to(webhook_handler))
    })
    .bind(format!("127.0.0.1:{}", port))
    .unwrap()
    .run();

    let handle = server.handle();
    tokio::spawn(server);
    
    // Wait a bit for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    (webhook_url, handle)
}

#[tokio::test]
async fn test_webhook_registration_requires_authentication() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("webhook_auth_test")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    let (_test_dir, server, client) = env.into_parts();

    let namespace = "test_user";
    let repo_name = "test_repo";

    // First, test that the server is working by hitting a simple endpoint
    let health_response = client
        .get(&format!("{}/api/health", server.base_url()))
        .send()
        .await
        .expect("Failed to send health request");
    println!("Health check status: {}", health_response.status());

    // Try to register webhook without authentication - should fail
    let webhook_request = json!({
        "path": format!("/{}/{}", namespace, repo_name),
        "webhook_url": "http://example.com/webhook",
        "currentOxenRevision": "dummy-revision",  
        "purpose": "test webhook",
        "contact": "admin@company.com"
    });

    let response = client
        .post(&format!("{}/api/repos/{}/{}/webhooks/add", server.base_url(), namespace, repo_name))
        .json(&webhook_request)
        .send()
        .await
        .expect("Failed to send request");

    // Debug the actual response
    let status = response.status();
    let response_body = response.text().await.expect("Failed to get response body");
    println!("Response status: {}", status);
    println!("Response body: {}", response_body);
    
    // Should return 401 Unauthorized
    assert_eq!(status, 401, "Expected 401 Unauthorized for unauthenticated request. Got: {} with body: {}", status, response_body);

    Ok(())
}

#[tokio::test]
async fn test_webhook_registration_with_invalid_revision() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("webhook_invalid_revision_test")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    let (_test_dir, server, client) = env.into_parts();

    let namespace = "test_user";
    let repo_name = "test_repo";

    // Try to register webhook with invalid revision
    let webhook_request = json!({
        "path": format!("/{}/{}", namespace, repo_name),
        "webhook_url": "http://example.com/webhook",
        "currentOxenRevision": "invalid-revision-hash",
        "purpose": "test webhook", 
        "contact": "admin@company.com"
    });

    let response = client
        .post(&format!("{}/api/repos/{}/{}/webhooks/add", server.base_url(), namespace, repo_name))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .json(&webhook_request)
        .send()
        .await
        .expect("Failed to send request");

    // Should return 400 with "no" error
    assert_eq!(response.status(), 400, "Expected 400 Bad Request for invalid revision");
    
    let response_json: Value = response.json().await.expect("Failed to parse response");
    assert_eq!(response_json["error"], "no", "Expected 'no' error response");

    Ok(())
}

#[tokio::test]
async fn test_webhook_registration_success() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("webhook_success_test")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    
    // Get the repo path and test directory before destructuring the environment
    let repo_path = env.test_dir().join("test_user").join("test_repo");
    let current_commit_id = get_current_commit_id_from_repo(&repo_path)?;
    println!("Got current commit ID: {}", current_commit_id);
    let _test_dir = env.test_dir().to_path_buf(); // Keep reference to test dir
    
    let (_test_dir, server, client) = env.into_parts();

    let namespace = "test_user";
    let repo_name = "test_repo";

    // Register webhook with correct revision
    let webhook_request = json!({
        "path": format!("/{}/{}", namespace, repo_name),
        "webhook_url": "http://example.com/webhook",
        "currentOxenRevision": current_commit_id,
        "purpose": "integration test webhook",
        "contact": "admin@company.com"
    });

    let response = client
        .post(&format!("{}/api/repos/{}/{}/webhooks/add", server.base_url(), namespace, repo_name))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .json(&webhook_request)
        .send()
        .await
        .expect("Failed to send webhook registration request");

    // Debug the response
    let status = response.status();
    println!("Webhook registration status: {}", status);
    println!("Used commit ID: {}", current_commit_id);

    if status != 200 {
        let response_text = response.text().await.expect("Failed to get response text");
        println!("Webhook registration error response: {}", response_text);
        panic!("Expected 200 OK for valid webhook registration. Got: {} with response: {}", status, response_text);
    }

    let webhook_response: Value = response.json().await.expect("Failed to parse webhook response");
    assert!(webhook_response["id"].is_string(), "Expected webhook ID in response");
    let _webhook_id = webhook_response["id"].as_str().unwrap();

    // Verify webhook is stored by listing webhooks
    let list_response = client
        .get(&format!("{}/api/repos/{}/{}/webhooks", server.base_url(), namespace, repo_name))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()
        .await
        .expect("Failed to send list webhooks request");

    assert_eq!(list_response.status(), 200);
    let list_data: Value = list_response.json().await.expect("Failed to parse list response");
    assert!(list_data["webhooks"].is_array());
    assert_eq!(list_data["webhooks"].as_array().unwrap().len(), 1);

    println!("✅ Webhook registration test passed!");
    
    Ok(())
}

#[tokio::test]
async fn test_webhook_auto_removal_after_consecutive_failures() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("webhook_auto_removal_test")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    
    // Get the repo path and current commit ID before destructuring the environment
    let repo_path = env.test_dir().join("test_user").join("test_repo");
    let current_commit_id = get_current_commit_id_from_repo(&repo_path)?;
    
    let (_test_dir, server, client) = env.into_parts();

    let namespace = "test_user";
    let repo_name = "test_repo";

    // Start mock webhook server that returns 500 errors
    let webhook_receiver = MockWebhookReceiver::new();
    webhook_receiver.set_response_status(500).await;
    let (webhook_url, webhook_handle) = start_mock_webhook_server(webhook_receiver.clone()).await;

    // Register webhook
    let webhook_request = json!({
        "path": format!("/{}/{}", namespace, repo_name),
        "webhook_url": webhook_url,
        "currentOxenRevision": current_commit_id,
        "purpose": "failing webhook test",
        "contact": "admin@company.com"
    });

    let response = client
        .post(&format!("{}/api/repos/{}/{}/webhooks/add", server.base_url(), namespace, repo_name))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .json(&webhook_request)
        .send()
        .await
        .expect("Failed to register webhook");

    // Debug webhook registration
    let status = response.status();
    let response_text = response.text().await.expect("Failed to get response text");
    println!("Webhook registration status: {}", status);
    println!("Webhook registration response: {}", response_text);

    assert_eq!(status, 200, "Expected 200 OK for webhook registration. Got: {} with response: {}", status, response_text);

    // Directly trigger 5 consecutive webhook failures to test auto-removal
    use liboxen::core::webhooks::WebhookNotifier;
    let mut notifier = WebhookNotifier::new_with_rate_limit(0); // No rate limiting for auto-removal test
    
    for i in 1..=5 {
        println!("Triggering webhook notification attempt {}", i);
        
        // Trigger webhook notification - this should fail since mock server returns 500
        // Use the same path that the webhook was registered for
        let notification_count = notifier.notify_path_changed(&repo_path, &format!("/{}/{}/test_file_{}.txt", namespace, repo_name, i)).await?;
        println!("Notification attempt {} sent to {} webhooks", i, notification_count);
        
        // Wait for webhook processing
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Wait a bit more for auto-removal processing
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Check that webhook was auto-removed
    let list_response = client
        .get(&format!("{}/api/repos/{}/{}/webhooks", server.base_url(), namespace, repo_name))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()
        .await
        .expect("Failed to list webhooks");

    let list_data: Value = list_response.json().await.expect("Failed to parse response");
    assert_eq!(list_data["webhooks"].as_array().unwrap().len(), 0, "Webhook should be auto-removed after 5 failures");

    // Check stats show auto-removal
    let stats_response = client
        .get(&format!("{}/api/repos/{}/{}/webhooks/stats", server.base_url(), namespace, repo_name))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()
        .await
        .expect("Failed to get stats");

    let stats_data: Value = stats_response.json().await.expect("Failed to parse stats");
    // Note: stats will show 0 total_webhooks since the webhook was removed
    assert_eq!(stats_data["stats"]["total_webhooks"], 0);

    webhook_handle.stop(true).await;
    
    Ok(())
}

#[tokio::test]
async fn test_webhook_configuration_management() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("webhook_config_test")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    
    // Get repo path before destructuring env 
    let repo_path = env.test_dir().join("test_user").join("test_repo");
    let oxen_dir = repo_path.join(".oxen");
    
    let (_test_dir, server, client) = env.into_parts();

    let namespace = "test_user";
    let repo_name = "test_repo";

    // Get default webhook configuration
    let config_response = client
        .get(&format!("{}/api/repos/{}/{}/webhooks/config", server.base_url(), namespace, repo_name))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()
        .await
        .expect("Failed to get webhook config");

    let status = config_response.status();
    if status != 200 {
        let error_body = config_response.text().await.unwrap_or("No error body".to_string());
        println!("Config endpoint failed with status {}: {}", status, error_body);
        println!("Repo path exists: {}", repo_path.exists());
        println!("Oxen dir exists: {}", oxen_dir.exists());
        panic!("Expected 200 OK for webhook config, got {}", status);
    }
    let config_data: Value = config_response.json().await.expect("Failed to parse config");
    
    // Should default to inline mode
    assert_eq!(config_data["mode"], "Inline");
    assert_eq!(config_data["enabled"], true);

    // Update configuration to queue mode
    let new_config = json!({
        "mode": "Queue",
        "enabled": true,
        "queue_path": "/tmp/test_webhook_events"
    });

    let update_response = client
        .put(&format!("{}/api/repos/{}/{}/webhooks/config", server.base_url(), namespace, repo_name))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .json(&new_config)
        .send()
        .await
        .expect("Failed to update webhook config");

    assert_eq!(update_response.status(), 200);

    // Verify configuration was updated
    let updated_config_response = client
        .get(&format!("{}/api/repos/{}/{}/webhooks/config", server.base_url(), namespace, repo_name))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .send()
        .await
        .expect("Failed to get updated config");

    let updated_config: Value = updated_config_response.json().await.expect("Failed to parse updated config");
    assert_eq!(updated_config["mode"], "Queue");
    assert_eq!(updated_config["queue_path"], "/tmp/test_webhook_events");

    Ok(())
}

#[tokio::test]
async fn test_webhook_rate_limiting() -> Result<(), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("webhook_rate_limit_test")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    let bearer_token = env.create_test_bearer_token()?;
    
    // Get the repo path and current commit ID before destructuring the environment
    let repo_path = env.test_dir().join("test_user").join("test_repo");
    let current_commit_id = get_current_commit_id_from_repo(&repo_path)?;
    
    let (_test_dir, server, client) = env.into_parts();

    let namespace = "test_user";
    let repo_name = "test_repo";

    // Start mock webhook server
    let webhook_receiver = MockWebhookReceiver::new();
    let (webhook_url, webhook_handle) = start_mock_webhook_server(webhook_receiver.clone()).await;

    // Register webhook
    let webhook_request = json!({
        "path": format!("/{}/{}", namespace, repo_name),
        "webhook_url": webhook_url,
        "currentOxenRevision": current_commit_id,
        "purpose": "rate limit test webhook",
        "contact": "admin@company.com"
    });

    client
        .post(&format!("{}/api/repos/{}/{}/webhooks/add", server.base_url(), namespace, repo_name))
        .header("Authorization", format!("Bearer {}", bearer_token))
        .json(&webhook_request)
        .send()
        .await
        .expect("Failed to register webhook");

    // Test rate limiting with direct webhook notifications (bypassing broken upload->commit chain)
    use liboxen::core::webhooks::WebhookNotifier;
    let mut notifier = WebhookNotifier::new_with_rate_limit(1); // 1 second rate limit for testing
    
    println!("Testing rate limiting with 1-second intervals...");
    
    // First notification should succeed - use paths that match the registered webhook
    let notification_count_1 = notifier.notify_path_changed(&repo_path, &format!("/{}/{}/rate_test_1.txt", namespace, repo_name)).await?;
    println!("First notification sent to {} webhooks", notification_count_1);
    
    // Second notification immediately after should be rate limited (skipped)
    let notification_count_2 = notifier.notify_path_changed(&repo_path, &format!("/{}/{}/rate_test_2.txt", namespace, repo_name)).await?;
    println!("Second notification sent to {} webhooks (should be 0 due to rate limiting)", notification_count_2);
    
    // Wait longer than rate limit, then third notification should succeed
    tokio::time::sleep(Duration::from_millis(1100)).await; // Wait > 1 second
    let notification_count_3 = notifier.notify_path_changed(&repo_path, &format!("/{}/{}/rate_test_3.txt", namespace, repo_name)).await?;
    println!("Third notification sent to {} webhooks (should be 1 after rate limit expires)", notification_count_3);

    // Wait for webhook processing
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify rate limiting behavior:
    // - First notification should succeed (1 webhook)
    // - Second notification should be rate limited (0 webhooks) 
    // - Third notification should succeed after waiting (1 webhook)
    let notifications = webhook_receiver.get_notifications().await;
    
    assert_eq!(notification_count_1, 1, "First notification should succeed");
    assert_eq!(notification_count_2, 0, "Second notification should be rate limited");
    assert_eq!(notification_count_3, 1, "Third notification should succeed after rate limit expires");
    
    // Total notifications received should be 2 (first + third)
    assert_eq!(notifications.len(), 2, "Should receive 2 notifications total (rate limiting blocks the middle one)");

    webhook_handle.stop(true).await;
    
    Ok(())
}