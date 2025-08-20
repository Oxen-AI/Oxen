use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use actix_web::{web, App, HttpServer, HttpResponse, Result as ActixResult};
use serde_json::{json, Value};
use tempfile::TempDir;

use crate::common::TestRepositoryBuilder;

/// Mock webhook receiver that captures all webhook notifications with detailed tracking
#[derive(Debug, Clone, Default)]
struct MockWebhookReceiver {
    notifications: Arc<Mutex<Vec<Value>>>,
    response_status: Arc<Mutex<u16>>,
}

impl MockWebhookReceiver {
    fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    async fn set_response_status(&self, status: u16) {
        *self.response_status.lock().await = status;
    }

    async fn get_notifications(&self) -> Vec<Value> {
        self.notifications.lock().await.clone()
    }

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
    // Retry port binding in case of conflicts
    for attempt in 0..5 {
        // Find available port
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let webhook_url = format!("http://127.0.0.1:{}/webhook", port);
        
        let server_result = HttpServer::new({
            let receiver = receiver.clone();
            move || {
                App::new()
                    .app_data(web::Data::new(receiver.clone()))
                    .route("/webhook", web::post().to(webhook_handler))
            }
        })
        .bind(("127.0.0.1", port));

        match server_result {
            Ok(server) => {
                let server = server.run();
                let handle = server.handle();
                tokio::spawn(server);
                
                // Wait for server to start
                tokio::time::sleep(Duration::from_millis(150)).await;
                
                return (webhook_url, handle);
            }
            Err(_) if attempt < 4 => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(e) => panic!("Failed to bind webhook server after 5 attempts: {}", e),
        }
    }
    unreachable!()
}

/// Test that parent directory webhooks are triggered by child file changes
#[tokio::test]
async fn test_hierarchical_parent_directory_matching() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for our test repository
    let temp_dir = TempDir::new()?;
    
    // Build a test repository with nested directory structure
    let test_repo = TestRepositoryBuilder::new("test_user", "hierarchical_test")
        .with_base_dir(temp_dir.path())
        .with_file("a/b/c/d.txt", "test content")
        .with_commit_message("Initial commit with nested structure")
        .build()
        .await?;
    
    let repo = test_repo.repo;
    
    // Start mock webhook servers for different hierarchy levels
    let webhook_receiver_root = MockWebhookReceiver::new();
    let webhook_receiver_a = MockWebhookReceiver::new();
    let webhook_receiver_ab = MockWebhookReceiver::new();
    let webhook_receiver_abc = MockWebhookReceiver::new();
    
    let (webhook_url_root, _handle_root) = start_mock_webhook_server(webhook_receiver_root.clone()).await;
    let (webhook_url_a, _handle_a) = start_mock_webhook_server(webhook_receiver_a.clone()).await;
    let (webhook_url_ab, _handle_ab) = start_mock_webhook_server(webhook_receiver_ab.clone()).await;
    let (webhook_url_abc, _handle_abc) = start_mock_webhook_server(webhook_receiver_abc.clone()).await;
    
    // Create webhook database and register webhooks at different hierarchy levels
    use liboxen::core::db::webhooks::WebhookDB;
    use liboxen::model::WebhookAddRequest;
    
    let webhook_db_path = repo.path.join(".oxen").join("webhooks");
    std::fs::create_dir_all(&webhook_db_path)?;
    let webhook_db = WebhookDB::new(&webhook_db_path)?;
    
    // Register webhooks for different path levels
    let webhooks = [
        ("/", webhook_url_root.clone(), "root-webhook"),
        ("/a", webhook_url_a.clone(), "a-webhook"), 
        ("/a/b", webhook_url_ab.clone(), "ab-webhook"),
        ("/a/b/c", webhook_url_abc.clone(), "abc-webhook"),
    ];
    
    for (path, url, purpose) in &webhooks {
        let webhook_request = WebhookAddRequest {
            path: path.to_string(),
            webhook_url: url.clone(),
            current_oxen_revision: Some("old-revision".to_string()),
            current_oxen_revision_camel: None,
            purpose: purpose.to_string(),
            contact: "admin@company.com".to_string(),
        };
        webhook_db.add_webhook(webhook_request)?;
    }
    
    // Test hierarchical matching directly using the webhook database
    let webhooks_for_change = webhook_db.list_webhooks_for_path("/a/b/c/d.txt")?;
    let notification_count = webhooks_for_change.len();
    
    // Send mock notifications to verify the webhook endpoints
    use reqwest::Client;
    let client = Client::new();
    for webhook in &webhooks_for_change {
        let notification = serde_json::json!({
            "path": "/a/b/c/d.txt"
        });
        let _response = client.post(&webhook.webhook_url)
            .json(&notification)
            .send()
            .await?;
    }
    
    // All 4 webhooks should be triggered (root, a, a/b, a/b/c)
    assert_eq!(notification_count, 4, "All parent webhooks should be triggered by child file change");
    
    // Wait for async HTTP requests to complete
    tokio::time::sleep(Duration::from_millis(300)).await;
    
    // Verify each webhook received the notification
    let notifications_root = webhook_receiver_root.get_notifications().await;
    let notifications_a = webhook_receiver_a.get_notifications().await;
    let notifications_ab = webhook_receiver_ab.get_notifications().await;
    let notifications_abc = webhook_receiver_abc.get_notifications().await;
    
    assert_eq!(notifications_root.len(), 1, "Root webhook should receive notification");
    assert_eq!(notifications_a.len(), 1, "'/a' webhook should receive notification");
    assert_eq!(notifications_ab.len(), 1, "'/a/b' webhook should receive notification");
    assert_eq!(notifications_abc.len(), 1, "'/a/b/c' webhook should receive notification");
    
    // Verify all notifications contain the correct changed path
    for notifications in [&notifications_root, &notifications_a, &notifications_ab, &notifications_abc] {
        assert_eq!(notifications[0]["path"], "/a/b/c/d.txt");
    }
    
    println!("✅ Hierarchical parent directory matching test passed!");
    Ok(())
}

/// Test that sibling directory webhooks are NOT triggered by unrelated changes
#[tokio::test]
async fn test_hierarchical_sibling_isolation() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for our test repository
    let temp_dir = TempDir::new()?;
    
    // Build a test repository with sibling directory structure
    let test_repo = TestRepositoryBuilder::new("test_user", "sibling_test")
        .with_base_dir(temp_dir.path())
        .with_file("data/file.txt", "data content")
        .with_file("logs/file.txt", "log content")
        .with_file("config/file.txt", "config content")
        .build()
        .await?;
    
    let repo = test_repo.repo;
    
    // Start mock webhook servers for different sibling directories
    let webhook_receiver_data = MockWebhookReceiver::new();
    let webhook_receiver_logs = MockWebhookReceiver::new();
    let webhook_receiver_config = MockWebhookReceiver::new();
    
    let (webhook_url_data, _handle_data) = start_mock_webhook_server(webhook_receiver_data.clone()).await;
    let (webhook_url_logs, _handle_logs) = start_mock_webhook_server(webhook_receiver_logs.clone()).await;
    let (webhook_url_config, _handle_config) = start_mock_webhook_server(webhook_receiver_config.clone()).await;
    
    // Create webhook database and register webhooks for sibling directories
    use liboxen::core::db::webhooks::WebhookDB;
    use liboxen::model::WebhookAddRequest;
    
    let webhook_db_path = repo.path.join(".oxen").join("webhooks");
    std::fs::create_dir_all(&webhook_db_path)?;
    let webhook_db = WebhookDB::new(&webhook_db_path)?;
    
    // Register webhooks for sibling directories
    let webhooks = [
        ("/data", webhook_url_data.clone(), "data-webhook"),
        ("/logs", webhook_url_logs.clone(), "logs-webhook"),
        ("/config", webhook_url_config.clone(), "config-webhook"),
    ];
    
    for (path, url, purpose) in &webhooks {
        let webhook_request = WebhookAddRequest {
            path: path.to_string(),
            webhook_url: url.clone(),
            current_oxen_revision: Some("old-revision".to_string()),
            current_oxen_revision_camel: None,
            purpose: purpose.to_string(),
            contact: "admin@company.com".to_string(),
        };
        webhook_db.add_webhook(webhook_request)?;
    }
    
    // Test webhook notification for a change in the data directory
    let webhooks_for_change = webhook_db.list_webhooks_for_path("/data/new_file.txt")?;
    let notification_count = webhooks_for_change.len();
    
    // Send mock notifications 
    use reqwest::Client;
    let client = Client::new();
    for webhook in &webhooks_for_change {
        let notification = serde_json::json!({
            "path": "/data/new_file.txt"
        });
        let _response = client.post(&webhook.webhook_url)
            .json(&notification)
            .send()
            .await?;
    }
    
    // Only 1 webhook should be triggered (data directory)
    assert_eq!(notification_count, 1, "Only the matching directory webhook should be triggered");
    
    // Wait for async HTTP requests to complete
    tokio::time::sleep(Duration::from_millis(300)).await;
    
    // Verify only the data webhook received the notification
    let notifications_data = webhook_receiver_data.get_notifications().await;
    let notifications_logs = webhook_receiver_logs.get_notifications().await;
    let notifications_config = webhook_receiver_config.get_notifications().await;
    
    assert_eq!(notifications_data.len(), 1, "Data webhook should receive notification");
    assert_eq!(notifications_logs.len(), 0, "Logs webhook should NOT receive notification for data change");
    assert_eq!(notifications_config.len(), 0, "Config webhook should NOT receive notification for data change");
    
    // Verify the notification contains the correct changed path
    assert_eq!(notifications_data[0]["path"], "/data/new_file.txt");
    
    println!("✅ Hierarchical sibling isolation test passed!");
    Ok(())
}

/// Test path normalization edge cases
#[tokio::test]
async fn test_path_normalization() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for our test repository
    let temp_dir = TempDir::new()?;
    
    let test_repo = TestRepositoryBuilder::new("test_user", "normalization_test")
        .with_base_dir(temp_dir.path())
        .with_file("test.txt", "test content")
        .build()
        .await?;
    
    let repo = test_repo.repo;
    
    // Start mock webhook server
    let webhook_receiver = MockWebhookReceiver::new();
    let (webhook_url, _handle) = start_mock_webhook_server(webhook_receiver.clone()).await;
    
    // Create webhook database
    use liboxen::core::db::webhooks::WebhookDB;
    use liboxen::model::WebhookAddRequest;
    
    let webhook_db_path = repo.path.join(".oxen").join("webhooks");
    std::fs::create_dir_all(&webhook_db_path)?;
    let webhook_db = WebhookDB::new(&webhook_db_path)?;
    
    // Test various path formats that should be normalized
    let test_cases = [
        ("data/", "/data/subdir/file.txt", true, "Trailing slash should be handled"),
        ("data", "/data/subdir/file.txt", true, "No leading slash should be handled"),
        ("/data/", "/data/subdir/file.txt", true, "Both slashes should be handled"),
        ("/data", "/data/subdir/file.txt", true, "Normalized format should work"),
        ("other", "/data/subdir/file.txt", false, "Non-matching paths should not trigger"),
    ];
    
    for (i, (webhook_path, changed_path, should_match, description)) in test_cases.iter().enumerate() {
        // Clear previous notifications
        webhook_receiver.clear_notifications().await;
        
        // Create webhook with the test path format
        let webhook_request = WebhookAddRequest {
            path: webhook_path.to_string(),
            webhook_url: webhook_url.clone(),
            current_oxen_revision: Some("old-revision".to_string()),
            current_oxen_revision_camel: None,
            purpose: format!("normalization-test-{}", i),
            contact: "admin@company.com".to_string(),
        };
        
        // Remove any existing webhooks first
        let existing_webhooks = webhook_db.list_all_webhooks()?;
        for webhook in existing_webhooks {
            webhook_db.remove_webhook(&webhook.id)?;
        }
        
        // Add the test webhook
        webhook_db.add_webhook(webhook_request)?;
        
        // Test notification directly
        let webhooks_for_change = webhook_db.list_webhooks_for_path(changed_path)?;
        let notification_count = webhooks_for_change.len();
        
        // Send mock notifications
        use reqwest::Client;
        let client = Client::new();
        for webhook in &webhooks_for_change {
            let notification = serde_json::json!({
                "path": changed_path
            });
            let _response = client.post(&webhook.webhook_url)
                .json(&notification)
                .send()
                .await?;
        }
        
        if *should_match {
            assert_eq!(notification_count, 1, "Test case failed: {}", description);
        } else {
            assert_eq!(notification_count, 0, "Test case failed: {}", description);
        }
        
        // Wait for async processing
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let notifications = webhook_receiver.get_notifications().await;
        if *should_match {
            assert_eq!(notifications.len(), 1, "Should receive notification: {}", description);
            assert_eq!(notifications[0]["path"], *changed_path);
        } else {
            assert_eq!(notifications.len(), 0, "Should NOT receive notification: {}", description);
        }
    }
    
    println!("✅ Path normalization test passed!");
    Ok(())
}

/// Test root path webhook catches all changes
#[tokio::test]
async fn test_root_path_webhook() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for our test repository
    let temp_dir = TempDir::new()?;
    
    let test_repo = TestRepositoryBuilder::new("test_user", "root_test")
        .with_base_dir(temp_dir.path())
        .with_file("test.txt", "test content")
        .build()
        .await?;
    
    let repo = test_repo.repo;
    
    // Start mock webhook server
    let webhook_receiver = MockWebhookReceiver::new();
    let (webhook_url, _handle) = start_mock_webhook_server(webhook_receiver.clone()).await;
    
    // Create webhook database
    use liboxen::core::db::webhooks::WebhookDB;
    use liboxen::model::WebhookAddRequest;
    
    let webhook_db_path = repo.path.join(".oxen").join("webhooks");
    std::fs::create_dir_all(&webhook_db_path)?;
    let webhook_db = WebhookDB::new(&webhook_db_path)?;
    
    // Register webhook for root path
    let webhook_request = WebhookAddRequest {
        path: "/".to_string(),
        webhook_url: webhook_url.clone(),
        current_oxen_revision: Some("old-revision".to_string()),
        current_oxen_revision_camel: None,
        purpose: "root-webhook".to_string(),
        contact: "admin@company.com".to_string(),
    };
    webhook_db.add_webhook(webhook_request)?;
    
    // Test notification for various paths - all should trigger root webhook
    let test_paths = [
        "/file.txt",
        "/data/file.txt", 
        "/deep/nested/path/file.txt",
        "/a/b/c/d/e/f/file.txt",
    ];
    
    use reqwest::Client;
    let client = Client::new();
    
    for (_i, test_path) in test_paths.iter().enumerate() {
        // Clear previous notifications
        webhook_receiver.clear_notifications().await;
        
        let webhooks_for_change = webhook_db.list_webhooks_for_path(test_path)?;
        let notification_count = webhooks_for_change.len();
        
        // Send mock notifications
        for webhook in &webhooks_for_change {
            let notification = serde_json::json!({
                "path": test_path
            });
            let _response = client.post(&webhook.webhook_url)
                .json(&notification)
                .send()
                .await?;
        }
        
        assert_eq!(notification_count, 1, "Root webhook should catch path: {}", test_path);
        
        // Wait for async processing
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let notifications = webhook_receiver.get_notifications().await;
        assert_eq!(notifications.len(), 1, "Should receive notification for path: {}", test_path);
        assert_eq!(notifications[0]["path"], *test_path);
    }
    
    println!("✅ Root path webhook test passed!");
    Ok(())
}