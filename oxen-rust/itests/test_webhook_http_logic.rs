use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use actix_web::{web, App, HttpServer, HttpResponse, Result as ActixResult};
use serde_json::{json, Value};

/// Mock webhook receiver that captures all webhook notifications
#[derive(Debug, Clone, Default)]
struct MockWebhookReceiver {
    notifications: Arc<Mutex<Vec<Value>>>,
    response_status: Arc<Mutex<u16>>,
    request_count: Arc<Mutex<u32>>,
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

    async fn get_request_count(&self) -> u32 {
        *self.request_count.lock().await
    }
}

async fn webhook_handler(
    payload: web::Json<Value>,
    data: web::Data<MockWebhookReceiver>,
) -> ActixResult<HttpResponse> {
    // Increment request counter
    *data.request_count.lock().await += 1;
    
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
    .bind(("127.0.0.1", port))
    .unwrap()
    .run();

    let handle = server.handle();
    tokio::spawn(server);
    
    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    (webhook_url, handle)
}

/// Test the HTTP webhook notification logic in complete isolation
#[tokio::test] 
async fn test_webhook_http_success() -> Result<(), Box<dyn std::error::Error>> {
    // Start mock webhook server
    let webhook_receiver = MockWebhookReceiver::new();
    let (webhook_url, _webhook_handle) = start_mock_webhook_server(webhook_receiver.clone()).await;
    
    // Test webhook HTTP call directly using reqwest (same as WebhookNotifier uses)
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    
    let notification_payload = json!({
        "path": "/test/path",
        "timestamp": "2023-01-01T00:00:00Z",
        "event_type": "commit"
    });
    
    // Send webhook notification
    let response = client
        .post(&webhook_url)
        .json(&notification_payload)
        .send()
        .await?;
    
    // Verify successful response
    assert!(response.status().is_success(), "Webhook should return success status");
    
    let response_body: Value = response.json().await?;
    assert_eq!(response_body["status"], "received");
    
    // Wait for async processing
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Verify webhook received the notification
    let notifications = webhook_receiver.get_notifications().await;
    assert_eq!(notifications.len(), 1, "Should have received exactly 1 notification");
    
    let received_notification = &notifications[0];
    assert_eq!(received_notification["path"], "/test/path");
    assert_eq!(received_notification["event_type"], "commit");
    
    println!("✅ Webhook HTTP success test passed!");
    Ok(())
}

/// Test webhook HTTP failure handling
#[tokio::test]
async fn test_webhook_http_failure() -> Result<(), Box<dyn std::error::Error>> {
    // Start mock webhook server that returns 500 errors
    let webhook_receiver = MockWebhookReceiver::new();
    webhook_receiver.set_response_status(500).await;
    let (webhook_url, _webhook_handle) = start_mock_webhook_server(webhook_receiver.clone()).await;
    
    // Test webhook HTTP call
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    
    let notification_payload = json!({
        "path": "/test/failure",
        "event_type": "commit"
    });
    
    // Send webhook notification
    let response = client
        .post(&webhook_url)
        .json(&notification_payload)
        .send()
        .await?;
    
    // Verify failure response
    assert_eq!(response.status(), 500, "Webhook should return 500 error");
    
    let response_body: Value = response.json().await?;
    assert_eq!(response_body["error"], "internal error");
    
    // Wait for async processing
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Verify webhook still received the request attempt
    let notifications = webhook_receiver.get_notifications().await;
    assert_eq!(notifications.len(), 1, "Should have received 1 notification attempt");
    
    let request_count = webhook_receiver.get_request_count().await;
    assert_eq!(request_count, 1, "Should have received exactly 1 HTTP request");
    
    println!("✅ Webhook HTTP failure test passed!");
    Ok(())
}

/// Test webhook timeout handling
#[tokio::test]
async fn test_webhook_timeout() -> Result<(), Box<dyn std::error::Error>> {
    // Create a webhook server that delays responses
    let webhook_receiver = MockWebhookReceiver::new();
    let delayed_receiver = webhook_receiver.clone();
    
    // Start server on available port
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    
    let webhook_url = format!("http://127.0.0.1:{}/slow-webhook", port);
    
    let server = HttpServer::new(move || {
        let receiver = delayed_receiver.clone();
        App::new()
            .app_data(web::Data::new(receiver))
            .route("/slow-webhook", web::post().to(|payload: web::Json<Value>, data: web::Data<MockWebhookReceiver>| async move {
                // Store the notification
                data.notifications.lock().await.push(payload.into_inner());
                
                // Simulate slow processing (longer than client timeout)
                tokio::time::sleep(Duration::from_millis(2000)).await;
                
                Ok::<_, actix_web::Error>(HttpResponse::Ok().json(json!({"status": "slow_received"})))
            }))
    })
    .bind(("127.0.0.1", port))
    .unwrap()
    .run();

    let _handle = server.handle();
    tokio::spawn(server);
    
    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Test webhook HTTP call with short timeout
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(500))  // Short timeout
        .build()?;
    
    let notification_payload = json!({
        "path": "/test/timeout",
        "event_type": "commit"
    });
    
    // Send webhook notification - should timeout
    let result = client
        .post(&webhook_url)
        .json(&notification_payload)
        .send()
        .await;
    
    // Verify timeout error
    assert!(result.is_err(), "Webhook request should timeout and fail");
    
    let error = result.unwrap_err();
    assert!(error.is_timeout(), "Error should be a timeout error");
    
    println!("✅ Webhook timeout test passed!");
    Ok(())
}

/// Test concurrent webhook notifications
#[tokio::test]
async fn test_concurrent_webhook_notifications() -> Result<(), Box<dyn std::error::Error>> {
    // Start mock webhook server
    let webhook_receiver = MockWebhookReceiver::new();
    let (webhook_url, _webhook_handle) = start_mock_webhook_server(webhook_receiver.clone()).await;
    
    // Create HTTP client
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    
    // Send multiple concurrent webhook notifications
    let mut handles = vec![];
    
    for i in 0..5 {
        let client_clone = client.clone();
        let url_clone = webhook_url.clone();
        
        let handle = tokio::spawn(async move {
            let notification_payload = json!({
                "path": format!("/test/concurrent/{}", i),
                "event_type": "commit",
                "sequence": i
            });
            
            client_clone
                .post(&url_clone)
                .json(&notification_payload)
                .send()
                .await
        });
        
        handles.push(handle);
    }
    
    // Wait for all requests to complete
    let results = futures::future::join_all(handles).await;
    
    // Verify all requests succeeded
    let mut success_count = 0;
    for result in results {
        match result {
            Ok(Ok(response)) => {
                if response.status().is_success() {
                    success_count += 1;
                }
            }
            _ => {}
        }
    }
    
    assert_eq!(success_count, 5, "All 5 concurrent webhook requests should succeed");
    
    // Wait for async processing
    tokio::time::sleep(Duration::from_millis(300)).await;
    
    // Verify webhook received all notifications
    let notifications = webhook_receiver.get_notifications().await;
    assert_eq!(notifications.len(), 5, "Should have received all 5 notifications");
    
    let request_count = webhook_receiver.get_request_count().await;
    assert_eq!(request_count, 5, "Should have received exactly 5 HTTP requests");
    
    // Verify all sequence numbers are present
    let mut sequences: Vec<i32> = notifications
        .iter()
        .map(|n| n["sequence"].as_i64().unwrap() as i32)
        .collect();
    sequences.sort();
    
    assert_eq!(sequences, vec![0, 1, 2, 3, 4], "All sequence numbers should be present");
    
    println!("✅ Concurrent webhook notifications test passed!");
    Ok(())
}