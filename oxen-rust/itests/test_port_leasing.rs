use crate::common::{TestPortAllocator, TestServer};

/// Demonstration test showing automatic port management
#[tokio::test]
async fn test_port_management_demo() {
    println!("🚦 Testing automatic port management...");

    // Create a fresh allocator for this test to avoid interference with other tests
    let allocator = TestPortAllocator::new();

    // Lease multiple ports
    let lease1 = allocator.lease_port().expect("Should lease first port");
    let lease2 = allocator.lease_port().expect("Should lease second port");
    let lease3 = allocator.lease_port().expect("Should lease third port");

    println!(
        "📋 Leased ports: {}, {}, {}",
        lease1.port(),
        lease2.port(),
        lease3.port()
    );

    // All ports should be different
    assert_ne!(lease1.port(), lease2.port());
    assert_ne!(lease2.port(), lease3.port());
    assert_ne!(lease1.port(), lease3.port());

    // Check allocated count
    let allocated = allocator.allocated_ports();
    assert_eq!(allocated.len(), 3);
    println!("✅ {} ports currently allocated", allocated.len());

    // Drop one lease
    drop(lease2);

    // Should have one less allocated
    let allocated_after_drop = allocator.allocated_ports();
    assert_eq!(allocated_after_drop.len(), 2);
    println!(
        "📤 Port returned, {} ports still allocated",
        allocated_after_drop.len()
    );

    // Should be able to lease another port (could reuse the dropped one)
    let lease4 = allocator.lease_port().expect("Should lease fourth port");
    println!("🔄 Leased new port: {}", lease4.port());

    println!("🎉 Port management working correctly!");
}

/// Test automatic port management with TestServer
#[tokio::test]
async fn test_server_with_auto_port() {
    println!("🏗️ Testing TestServer with automatic port management...");

    let test_dir = std::env::temp_dir().join("port_test");
    let _ = std::fs::remove_dir_all(&test_dir);
    std::fs::create_dir_all(&test_dir).expect("Failed to create test directory");

    // Start server with auto-managed port - no conflicts possible!
    let server = TestServer::start_with_auto_port(&test_dir)
        .await
        .expect("Failed to start test server with auto port");

    println!("🌐 Server started at: {}", server.base_url());

    // Make a simple HTTP request to verify it's working
    let client = reqwest::Client::new();
    let response = client
        .get(&format!("{}/api/health", server.base_url()))
        .send()
        .await
        .expect("Failed to send health request");

    println!("🏥 Health check status: {}", response.status());
    assert!(response.status().is_success());

    // Clean up
    let _ = std::fs::remove_dir_all(&test_dir);

    println!("✅ Auto-port server test completed!");
}

/// Test multiple concurrent servers with auto ports
#[tokio::test]
async fn test_multiple_concurrent_servers() {
    use tokio::task;

    println!("🏭 Testing multiple concurrent servers with auto ports...");

    let base_dir = std::env::temp_dir().join("multi_server_test");
    let _ = std::fs::remove_dir_all(&base_dir);

    // Start multiple servers concurrently
    let mut handles = Vec::new();

    for i in 0..3 {
        let test_dir = base_dir.join(format!("server_{}", i));
        std::fs::create_dir_all(&test_dir).expect("Failed to create test directory");

        let handle = task::spawn(async move {
            println!("🚀 Starting server {}...", i);

            let server = TestServer::start_with_auto_port(&test_dir)
                .await
                .expect(&format!("Failed to start server {}", i));

            println!("✅ Server {} started at {}", i, server.base_url());

            // Make a health check
            let client = reqwest::Client::new();
            let response = client
                .get(&format!("{}/api/health", server.base_url()))
                .send()
                .await
                .expect(&format!("Failed health check for server {}", i));

            assert!(response.status().is_success());

            (i, server.base_url().to_string())
        });

        handles.push(handle);
    }

    // Wait for all servers to start and verify they're all on different ports
    let mut results = Vec::new();
    for handle in handles {
        let (server_id, base_url) = handle.await.expect("Server task should complete");
        results.push((server_id, base_url));
    }

    // Extract ports and verify they're all different
    let ports: Vec<u16> = results
        .iter()
        .map(|(_, url): &(i32, String)| url.split(':').last().unwrap().parse().unwrap())
        .collect();

    println!("🔍 Server ports: {:?}", ports);

    // All ports should be unique
    for i in 0..ports.len() {
        for j in (i + 1)..ports.len() {
            assert_ne!(ports[i], ports[j], "Ports should be unique");
        }
    }

    // Clean up
    let _ = std::fs::remove_dir_all(&base_dir);

    println!(
        "🎉 All {} servers started successfully on unique ports!",
        results.len()
    );
}
