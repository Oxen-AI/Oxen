# Integration Tests

This directory contains HTTP-based integration tests for the Oxen server. These tests start real `oxen-server` processes and make actual HTTP requests to test the complete system behavior.

## Test Architecture

### Process Lifecycle
- **Each test starts its own `oxen-server` process** - ensures complete isolation
- **Server lifetime**: Created at test start, automatically killed when test completes (via `Drop` trait)
- **No shared servers** between tests - prevents state contamination

### Process Count
- **Test runner**: 1 main cargo process
- **Per test**: 1 additional `oxen-server` subprocess 
- **Parallel execution**: If running tests in parallel (`--test-threads=4`), up to 4 server processes simultaneously
- **Total**: 1 + N server processes (where N = number of concurrent tests)

## Architecture: What's Real vs Mocked

```text
                    🌐 HTTP Requests (REAL)
                    ┌─────────────────────┐
                    │   reqwest::Client   │
                    │  (Integration Test) │
                    └──────────┬──────────┘
                               │ HTTP/TCP
                    ┌──────────▼──────────┐
                    │   oxen-server       │ ◄── REAL Process
                    │   (Actual Binary)   │
                    └──────────┬──────────┘
                               │ API Calls
                    ┌──────────▼──────────┐
                    │   Server Routes     │ ◄── REAL HTTP Handlers
                    │   (Actix Web)       │
                    └──────────┬──────────┘
                               │ Business Logic
                    ┌──────────▼──────────┐
                    │   Repository APIs   │ ◄── REAL Business Logic
                    │   (liboxen)         │
                    └──────────┬──────────┘
                               │ Storage Interface
                    ┌──────────▼──────────┐
                    │   VersionStore      │ ◄── REAL Interface
                    │   (Trait)           │
                    └───────────┬─────────┘
                                │ Implementation
          ┌─────────────────────┼──────────────────┐
          │                     │                  │
┌─────────▼─────────┐  ┌────────▼───────┐  ┌───────▼───────┐
│ LocalVersionStore │  │ S3VersionStore │  │ (InMemoryStore│ 
│   (Production)    │  │  (Production)  │  │  - DISABLED)  │
│   Filesystem I/O  │  │   AWS S3 API   │  │               │
│     ~50ms         │  │    ~100ms      │  │               │
└───────────────────┘  └────────────────┘  └───────────────┘
         │                     │                   
    ┌────▼────┐         ┌──────▼──────┐       
    │  Disk   │         │   AWS S3    │       
    │ Storage │         │   Buckets   │       
    └─────────┘         └─────────────┘       
```

### What This Achieves For Testing
- **🌐 Real HTTP**: Actual network requests test the full HTTP stack
- **🔧 Real Server**: Complete oxen-server process with all middleware
- **💾 Real Storage**: Filesystem-based storage for realistic testing
- **🎯 Real APIs**: All business logic and API endpoints are exercised
- **🔒 Isolation**: Each test gets fresh filesystem state in unique directories

## Running Tests

### Run All Integration Tests
```bash
cargo test --test integration_tests -- --nocapture
```

### Run Specific Test
```bash
cargo test --test integration_tests oxen_server_health_should_be_accessible_via_http_get -- --nocapture
```

## Test Repository Creation

The integration tests use filesystem-based repositories for realistic testing. All storage operations use the actual liboxen repository implementation.

### Available Repository Creation Functions
- `make_initialized_repo_with_test_files()` - Basic text and CSV files
- `make_initialized_repo_with_test_user()` - CSV-focused repository with test user
- `TestRepositoryBuilder` - Fluent API for custom repository creation

### Custom Test Data with Fluent API
```rust
// Create test repository with custom files
let test_repo = TestRepositoryBuilder::new("namespace", "repo_name")
    .with_file("data.csv", "id,name\n1,Alice\n2,Bob")
    .with_file("config.json", r#"{"version": "1.0"}"#)
    .with_commit_message("Test data setup")
    .build()
    .await?;

// Access the repository directory
let repo_dir = test_repo.repo_dir();
```

## Debugging Tips

- **Use `-- --nocapture`** to see `println!` output
- **Check server logs** in `/tmp` if server fails to start
- **Test endpoints individually** before writing complex scenarios
- **Use `reqwest::Client::builder().timeout()` for slow endpoints
- **Set unique port numbers** to avoid conflicts between parallel tests

## Port Leasing System

The integration tests use a **thread-safe port leasing system** that automatically allocates available ports for parallel test execution. This eliminates port conflicts and makes tests more reliable.

### Using Auto-Port Allocation

**Recommended approach** for new tests:

```rust
#[tokio::test]
async fn test_my_feature() {
    let test_dir = std::env::temp_dir().join("my_test");
    std::fs::create_dir_all(&test_dir).expect("Failed to create test directory");
    
    // Start server with automatic port allocation
    let server = TestServer::start_with_auto_port(&test_dir).await
        .expect("Failed to start test server");
    
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client");
    
    // Use server.base_url() in your requests
    let response = client.get(&format!("{}/api/health", server.base_url()))
        .send()
        .await
        .expect("Failed to send request");
    
    // Test your logic here...
    
    // Cleanup happens automatically when server is dropped
    let _ = std::fs::remove_dir_all(&test_dir);
}
```

### Benefits of Port Leasing

✅ **Thread-safe**: Multiple tests can run in parallel without port conflicts  
✅ **Automatic cleanup**: Ports are freed when test completes  
✅ **No manual allocation**: No need to track port numbers manually  
✅ **Reliable**: Actually binds to port to verify availability  
✅ **Scalable**: Supports as many parallel tests as available ports

### Legacy Manual Port Assignment

Some tests still use manual port assignment via `TestServer::start_with_sync_dir(&test_dir, port)`. These are being gradually converted to auto-port allocation. If you need to add a manual port test, use the port range 3000-4000 and ensure no conflicts with the port allocation table below.

### Port Range

- **Auto-allocated ports**: 3000-4000 (managed by `TestPortAllocator` in `port_leaser.rs`)
- **Manual ports**: Use sparingly and check for conflicts

## CI Considerations

- Tests should be **deterministic** and not rely on external services
- Use **unique temporary directories** to avoid conflicts
- **Clean up resources** properly (servers auto-cleanup via Drop trait)
- Consider **test timeouts** for CI environments

# Rationale - Why Transient Servers Instead of a Shared Server?

This integration test approach spins up **individual oxen-server processes per test** rather than using a single shared server (as described in the main [README.md](../README.md) testing section). Here's why this architecture provides superior developer experience and testing reliability:

## 🚀 **Development Velocity Benefits**

### **1. RustRover/IDE Debugging Paradise**
- **Set breakpoints in BOTH client AND server code simultaneously**
- Step through HTTP request → server processing → response in one debugging session
- No need for two separate IDE instances or processes
- **Zero context switching** between client/server debugging

### **2. Lightning-Fast Iteration Cycles**
```rust
// Change server code → F9 (run single test) → immediately see results
// vs. Change server code → restart shared server → run test suite
```
- **No server restart** required between test runs
- **Instant feedback** on code changes
- Each test is **completely isolated** - no "did my last test break this one?" confusion

### **3. Parallel Development Workflow**
- Multiple developers can run tests simultaneously (different ports)
- No "server is already running" conflicts
- **CI/CD runs reliably** without port conflicts or server state issues

## 🔬 **Superior Debugging Experience**

### **4. Granular Test Isolation**
```rust
// This specific test fails - debug EXACTLY this scenario
cargo test test_csv_file_upload -- --nocapture
// vs. Run entire suite → find failure → restart server → try to reproduce
```

### **5. Deterministic State**
- **Every test starts with clean slate** - no leftover data from previous tests
- **Reproducible failures** - same test always behaves the same way
- **No mysterious "works on my machine"** issues from shared server state

### **6. Failure Investigation Speed**
- **Immediate pinpointing**: Failure in `test_put_file_naming_behavior` → examine ONLY that test
- **Full-stack traces** include both client and server code paths
- **No log noise** from other tests running against shared server

## ⚡ **Performance & Reliability Advantages**

### **7. No Server Management Overhead**
```bash
# Shared server approach (README.md):
./target/debug/oxen-server start  # Terminal 1
cargo test                        # Terminal 2 
# Kill server, restart, repeat...

# Transient approach:
cargo test  # Done. Everything handled automatically.
```

### **8. Test Suite Reliability**
- **No flaky tests** due to shared server state
- **No race conditions** between tests modifying same server
- **No "server died mid-test"** failures

### **9. Resource Efficiency**
- Servers **auto-cleanup** when tests complete
- **No memory leaks** from long-running shared server
- **Garbage collection** of test data happens automatically

## 🛠️ **Developer Experience Wins**

### **10. New Contributor Onboarding**
```bash
git clone repo
cargo build
cargo test  # Everything just works!
```
- **No complex setup** or "start server first" instructions
- **No forgotten cleanup** when switching branches
- **Immediate success** for new contributors

### **11. Feature Development Flow**
```rust
// Working on new API endpoint:
1. Write test with expected behavior
2. Set breakpoint in server code
3. F9 → step through implementation
4. Fix → F9 → verify fix
5. Repeat until perfect
```

### **12. Bug Investigation Workflow**
```rust
// Bug report: "CSV upload fails with large files"
1. Write failing test reproducing exact scenario
2. Set breakpoints across request pipeline
3. Run single test → see exactly where/why it breaks
4. Fix & verify in same session
```

## 🏗️ **Architecture & Testing Benefits**

### **13. True Integration Coverage**
- Tests **actual server startup/shutdown** logic
- Validates **port binding, health checks, graceful shutdown**
- Exercises **real error handling** during server initialization

### **14. Environment Parity**
- **Same binary** used in production
- **Same configuration** patterns
- **Same resource constraints** (memory, file handles, etc.)

### **15. Configuration Testing**
- Each test can use **different server configurations**
- Test **edge cases** like low memory, restricted permissions
- Validate **environment variable handling**

## 📊 **Comparison: Shared vs Transient Server**

| Aspect                    | Shared Server (README)                    | Transient Server (Our Approach)   |
|---------------------------|-------------------------------------------|-----------------------------------|
| **Setup Time**            | Manual server start                       | ✅ Automatic                       |
| **Debugging**             | Two processes                             | ✅ Single debug session            |
| **Test Isolation**        | ❌ Shared state                            | ✅ Complete isolation              |
| **Parallel Testing**      | ❌ Port conflicts, shared state issues     | ✅ Thread-safe port leasing system |
| **CI/CD Reliability**     | ❌ Server management                       | ✅ Self-contained                  |
| **Developer Onboarding**  | Multi-step setup                          | ✅ `cargo test`                    |
| **Failure Investigation** | Log mining                                | ✅ Pinpoint debugging              |
| **Resource Cleanup**      | Manual                                    | ✅ Automatic                       |

## 🎯 **The Bottom Line**

**Shared Server Approach (main README.md):**
- Good for: Manual testing, long-running server validation
- Pain points: Context switching, state pollution, setup complexity

**Transient Server Approach (This Implementation):**
- **Optimized for developer productivity**
- **RustRover debugging workflow** becomes seamless
- **Faster iteration** = faster feature development = happier developers

## 💡 **Strategic Value**

For a **contributor-driven project**, removing friction from the development workflow is crucial. The transient server approach transforms testing from:

❌ **"Ugh, I need to restart the server again"**  
✅ **"Let me just F9 and step through this bug"**

This **reduces the barrier to contribution** and makes the codebase more accessible to new developers, which directly benefits the project's growth and community engagement.

The **real filesystem storage** helps catch real-world edge cases and ensures tests behave like production - making the tests more valuable and realistic.

## 🔄 **When to Use Each Approach**

**Use Transient Servers (this approach) for:**
- Feature development and debugging
- Regression testing
- CI/CD pipelines
- New contributor onboarding
- API contract validation

**Use Shared Server (README.md approach) for:**
- Manual API exploration
- Load testing
- Long-running integration scenarios
- Production-like environment validation