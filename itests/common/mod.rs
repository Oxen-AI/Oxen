#[allow(dead_code)]
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;
// Import from the server crate - we'll need to add this as a dependency
// use oxen_server;

pub mod in_memory_storage;
pub use in_memory_storage::InMemoryVersionStore;

pub mod test_repository_builder;
pub use test_repository_builder::TestRepositoryBuilder;

pub mod port_leaser;
pub use port_leaser::{PortLease, TestPortAllocator};

pub struct TestServer {
    child: Child,
    base_url: String,
    _port_lease: Option<PortLease>, // Keep lease alive for server lifetime
}

impl TestServer {
    /// Create an access key manager for the test server's sync directory
    /// Note: This would need the oxen_server crate to be available as a dependency
    /// For now, we'll comment this out until we can add the proper dependency
    // pub fn create_access_key_manager(sync_dir: &std::path::Path) -> Result<oxen_server::auth::access_keys::AccessKeyManager, liboxen::error::OxenError> {
    //     oxen_server::auth::access_keys::AccessKeyManager::new(sync_dir)
    // }

    /// Start a real oxen-server process with custom sync directory
    async fn start_server_impl(
        sync_dir: &std::path::Path,
        port: u16,
        port_lease: Option<PortLease>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Create the sync directory
        std::fs::create_dir_all(&sync_dir)?;

        // Find the oxen-server binary
        let server_path = std::env::current_dir()?
            .join("target")
            .join("debug")
            .join("oxen-server");

        if !server_path.exists() {
            return Err("oxen-server binary not found. Run 'cargo build' first".into());
        }

        // Start the server process
        let mut child = Command::new(server_path)
            .arg("start")
            .arg("--ip")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(&port.to_string())
            .env("SYNC_DIR", &sync_dir)
            .stdout(Stdio::null()) // Suppress output to avoid hanging
            .stderr(Stdio::null())
            .spawn()?;

        // Check if process is still running
        match child.try_wait() {
            Ok(Some(status)) => {
                return Err(format!("Server process exited early with status: {}", status).into());
            }
            Ok(None) => {
                // Process is still running, good
            }
            Err(e) => {
                return Err(format!("Error checking server process: {}", e).into());
            }
        }

        // Try to connect to health endpoint to verify server is ready
        let client = reqwest::Client::new();
        let base_url = format!("http://127.0.0.1:{}", port);
        let start_time = std::time::Instant::now();

        for i in 0..1000 {
            if let Ok(response) = client.get(&format!("{}/api/health", base_url)).send().await {
                if response.status().is_success() {
                    let elapsed = start_time.elapsed();
                    let port_info = match &port_lease {
                        Some(_) => format!(" on auto-port {}", port),
                        None => String::new(),
                    };
                    println!(
                        "Server started in {:?} (attempt {}){}",
                        elapsed,
                        i + 1,
                        port_info
                    );
                    return Ok(TestServer {
                        child,
                        base_url,
                        _port_lease: port_lease,
                    });
                }
            }
            sleep(Duration::from_millis(5)).await;
        }

        // If we get here, server didn't start properly
        let _ = child.kill();
        Err("Server failed to start or health check failed".into())
    }

    pub async fn start_with_sync_dir(
        sync_dir: &std::path::Path,
        port: u16,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::start_server_impl(sync_dir, port, None).await
    }

    /// Start a real oxen-server process with automatic port allocation
    /// This method is thread-safe and prevents port conflicts in parallel tests
    pub async fn start_with_auto_port(
        sync_dir: &std::path::Path,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Lease a port from the global allocator
        let port_lease = TestPortAllocator::instance()
            .lease_port()
            .map_err(|e| format!("Failed to lease port: {}", e))?;

        let port = port_lease.port();

        Self::start_server_impl(sync_dir, port, Some(port_lease)).await
    }

    /// Get the base URL for this test server
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Create a dummy TestServer instance for safe mem::replace operations
    /// This should never be used for actual server operations
    fn dummy() -> Self {
        use std::process::Stdio;

        // Create a dummy child process that immediately exits
        let mut child = std::process::Command::new("true")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("Failed to create dummy child process");

        // Wait for it to exit immediately
        let _ = child.wait();

        Self {
            child,
            base_url: String::new(),
            _port_lease: None,
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Clean up the server process
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Builder for creating test repositories with various configurations
#[derive(Default)]
pub struct TestRepoBuilder {
    base_dir: Option<std::path::PathBuf>,
    repo_name: Option<String>,
    user_name: Option<String>,
    user_email: Option<String>,
    commit_message: Option<String>,
    files: Vec<(String, String)>, // (filename, content)
    use_in_memory_storage: bool,
    namespace: Option<String>,
}

impl TestRepoBuilder {
    /// Create a new TestRepoBuilder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the base directory for the repository
    pub fn base_dir<P: AsRef<std::path::Path>>(mut self, dir: P) -> Self {
        self.base_dir = Some(dir.as_ref().to_path_buf());
        self
    }

    /// Set the repository name (defaults to "test_repo")
    pub fn repo_name<S: Into<String>>(mut self, name: S) -> Self {
        self.repo_name = Some(name.into());
        self
    }

    /// Set the user name for commits (defaults to "Test User")
    pub fn user_name<S: Into<String>>(mut self, name: S) -> Self {
        self.user_name = Some(name.into());
        self
    }

    /// Set the user email for commits (defaults to "test@example.com")
    pub fn user_email<S: Into<String>>(mut self, email: S) -> Self {
        self.user_email = Some(email.into());
        self
    }

    /// Set the commit message (defaults to "Initial commit")
    pub fn commit_message<S: Into<String>>(mut self, message: S) -> Self {
        self.commit_message = Some(message.into());
        self
    }

    /// Add a file to be created in the repository
    pub fn add_file<S: Into<String>>(mut self, filename: S, content: S) -> Self {
        self.files.push((filename.into(), content.into()));
        self
    }

    /// Add a CSV file with sample data
    #[allow(dead_code)]
    pub fn add_csv_file<S: Into<String>>(mut self, filename: S) -> Self {
        let csv_content = "product,price,category\nLaptop,999.99,Electronics\nChair,149.50,Furniture\nBook,19.99,Education";
        self.files.push((filename.into(), csv_content.to_string()));
        self
    }

    /// Add a text file with sample data
    #[allow(dead_code)]
    pub fn add_text_file<S: Into<String>>(mut self, filename: S) -> Self {
        let text_content = "Hello from Oxen integration test!\nThis is real file content.";
        self.files.push((filename.into(), text_content.to_string()));
        self
    }

    /// Add a sample data CSV file
    #[allow(dead_code)]
    pub fn add_data_csv_file<S: Into<String>>(mut self, filename: S) -> Self {
        let csv_content =
            "name,age,city\nAlice,30,New York\nBob,25,San Francisco\nCharlie,35,Chicago";
        self.files.push((filename.into(), csv_content.to_string()));
        self
    }

    /// Enable in-memory storage for the repository
    pub fn use_in_memory_storage(mut self, use_memory: bool) -> Self {
        self.use_in_memory_storage = use_memory;
        self
    }

    /// Set the namespace for the repository (defaults to "test_user")
    pub fn namespace<S: Into<String>>(mut self, namespace: S) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Build the repository and return the path (and repo if using in-memory storage)
    pub async fn build(self) -> Result<TestRepoResult, Box<dyn std::error::Error>> {
        let base_dir = self.base_dir.ok_or("Base directory must be set")?;
        let repo_name = self.repo_name.unwrap_or_else(|| "test_repo".to_string());
        let namespace = self.namespace.unwrap_or_else(|| "test_user".to_string());
        let user_name = self.user_name.unwrap_or_else(|| "Test User".to_string());
        let user_email = self
            .user_email
            .unwrap_or_else(|| "test@example.com".to_string());
        let commit_message = self
            .commit_message
            .unwrap_or_else(|| "Initial commit".to_string());

        let repo_dir = base_dir.join(&namespace).join(&repo_name);
        std::fs::create_dir_all(&repo_dir)?;

        // Initialize repository with or without in-memory storage
        let repo = if self.use_in_memory_storage {
            init_repo_with_in_memory_storage(&repo_dir).await?
        } else {
            liboxen::repositories::init(&repo_dir)?
        };

        // Create files if any were specified
        if !self.files.is_empty() {
            for (filename, content) in &self.files {
                let file_path = repo_dir.join(filename);
                std::fs::write(&file_path, content)?;
                liboxen::repositories::add(&repo, &file_path).await?;
            }

            // Commit the files
            let user = liboxen::model::User {
                name: user_name,
                email: user_email,
            };
            liboxen::repositories::commits::commit_writer::commit_with_user(
                &repo,
                &commit_message,
                &user,
            )?;
        }

        Ok(TestRepoResult {
            repo_dir,
            repo: if self.use_in_memory_storage {
                Some(repo)
            } else {
                None
            },
        })
    }
}

/// Result of building a test repository
pub struct TestRepoResult {
    pub repo_dir: std::path::PathBuf,
    pub repo: Option<liboxen::model::LocalRepository>,
}

impl TestRepoResult {
    /// Get the repository directory path
    #[allow(dead_code)]
    pub fn path(&self) -> &std::path::Path {
        &self.repo_dir
    }

    /// Get the repository (if using in-memory storage)
    #[allow(dead_code)]
    pub fn repo(&self) -> Option<&liboxen::model::LocalRepository> {
        self.repo.as_ref()
    }

    /// Convert to tuple for backward compatibility
    #[allow(dead_code)]
    pub fn into_tuple(self) -> (std::path::PathBuf, Option<liboxen::model::LocalRepository>) {
        (self.repo_dir, self.repo)
    }
}

/// Create an initialized repository with test user configuration
#[allow(dead_code)]
pub async fn make_initialized_repo_with_test_user(
    base_dir: &std::path::Path,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let result = TestRepoBuilder::new()
        .base_dir(base_dir)
        .repo_name("csv_repo")
        .namespace("test_user")
        .user_name("Test")
        .user_email("test@test.com")
        .commit_message("Add CSV data")
        .add_file("products.csv", "product,price,category\nLaptop,999.99,Electronics\nChair,149.50,Furniture\nBook,19.99,Education")
        .build()
        .await?;

    Ok(result.repo_dir)
}

/// Create an initialized repository with test user and files
#[allow(dead_code)]
pub async fn make_initialized_repo_with_test_files(
    base_dir: &std::path::Path,
) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
    let result = TestRepoBuilder::new()
        .base_dir(base_dir)
        .repo_name("test_repo")
        .namespace("test_user")
        .user_name("Test User")
        .user_email("test@example.com")
        .commit_message("Initial commit with test files")
        .add_file(
            "test.txt",
            "Hello from Oxen integration test!\nThis is real file content.",
        )
        .add_file(
            "data.csv",
            "name,age,city\nAlice,30,New York\nBob,25,San Francisco\nCharlie,35,Chicago",
        )
        .build()
        .await?;

    Ok(result.repo_dir)
}

/// Create an initialized repository with test user and CSV file using in-memory storage
#[allow(dead_code)]
pub async fn make_initialized_repo_with_test_user_in_memory(
    base_dir: &std::path::Path,
) -> Result<(std::path::PathBuf, liboxen::model::LocalRepository), Box<dyn std::error::Error>> {
    let result = TestRepoBuilder::new()
        .base_dir(base_dir)
        .repo_name("csv_repo")
        .namespace("test_user")
        .user_name("Test")
        .user_email("test@test.com")
        .commit_message("Add CSV data")
        .add_file("products.csv", "product,price,category\nLaptop,999.99,Electronics\nChair,149.50,Furniture\nBook,19.99,Education")
        .use_in_memory_storage(true)
        .build()
        .await?;

    Ok((result.repo_dir, result.repo.unwrap()))
}

/// Create an initialized repository with in-memory storage for testing
#[allow(dead_code)]
pub async fn make_initialized_repo_with_in_memory_storage(
    base_dir: &std::path::Path,
) -> Result<(std::path::PathBuf, liboxen::model::LocalRepository), Box<dyn std::error::Error>> {
    let result = TestRepoBuilder::new()
        .base_dir(base_dir)
        .repo_name("memory_repo")
        .namespace("test_user")
        .use_in_memory_storage(true)
        .build()
        .await?;

    Ok((result.repo_dir, result.repo.unwrap()))
}

/// Test environment configuration builder
#[derive(Default)]
pub struct TestEnvironmentBuilder {
    timeout_secs: Option<u64>,
    create_repo: Option<bool>,
    repo_type: Option<RepoType>,
    test_name: Option<String>,
    bearer_token: Option<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum RepoType {
    WithTestFiles,
    WithTestUser,
    WithCsv,
    Empty,
}

impl TestEnvironmentBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn timeout_secs(mut self, timeout: u64) -> Self {
        self.timeout_secs = Some(timeout);
        self
    }

    pub fn with_repo(mut self, repo_type: RepoType) -> Self {
        self.create_repo = Some(true);
        self.repo_type = Some(repo_type);
        self
    }

    pub fn without_repo(mut self) -> Self {
        self.create_repo = Some(false);
        self
    }

    pub fn test_name<S: Into<String>>(mut self, name: S) -> Self {
        self.test_name = Some(name.into());
        self
    }

    #[allow(dead_code)]
    pub fn with_bearer_token<S: Into<String>>(mut self, token: S) -> Self {
        self.bearer_token = Some(token.into());
        self
    }

    pub async fn build(self) -> Result<TestEnvironment, Box<dyn std::error::Error>> {
        let timeout = self.timeout_secs.unwrap_or(10);
        let create_repo = self.create_repo.unwrap_or(true);
        let test_name = self.test_name.unwrap_or_else(|| "test".to_string());

        let unique_id = std::thread::current().id();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let test_dir =
            std::env::temp_dir().join(format!("oxen_{}_{:?}_{}", test_name, unique_id, timestamp));
        let _ = std::fs::remove_dir_all(&test_dir);
        std::fs::create_dir_all(&test_dir).expect("Failed to create test directory");

        // Create repository if requested
        let repo_dir = if create_repo {
            let repo_type = self.repo_type.unwrap_or(RepoType::WithTestFiles);
            let result = match repo_type {
                RepoType::WithTestFiles => {
                    make_initialized_repo_with_test_files_in_memory(&test_dir)
                        .await?
                        .0
                }
                RepoType::WithTestUser => {
                    make_initialized_repo_with_test_user_in_memory(&test_dir)
                        .await?
                        .0
                }
                RepoType::WithCsv => {
                    let result = TestRepoBuilder::new()
                        .base_dir(&test_dir)
                        .repo_name("csv_repo")
                        .namespace("test_user")
                        .add_file("products.csv", "product,price,category\nLaptop,999.99,Electronics\nChair,149.50,Furniture\nBook,19.99,Education")
                        .use_in_memory_storage(true)
                        .build()
                        .await?;
                    result.repo_dir
                }
                RepoType::Empty => {
                    let result = TestRepoBuilder::new()
                        .base_dir(&test_dir)
                        .repo_name("empty_repo")
                        .namespace("test_user")
                        .use_in_memory_storage(true)
                        .build()
                        .await?;
                    result.repo_dir
                }
            };
            Some(result)
        } else {
            None
        };

        // Start oxen-server with auto-port allocation
        let server = TestServer::start_with_auto_port(&test_dir)
            .await
            .expect("Failed to start test server");

        // Create HTTP client with optional bearer token
        let client = if let Some(token) = self.bearer_token {
            let mut headers = reqwest::header::HeaderMap::new();
            let auth_value = reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token))
                .expect("Invalid bearer token");
            headers.insert(reqwest::header::AUTHORIZATION, auth_value);

            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(timeout))
                .default_headers(headers)
                .build()
                .expect("Failed to create HTTP client with bearer token")
        } else {
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(timeout))
                .build()
                .expect("Failed to create HTTP client")
        };

        Ok(TestEnvironment {
            test_dir,
            repo_dir,
            server,
            client,
            cleanup: true,
        })
    }
}

/// RAII Test environment that automatically cleans up
pub struct TestEnvironment {
    test_dir: std::path::PathBuf,
    repo_dir: Option<std::path::PathBuf>, // Track the actual repo directory for DB cache cleanup
    server: TestServer,
    client: reqwest::Client,
    cleanup: bool,
}

impl TestEnvironment {
    pub fn builder() -> TestEnvironmentBuilder {
        TestEnvironmentBuilder::new()
    }

    /// Create a valid bearer token for testing that works with the server's AccessKeyManager
    #[allow(dead_code)]
    pub fn create_test_bearer_token(&self) -> Result<String, Box<dyn std::error::Error>> {
        // Create a test user
        let test_user = liboxen::model::User {
            name: "Test User".to_string(),
            email: "test@example.com".to_string(),
        };

        // Set up the server's access key infrastructure
        // The server expects keys to be in sync_dir/.oxen/
        let oxen_hidden_dir = liboxen::util::fs::oxen_hidden_dir(&self.test_dir);
        std::fs::create_dir_all(&oxen_hidden_dir)?;

        // Create the secret key file that AccessKeyManager expects
        let secret_key_path = oxen_hidden_dir.join("SECRET_KEY_BASE");
        let secret = "test-secret-key-for-oxen-testing-only";
        liboxen::util::fs::write_to_path(&secret_key_path, secret)?;

        // Create the keys database directory that AccessKeyManager expects
        let keys_dir = oxen_hidden_dir.join("keys");
        std::fs::create_dir_all(&keys_dir)?;

        // Manually create and store the token in the database like AccessKeyManager does
        use jsonwebtoken::{encode, EncodingKey, Header};
        use serde::{Deserialize, Serialize};
        use rocksdb::{DBWithThreadMode, MultiThreaded, Options};

        #[derive(Debug, Serialize, Deserialize)]
        struct Claims {
            id: String,
            name: String,
            email: String,
        }

        let claims = Claims {
            id: uuid::Uuid::new_v4().to_string(),
            name: test_user.name,
            email: test_user.email,
        };

        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(secret.as_ref()),
        )?;
        
        // Store the token in the database like AccessKeyManager::create does
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DBWithThreadMode::<MultiThreaded>::open(&opts, &keys_dir)?;
        let encoded_claim = serde_json::to_string(&claims)?;
        db.put(&token, encoded_claim)?;
        
        Ok(token)
    }

    #[allow(dead_code)]
    pub fn test_dir(&self) -> &std::path::Path {
        &self.test_dir
    }

    #[allow(dead_code)]
    pub fn server(&self) -> &TestServer {
        &self.server
    }

    #[allow(dead_code)]
    pub fn client(&self) -> &reqwest::Client {
        &self.client
    }

    pub fn into_parts(mut self) -> (std::path::PathBuf, TestServer, reqwest::Client) {
        // Disable cleanup since caller is taking ownership
        self.cleanup = false;

        // Release DB cache instances to prevent LOCK file conflicts with server
        // This closes any RocksDB instances opened during repository initialization
        if let Some(repo_dir) = &self.repo_dir {
            let _ = liboxen::core::staged::remove_from_cache_with_children(repo_dir);
            let _ = liboxen::core::refs::remove_from_cache(repo_dir);
        }

        // Safely move out fields using mem::replace
        let test_dir = std::mem::replace(&mut self.test_dir, std::path::PathBuf::new());
        let server = std::mem::replace(&mut self.server, TestServer::dummy());
        let client = std::mem::replace(&mut self.client, reqwest::Client::new());

        (test_dir, server, client)
    }
}

impl Drop for TestEnvironment {
    fn drop(&mut self) {
        if self.cleanup {
            let _ = std::fs::remove_dir_all(&self.test_dir);
        }
    }
}

/// Helper function to create test environment with auto-port allocation
/// This replaces the manual create_test_environment(port) pattern
#[allow(dead_code)]
pub async fn create_test_environment_with_auto_port(
) -> Result<(std::path::PathBuf, TestServer, reqwest::Client), Box<dyn std::error::Error>> {
    let env = TestEnvironment::builder()
        .test_name("auto_port_test")
        .with_repo(RepoType::WithTestFiles)
        .build()
        .await?;

    Ok(env.into_parts())
}

/// Create an initialized repository with test files using in-memory storage
#[allow(dead_code)]
pub async fn make_initialized_repo_with_test_files_in_memory(
    base_dir: &std::path::Path,
) -> Result<(std::path::PathBuf, liboxen::model::LocalRepository), Box<dyn std::error::Error>> {
    let result = TestRepoBuilder::new()
        .base_dir(base_dir)
        .repo_name("test_repo")
        .namespace("test_user")
        .user_name("Test User")
        .user_email("test@example.com")
        .commit_message("Initial commit with test files")
        .add_file(
            "test.txt",
            "Hello from Oxen integration test!\nThis is real file content.",
        )
        .add_file(
            "data.csv",
            "name,age,city\nAlice,30,New York\nBob,25,San Francisco\nCharlie,35,Chicago",
        )
        .use_in_memory_storage(false)
        .build()
        .await?;

    let repo = if let Some(repo) = result.repo {
        repo
    } else {
        // For non-in-memory storage, we need to load the repo from the directory
        liboxen::model::LocalRepository::from_dir(&result.repo_dir)?
    };
    Ok((result.repo_dir, repo))
}

/// Helper function to initialize a repository with in-memory storage using composition
/// This creates the repository structure and injects the in-memory storage
#[allow(dead_code)]
async fn init_repo_with_in_memory_storage(
    repo_dir: &std::path::Path,
) -> Result<liboxen::model::LocalRepository, Box<dyn std::error::Error>> {
    // First initialize the repository filesystem structure
    let _repo = liboxen::repositories::init(repo_dir)?;

    // Create the repository with in-memory storage
    let in_memory_store = std::sync::Arc::new(InMemoryVersionStore::new());
    let repo = liboxen::model::LocalRepository::with_version_store(repo_dir, in_memory_store)?;

    Ok(repo)
}
