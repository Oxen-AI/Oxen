use liboxen::error::OxenError;
use liboxen::model::{LocalRepository, User};
use liboxen::repositories;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// A fluent builder for creating test repositories with files and commits
pub struct TestRepositoryBuilder {
    namespace: String,
    repo_name: String,
    files: HashMap<String, String>,
    commit_message: Option<String>,
    user: Option<User>,
    use_memory_storage: bool,
    base_dir: Option<PathBuf>,
}

/// Result of building a test repository
pub struct TestRepository {
    pub repo: LocalRepository,
    pub repo_dir: PathBuf,
    pub files: HashMap<String, String>,
}

impl TestRepositoryBuilder {
    /// Create a new test repository builder
    pub fn new(namespace: &str, repo_name: &str) -> Self {
        Self {
            namespace: namespace.to_string(),
            repo_name: repo_name.to_string(),
            files: HashMap::new(),
            commit_message: None,
            user: None,
            use_memory_storage: false, // Default to filesystem for compatibility
            base_dir: None,
        }
    }

    /// Add a file with content to the repository
    pub fn with_file(mut self, path: &str, content: &str) -> Self {
        self.files.insert(path.to_string(), content.to_string());
        self
    }

    /// Set the commit message for the initial commit
    pub fn with_commit_message(mut self, message: &str) -> Self {
        self.commit_message = Some(message.to_string());
        self
    }

    /// Set the user for commits (defaults to test user)
    pub fn with_user(mut self, name: &str, email: &str) -> Self {
        self.user = Some(User {
            name: name.to_string(),
            email: email.to_string(),
        });
        self
    }

    /// Use file system storage instead of in-memory (slower but more realistic)
    pub fn with_filesystem_storage(mut self) -> Self {
        self.use_memory_storage = false;
        self
    }

    /// Use in-memory storage for faster tests (may have limited functionality)
    pub fn with_memory_storage(mut self) -> Self {
        self.use_memory_storage = true;
        self
    }

    /// Set the base directory for the repository (defaults to temp dir)
    pub fn with_base_dir(mut self, base_dir: &Path) -> Self {
        self.base_dir = Some(base_dir.to_path_buf());
        self
    }

    /// Build the test repository
    pub async fn build(self) -> Result<TestRepository, OxenError> {
        let base_dir = self.base_dir.clone().unwrap_or_else(|| {
            // Use unique directory for each test to avoid lock conflicts
            let unique_id = std::thread::current().id();
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let temp_dir =
                std::env::temp_dir().join(format!("oxen_test_repos_{:?}_{}", unique_id, timestamp));
            if let Err(e) = std::fs::remove_dir_all(&temp_dir) {
                log::debug!("Failed to remove temp directory {:?}: {}", temp_dir, e);
            }
            temp_dir
        });

        let repo_dir = base_dir.join(&self.namespace).join(&self.repo_name);
        std::fs::create_dir_all(&repo_dir).map_err(|e| OxenError::IO(e))?;

        // Initialize repository with appropriate storage
        let repo = if self.use_memory_storage {
            self.init_repo_with_in_memory_storage(&repo_dir).await?
        } else {
            repositories::init(&repo_dir)?
        };

        // Create and add files
        for (file_path, content) in &self.files {
            let full_path = repo_dir.join(file_path);

            // Create parent directories if needed
            if let Some(parent) = full_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| OxenError::IO(e))?;
            }

            std::fs::write(&full_path, content).map_err(|e| OxenError::IO(e))?;

            repositories::add(&repo, &full_path).await?;
        }

        // Commit if there are files
        if !self.files.is_empty() {
            let user = self.user.unwrap_or_else(|| User {
                name: "Test User".to_string(),
                email: "test@example.com".to_string(),
            });

            let commit_msg = self
                .commit_message
                .unwrap_or_else(|| "Initial commit".to_string());

            repositories::commits::commit_writer::commit_with_user(&repo, &commit_msg, &user)
                .map(|_| ())?;
        }

        Ok(TestRepository {
            repo,
            repo_dir,
            files: self.files,
        })
    }

    /// Initialize a repository with in-memory storage using composition
    async fn init_repo_with_in_memory_storage(
        &self,
        repo_dir: &Path,
    ) -> Result<LocalRepository, OxenError> {
        // First initialize the repository filesystem structure
        let _repo = repositories::init(repo_dir)?;

        // Create the repository with in-memory storage
        let in_memory_store = std::sync::Arc::new(super::InMemoryVersionStore::new());
        let repo = LocalRepository::with_version_store(repo_dir, in_memory_store)?;

        Ok(repo)
    }
}

impl TestRepository {
    /// Get the repository
    pub fn repo(&self) -> &LocalRepository {
        &self.repo
    }

    /// Get the repository directory
    pub fn repo_dir(&self) -> &Path {
        &self.repo_dir
    }

    /// Get all files that were added
    pub fn files(&self) -> &HashMap<String, String> {
        &self.files
    }

    /// Get a specific file's content
    pub fn file_content(&self, path: &str) -> Option<&String> {
        self.files.get(path)
    }

    /// Add another file to the repository
    pub async fn add_file(&mut self, path: &str, content: &str) -> Result<(), OxenError> {
        let full_path = self.repo_dir.join(path);

        // Create parent directories if needed
        if let Some(parent) = full_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| OxenError::IO(e))?;
        }

        std::fs::write(&full_path, content).map_err(|e| OxenError::IO(e))?;

        // Ensure staged directory exists for filesystem-based operations
        let staged_dir = self.repo_dir.join(".oxen").join("staged");
        if !staged_dir.exists() {
            std::fs::create_dir_all(&staged_dir).map_err(|e| OxenError::IO(e))?;
        }

        repositories::add(&self.repo, &full_path).await?;
        self.files.insert(path.to_string(), content.to_string());

        Ok(())
    }

    /// Commit changes
    pub fn commit(&self, message: &str) -> Result<(), OxenError> {
        let user = User {
            name: "Test User".to_string(),
            email: "test@example.com".to_string(),
        };

        repositories::commits::commit_writer::commit_with_user(&self.repo, message, &user)
            .map(|_| ())
    }

    /// Commit changes with specific user
    #[allow(dead_code)]
    pub fn commit_with_user(&self, message: &str, user: &User) -> Result<(), OxenError> {
        repositories::commits::commit_writer::commit_with_user(&self.repo, message, user)
            .map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_repository_creation() {
        let test_repo = TestRepositoryBuilder::new("test_namespace", "test_repo")
            .with_file("data.csv", "id,name\n1,Alice\n2,Bob")
            .with_file("config.json", r#"{"version": "1.0"}"#)
            .with_commit_message("Test data setup")
            .build()
            .await
            .unwrap();

        assert_eq!(test_repo.files().len(), 2);
        assert_eq!(
            test_repo.file_content("data.csv").unwrap(),
            "id,name\n1,Alice\n2,Bob"
        );
        assert_eq!(
            test_repo.file_content("config.json").unwrap(),
            r#"{"version": "1.0"}"#
        );
    }

    #[tokio::test]
    async fn test_nested_file_creation() {
        let test_repo = TestRepositoryBuilder::new("test_namespace", "nested_repo")
            .with_file("src/main.rs", "fn main() {}")
            .with_file("tests/integration.rs", "#[test] fn test() {}")
            .with_commit_message("Add source files")
            .build()
            .await
            .unwrap();

        assert_eq!(test_repo.files().len(), 2);
        assert!(test_repo.repo_dir().join("src/main.rs").exists());
        assert!(test_repo.repo_dir().join("tests/integration.rs").exists());
    }

    #[tokio::test]
    async fn test_custom_user() {
        let test_repo = TestRepositoryBuilder::new("test_namespace", "user_repo")
            .with_file("readme.txt", "Hello World")
            .with_user("Custom User", "custom@example.com")
            .with_commit_message("Custom commit")
            .build()
            .await
            .unwrap();

        assert_eq!(test_repo.files().len(), 1);
        // Note: We can't easily test the user in the commit without more repository introspection
    }

    #[tokio::test]
    async fn test_add_file_after_creation() {
        let mut test_repo = TestRepositoryBuilder::new("test_namespace", "mutable_repo")
            .with_file("initial.txt", "Initial content")
            .build()
            .await
            .unwrap();

        // Add another file
        test_repo
            .add_file("added.txt", "Added content")
            .await
            .unwrap();
        assert_eq!(test_repo.files().len(), 2);

        // Commit the changes
        test_repo.commit("Add new file").unwrap();
    }
}
