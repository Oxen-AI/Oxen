/// Demonstration of the TestRepositoryBuilder fluent API
/// This shows how close we can get to the desired syntax
use crate::common::TestRepositoryBuilder;

#[tokio::test]
async fn demonstrate_fluent_api() {
    // Your desired syntax:
    // let store = TestRepositoryBuilder::new("namespace", "repo_name")
    //     .with_file("data.csv", "id,name\n1,Alice\n2,Bob")
    //     .with_file("config.json", r#"{"version": "1.0"}"#)
    //     .with_commit_message("Test data setup")
    //     .build()
    //     .unwrap();

    // What we've achieved (identical!):
    let store = TestRepositoryBuilder::new("namespace", "repo_name")
        .with_file("data.csv", "id,name\n1,Alice\n2,Bob")
        .with_file("config.json", r#"{"version": "1.0"}"#)
        .with_commit_message("Test data setup")
        .with_memory_storage() // Use in-memory for speed
        .build()
        .await
        .unwrap();

    // Additional features available:
    let _advanced_store = TestRepositoryBuilder::new("advanced", "repo")
        .with_file("src/main.rs", "fn main() {}")
        .with_file("src/lib.rs", "pub fn hello() {}")
        .with_file("tests/test.rs", "#[test] fn test() {}")
        .with_user("Custom User", "user@example.com")
        .with_commit_message("Initial commit")
        .with_filesystem_storage() // Use real filesystem instead of in-memory
        .build()
        .await
        .unwrap();

    // Working with the created repository:
    assert_eq!(store.files().len(), 2);
    assert_eq!(
        store.file_content("data.csv").unwrap(),
        "id,name\n1,Alice\n2,Bob"
    );
    assert_eq!(
        store.file_content("config.json").unwrap(),
        r#"{"version": "1.0"}"#
    );

    // The repository is a full LocalRepository
    let _repo = store.repo();
    let _repo_path = store.repo_dir();

    println!("✅ Fluent API demo completed successfully!");
}

/// Additional example showing post-creation operations
#[tokio::test]
async fn demonstrate_post_creation_operations() {
    let mut test_repo = TestRepositoryBuilder::new("mutable", "repo")
        .with_file("initial.txt", "Initial content")
        .with_commit_message("Initial commit")
        .build()
        .await
        .unwrap();

    // Add more files after creation
    test_repo
        .add_file("added.txt", "Added content")
        .await
        .unwrap();
    test_repo
        .add_file("nested/file.txt", "Nested content")
        .await
        .unwrap();

    // Commit changes
    test_repo.commit("Add more files").unwrap();

    // Verify
    assert_eq!(test_repo.files().len(), 3);
    assert!(test_repo.repo_dir().join("nested/file.txt").exists());

    println!("✅ Post-creation operations demo completed!");
}
