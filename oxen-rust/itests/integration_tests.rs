// Catch all tests for the library

use crate::common::test_repository_builder::TestRepositoryBuilder;
use liboxen::error::OxenError;

#[tokio::test]
async fn test_oxen_ignore_file() -> Result<(), OxenError> {
    let test_repo = TestRepositoryBuilder::new("test_namespace", "ignore_file_repo")
        .with_file("ignoreme.txt", "I should be ignored")
        .build()
        .await?;

    let repo = test_repo.repo();
    let repo_dir = test_repo.repo_dir();

    let oxenignore_path = repo_dir.join(".oxenignore");
    liboxen::util::fs::write_to_path(&oxenignore_path, "ignoreme.txt")?;

    liboxen::command::add(repo, &oxenignore_path).await?;

    let status = liboxen::repositories::status(repo)?;
    assert_eq!(status.untracked_files.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_oxen_ignore_dir() -> Result<(), OxenError> {
    let test_repo = TestRepositoryBuilder::new("test_namespace", "ignore_dir_repo")
        .with_file("ignoreme/0.txt", "I should be ignored")
        .with_file("ignoreme/1.txt", "I should also be ignored")
        .build()
        .await?;

    let repo = test_repo.repo();
    let repo_dir = test_repo.repo_dir();

    let oxenignore_path = repo_dir.join(".oxenignore");
    liboxen::util::fs::write_to_path(&oxenignore_path, "ignoreme/")?;

    liboxen::command::add(repo, &oxenignore_path).await?;

    let status = liboxen::repositories::status(repo)?;
    assert_eq!(status.untracked_files.len(), 0);

    Ok(())
}
