//! # oxen fsck
//!
//! Check repository integrity
//!

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::repositories;
    use crate::test;

    #[tokio::test]
    async fn test_fsck_dry_run_detects_corrupted_version() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Add and commit a file so we have a version in the store
            let file_path = repo.path.join("hello.txt");
            test::write_txt_file_to_path(&file_path, "hello world")?;
            repositories::add(&repo, &file_path).await?;
            repositories::commit(&repo, "Adding hello.txt")?;

            let version_store = repo.version_store()?;
            let versions = version_store.list_versions().await?;
            assert!(!versions.is_empty());

            // Corrupt a version file by overwriting its data
            let hash = &versions[0];
            let version_path = version_store.get_version_path(hash)?;
            std::fs::write(&version_path, b"corrupted data")?;

            // Dry run should detect corruption but not delete
            let result = version_store.clean_corrupted_versions(true).await?;
            assert!(result.corrupted > 0);
            assert_eq!(result.cleaned, 0);

            // The corrupted file should still exist
            assert!(version_store.version_exists(hash).await?);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_fsck_clean_removes_corrupted_version() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Add and commit a file so we have a version in the store
            let file_path = repo.path.join("hello.txt");
            test::write_txt_file_to_path(&file_path, "hello world")?;
            repositories::add(&repo, &file_path).await?;
            repositories::commit(&repo, "Adding hello.txt")?;

            let version_store = repo.version_store()?;
            let versions = version_store.list_versions().await?;
            assert!(!versions.is_empty());

            // Corrupt a version file by overwriting its data
            let hash = &versions[0];
            let version_path = version_store.get_version_path(hash)?;
            std::fs::write(&version_path, b"corrupted data")?;

            // Clean should detect and remove the corrupted file
            let result = version_store.clean_corrupted_versions(false).await?;
            assert!(result.corrupted > 0);
            assert!(result.cleaned > 0);

            // The corrupted file should be gone
            assert!(!version_store.version_exists(hash).await?);

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_fsck_no_corruption_on_clean_repo() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            // Add and commit a file
            let file_path = repo.path.join("hello.txt");
            test::write_txt_file_to_path(&file_path, "hello world")?;
            repositories::add(&repo, &file_path).await?;
            repositories::commit(&repo, "Adding hello.txt")?;

            let version_store = repo.version_store()?;

            // No corruption on a clean repo
            let result = version_store.clean_corrupted_versions(true).await?;
            assert_eq!(result.corrupted, 0);
            assert!(result.scanned > 0);

            Ok(())
        })
        .await
    }
}
