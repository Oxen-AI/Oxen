// Catch all tests for the library

use std::path::Path;

use liboxen::error::OxenError;
use liboxen::repositories;
use liboxen::test;

#[test]
fn test_oxen_ignore_file() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        // Add a file that we are going to ignore
        let ignore_filename = "ignoreme.txt";
        let ignore_path = repo.path.join(ignore_filename);
        test::write_txt_file_to_path(ignore_path, "I should be ignored")?;

        let oxenignore_file = repo.path.join(".oxenignore");
        test::write_txt_file_to_path(oxenignore_file, ignore_filename)?;

        let status = repositories::status(&repo)?;
        // Only untracked file should be .oxenignore
        assert_eq!(status.untracked_files.len(), 1);
        assert_eq!(
            status.untracked_files.first().unwrap(),
            Path::new(".oxenignore")
        );

        Ok(())
    })
}

#[test]
fn test_oxen_ignore_dir() -> Result<(), OxenError> {
    test::run_training_data_repo_test_fully_committed(|repo| {
        // Add a file that we are going to ignore
        let ignore_dir = "ignoreme/";
        let ignore_path = repo.path.join(ignore_dir);
        std::fs::create_dir(&ignore_path)?;
        test::write_txt_file_to_path(ignore_path.join("0.txt"), "I should be ignored")?;
        test::write_txt_file_to_path(ignore_path.join("1.txt"), "I should also be ignored")?;

        let oxenignore_file = repo.path.join(".oxenignore");
        test::write_txt_file_to_path(oxenignore_file, "ignoreme.txt")?;

        let status = repositories::status(&repo)?;
        // Only untracked file should be .oxenignore
        assert_eq!(status.untracked_files.len(), 1);
        assert_eq!(
            status.untracked_files.first().unwrap(),
            Path::new(".oxenignore")
        );

        Ok(())
    })
}
