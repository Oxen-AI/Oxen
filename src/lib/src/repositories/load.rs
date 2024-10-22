use flate2::read::GzDecoder;
use std::{fs::File, path::Path};
use tar::Archive;

use crate::constants::DEFAULT_BRANCH_NAME;
use crate::repositories;
use crate::{error::OxenError, model::LocalRepository};

pub async fn load(
    src_path: &Path,
    dest_path: &Path,
    no_working_dir: bool,
) -> Result<(), OxenError> {
    let done_msg: String = format!(
        "✅ Loaded {:?} to an oxen repo at {:?}",
        src_path, dest_path
    );

    let dest_path = if dest_path.exists() {
        if dest_path.is_file() {
            return Err(OxenError::basic_str(
                "Destination path is a file, must be a directory",
            ));
        }
        dest_path.to_path_buf()
    } else {
        std::fs::create_dir_all(dest_path)?;
        dest_path.to_path_buf()
    };

    let file = File::open(src_path)?;
    let tar = GzDecoder::new(file);
    println!("🐂 Decompressing oxen repo into {:?}", dest_path);
    let mut archive = Archive::new(tar);
    archive.unpack(&dest_path)?;

    // Server repos - done unpacking
    if no_working_dir {
        println!("{done_msg}");
        return Ok(());
    }

    // Client repos - need to hydrate working dir from versions files
    let repo = LocalRepository::new(&dest_path)?;

    println!("🐂 Unpacking files to working directory {:?}", dest_path);
    let branch = repositories::branches::get_by_name(&repo, DEFAULT_BRANCH_NAME)?
        .ok_or(OxenError::local_branch_not_found(DEFAULT_BRANCH_NAME))?;
    let commit = repositories::commits::get_by_id(&repo, &branch.commit_id)?
        .ok_or(OxenError::commit_id_does_not_exist(&branch.commit_id))?;
    repositories::branches::set_working_repo_to_commit(&repo, &commit, true).await?;

    println!("{done_msg}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::error::OxenError;
    use crate::model::LocalRepository;
    use crate::repositories;
    use crate::test;
    use crate::util;

    #[test]
    fn test_command_save_repo() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Write one file
            let hello_file = repo.path.join("hello.txt");
            util::fs::write_to_path(&hello_file, "Hello World")?;
            // Add-commit
            repositories::add(&repo, &hello_file)?;
            repositories::commit(&repo, "Adding hello file")?;

            // Save to a path
            let save_path = repo.path.join(Path::new("backup.tar.gz"));
            repositories::save(&repo, &save_path)?;

            assert!(save_path.exists());

            // Cleanup tarball
            util::fs::remove_file(save_path)?;

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_command_save_load_repo_with_working_dir() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            test::run_empty_dir_test_async(|dir| async move {
                // Write one file
                let hello_file = repo.path.join("hello.txt");
                util::fs::write_to_path(&hello_file, "Hello World")?;
                // Add-commit
                repositories::add(&repo, &hello_file)?;
                repositories::commit(&repo, "Adding hello file")?;

                // Save to a path
                let save_path = dir.join(Path::new("backup.tar.gz"));
                repositories::save(&repo, &save_path)?;

                // Load from a path and hydrate
                let loaded_repo_path = dir.join(Path::new("loaded_repo"));
                repositories::load(&save_path, &loaded_repo_path, false).await?;

                let hydrated_repo = LocalRepository::from_dir(&loaded_repo_path)?;
                assert!(hydrated_repo.path.join("hello.txt").exists());

                // Cleanup tarball
                util::fs::remove_file(save_path)?;

                Ok(dir)
            })
            .await
        })
        .await
    }

    #[tokio::test]
    async fn test_command_save_load_repo_no_working_dir() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            test::run_empty_dir_test_async(|dir| async move {
                // Write one file
                let hello_file = repo.path.join("hello.txt");
                util::fs::write_to_path(&hello_file, "Hello World")?;
                // Add-commit
                repositories::add(&repo, &hello_file)?;
                repositories::commit(&repo, "Adding hello file")?;

                // Save to a path
                let save_path = dir.join(Path::new("backup.tar.gz"));
                repositories::save(&repo, &save_path)?;

                // Load from a path and hydrate
                let loaded_repo_path = dir.join(Path::new("loaded_repo"));
                repositories::load(&save_path, &loaded_repo_path, true).await?;

                let hydrated_repo = LocalRepository::from_dir(&loaded_repo_path)?;

                assert!(!hydrated_repo.path.join("hello.txt").exists());

                // Should have `hello.txt` in removed files bc it's in commits db but not working dir
                let status = repositories::status(&hydrated_repo)?;

                assert_eq!(status.removed_files.len(), 1);

                // Cleanup tarball
                util::fs::remove_file(save_path)?;

                Ok(dir)
            })
            .await
        })
        .await
    }

    #[tokio::test]
    async fn test_command_save_load_moved_and_removed() -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            test::run_empty_dir_test_async(|dir| async move {
                // Write one file
                let hello_file = repo.path.join("hello.txt");
                let goodbye_file = repo.path.join("goodbye.txt");
                util::fs::write_to_path(&hello_file, "Hello World")?;
                util::fs::write_to_path(&goodbye_file, "Goodbye World")?;
                // Add-commit
                repositories::add(&repo, &hello_file)?;
                repositories::add(&repo, &goodbye_file)?;
                repositories::commit(&repo, "Adding hello file")?;

                // Move hello into a folder
                let hello_dir = repo.path.join("hello_dir");
                std::fs::create_dir(&hello_dir)?;
                let moved_hello = hello_dir.join("hello.txt");
                util::fs::rename(&hello_file, &moved_hello)?;

                // Remove goodbye
                std::fs::remove_file(&goodbye_file)?;

                // Add a third file
                let third_file = repo.path.join("third.txt");
                util::fs::write_to_path(&third_file, "Third File")?;

                // Add-commit
                repositories::add(&repo, &moved_hello)?;
                repositories::add(&repo, &hello_file)?;
                repositories::add(&repo, &goodbye_file)?;
                repositories::add(&repo, &third_file)?;
                repositories::commit(&repo, "Moving hello file")?;

                // Save to a path
                let save_path = dir.join(Path::new("backup.tar.gz"));
                repositories::save(&repo, &save_path)?;

                // Load from a path and hydrate
                let loaded_repo_path = dir.join(Path::new("loaded_repo"));
                repositories::load(&save_path, &loaded_repo_path, false).await?;

                let hydrated_repo = LocalRepository::from_dir(&loaded_repo_path)?;

                // List files in repo
                let files = util::fs::rlist_files_in_dir(&hydrated_repo.path);
                println!("Files in hydrated repo: {:?}", files);

                assert!(hydrated_repo.path.join("third.txt").exists());
                assert!(hydrated_repo.path.join("hello_dir/hello.txt").exists());
                assert!(!hydrated_repo.path.join("hello.txt").exists());
                assert!(!hydrated_repo.path.join("goodbye.txt").exists());

                // Cleanup tarball
                util::fs::remove_file(save_path)?;

                Ok(dir)
            })
            .await
        })
        .await
    }
}
