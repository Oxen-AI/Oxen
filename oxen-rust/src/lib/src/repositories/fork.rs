use crate::error::OxenError;
use crate::util::fs as oxen_fs;
use crate::view::fork::{ForkStartResponse, ForkStatus, ForkStatusFile, ForkStatusResponse};
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use toml;

pub const FORK_STATUS_FILE: &str = ".oxen/fork_status.toml";

fn write_status(repo_path: &Path, status: &ForkStatus) -> Result<(), OxenError> {
    let status_path = repo_path.join(FORK_STATUS_FILE);
    if let Some(parent) = status_path.parent() {
        oxen_fs::create_dir_all(parent)?;
    }
    let status_file: ForkStatusFile = status.clone().into();
    fs::write(status_path, toml::to_string(&status_file)?)?;
    Ok(())
}

fn read_status(repo_path: &Path) -> Result<Option<ForkStatus>, OxenError> {
    let status_path = repo_path.join(FORK_STATUS_FILE);
    if !status_path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(&status_path)?;
    let status_file: ForkStatusFile = toml::from_str(&content).map_err(|e| {
        log::error!("Failed to parse fork status on file: {status_path:?} error: {e}");
        OxenError::basic_str(format!("Failed to parse fork status on file: {e}"))
    })?;

    let status = &status_file.status;

    Ok(Some(match status {
        ForkStatus::Started => ForkStatus::Started,
        ForkStatus::InProgress(_) => ForkStatus::InProgress(status_file.progress.unwrap_or(0.0)),
        ForkStatus::Complete => ForkStatus::Complete,
        ForkStatus::Counting(_) => ForkStatus::Counting(status_file.progress.unwrap_or(0.0) as u32),
        ForkStatus::Failed(_) => ForkStatus::Failed(
            status_file
                .error
                .unwrap_or_else(|| "Unknown error".to_string()),
        ),
    }))
}

pub fn start_fork(
    original_path: PathBuf,
    new_path: PathBuf,
) -> Result<ForkStartResponse, OxenError> {
    if new_path.exists() {
        return Err(OxenError::basic_str(format!(
            "A file already exists at the destination path: {}",
            new_path.to_string_lossy()
        )));
    }

    oxen_fs::create_dir_all(&new_path)?;
    write_status(&new_path, &ForkStatus::Counting(0))?;

    let new_path_clone = new_path.clone();
    let mut current_count = 0;

    thread::spawn(move || {
        let total_items = match count_items(&original_path, &new_path, &mut current_count) {
            Ok(count) => count as f32,
            Err(e) => {
                log::error!("Failed to count items: {e}");
                write_status(&new_path, &ForkStatus::Failed(e.to_string())).unwrap_or_else(|e| {
                    log::error!("Failed to write error status: {e}");
                });
                return;
            }
        };
        let mut copied_items = 0.0;
        match copy_dir_recursive(
            &original_path,
            &new_path,
            &new_path,
            total_items,
            &mut copied_items,
        ) {
            Ok(()) => {
                write_status(&new_path, &ForkStatus::Complete).unwrap_or_else(|e| {
                    log::error!("Failed to write completion status: {e}");
                });
            }
            Err(e) => {
                write_status(&new_path, &ForkStatus::Failed(e.to_string())).unwrap_or_else(|e| {
                    log::error!("Failed to write error status: {e}");
                });
            }
        }
    });

    Ok(ForkStartResponse {
        repository: new_path_clone.to_string_lossy().to_string(),
        fork_status: ForkStatus::Started.to_string(),
    })
}

pub fn get_fork_status(repo_path: &Path) -> Result<ForkStatusResponse, OxenError> {
    let status = read_status(repo_path)?.ok_or_else(OxenError::fork_status_not_found)?;

    Ok(ForkStatusResponse {
        repository: repo_path.to_string_lossy().to_string(),
        status: match status {
            ForkStatus::Started => ForkStatus::Started.to_string(),
            ForkStatus::Counting(_) => ForkStatus::Counting(0).to_string(),
            ForkStatus::InProgress(_) => ForkStatus::InProgress(0.0).to_string(),
            ForkStatus::Complete => ForkStatus::Complete.to_string(),
            ForkStatus::Failed(_) => ForkStatus::Failed("".to_string()).to_string(),
        },
        progress: match status {
            ForkStatus::InProgress(p) => Some(p),
            ForkStatus::Counting(c) => Some(c as f32),
            _ => None,
        },
        error: match status {
            ForkStatus::Failed(e) => Some(e),
            _ => None,
        },
    })
}

fn copy_dir_recursive(
    src: &Path,
    dst: &Path,
    status_repo: &Path,
    total_items: f32,
    copied_items: &mut f32,
) -> Result<(), OxenError> {
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let dest_path = dst.join(entry.file_name());

        if path.ends_with(".oxen/workspaces") {
            continue;
        }

        if path.is_dir() {
            oxen_fs::create_dir_all(&dest_path)?;
            copy_dir_recursive(&path, &dest_path, status_repo, total_items, copied_items)?;
        } else {
            fs::copy(&path, &dest_path)?;
            *copied_items += 1.0;
        }
    }

    let progress = if total_items > 0.0 {
        (*copied_items / total_items) * 100.0
    } else {
        100.0 // Assume completion if there are no items to copy
    };
    write_status(status_repo, &ForkStatus::InProgress(progress))?;
    Ok(())
}

fn count_items(path: &Path, status_repo: &Path, current_count: &mut u32) -> Result<u32, OxenError> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if path.ends_with(".oxen/workspaces") {
            continue;
        }
        if path.is_dir() {
            count_items(&path, status_repo, current_count)?;
        } else {
            *current_count += 1;
        }
    }
    write_status(status_repo, &ForkStatus::Counting(*current_count))?;
    Ok(*current_count)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::error::OxenError;
    use crate::{repositories, test};

    #[tokio::test]
    async fn test_fork_operations() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|test_dir| {
            async move {
                let original_repo_path = test_dir.join("original");
                let _original_repo = repositories::init(&original_repo_path)?;
                let forked_repo_path = test_dir.join("forked");

                // Create a directory and add a file to it
                let dir_path = original_repo_path.join("dir");
                oxen_fs::create_dir_all(&dir_path)?;
                let file_path = dir_path.join("test_file.txt");
                std::fs::write(file_path, "test file content")?;

                // Create a workspace directory and add a file to it
                let workspaces_path = original_repo_path.join(".oxen/workspaces");
                oxen_fs::create_dir_all(&workspaces_path)?;
                let workspace_file = workspaces_path.join("test_workspace.txt");
                std::fs::write(workspace_file, "test workspace content")?;

                start_fork(original_repo_path.clone(), forked_repo_path.clone())?;
                let mut current_status = "in_progress".to_string();
                let mut attempts = 0;
                const MAX_ATTEMPTS: u32 = 10; // 10 seconds timeout (10 * 1s)

                while current_status == "in_progress" && attempts < MAX_ATTEMPTS {
                    tokio::time::sleep(Duration::from_millis(1000)).await; // Wait for 1 second
                    current_status = match get_fork_status(&forked_repo_path) {
                        Ok(status) => status.status,
                        Err(e) => {
                            if let OxenError::ForkStatusNotFound(_) = e {
                                "in_progress".to_string()
                            } else {
                                return Err(e);
                            }
                        }
                    };
                    attempts += 1;
                }

                if attempts >= MAX_ATTEMPTS {
                    return Err(OxenError::basic_str("Fork operation timed out"));
                }

                let file_path = original_repo_path.clone().join("dir/test_file.txt");

                assert!(forked_repo_path.exists());
                // Verify that the content of the file is the same in both repos
                let new_file_path = forked_repo_path.join("dir/test_file.txt");
                let original_content = fs::read_to_string(&file_path)?;
                let mut retries = 10;
                let sleep_time = 100;
                let new_content = loop {
                    if new_file_path.exists() {
                        break fs::read_to_string(&new_file_path)?;
                    }

                    if retries == 0 {
                        return Err(OxenError::basic_str("File not found after retries"));
                    }

                    tokio::time::sleep(Duration::from_millis(sleep_time)).await;
                    retries -= 1;
                };

                assert_eq!(
                    original_content, new_content,
                    "The content of test_file.txt should be the same in both repositories"
                );

                // Verify that .oxen/workspaces was not copied
                let new_workspaces_path = forked_repo_path.join(".oxen/workspaces");
                assert!(
                    !new_workspaces_path.exists(),
                    ".oxen/workspaces should not be copied"
                );

                // Fork fails if repo exists
                let result = start_fork(original_repo_path.clone(), forked_repo_path.clone());
                assert!(
                    result.is_err(),
                    "Expected an error because the repo already exists."
                );

                Ok(())
            }
        })
        .await
    }
}
