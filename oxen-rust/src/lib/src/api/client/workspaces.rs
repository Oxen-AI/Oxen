pub mod changes;
pub mod commits;
pub mod data_frames;
pub mod files;

use std::path::Path;

pub use commits::commit;

use crate::api;
use crate::api::client;
use crate::error::OxenError;
use crate::model::RemoteRepository;
use crate::view::workspaces::{ListWorkspaceResponseView, WorkspaceResponseWithStatus};
use crate::view::workspaces::{NewWorkspace, WorkspaceResponse};
use crate::view::{StatusMessage, WorkspaceResponseView};

pub async fn list(remote_repo: &RemoteRepository) -> Result<Vec<WorkspaceResponse>, OxenError> {
    let url = api::endpoint::url_from_repo(remote_repo, "/workspaces")?;
    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<ListWorkspaceResponseView, serde_json::Error> =
        serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val.workspaces),
        Err(err) => Err(OxenError::basic_str(format!(
            "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn get(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
) -> Result<Option<WorkspaceResponse>, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let url = api::endpoint::url_from_repo(remote_repo, &format!("/workspaces/{workspace_id}"))?;
    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body_result = client::parse_json_body(&url, res).await;

    let workspace = body_result
        .ok()
        .and_then(|body| serde_json::from_str::<WorkspaceResponseView>(&body).ok())
        .map(|val| val.workspace);

    Ok(workspace)
}

pub async fn get_by_name(
    remote_repo: &RemoteRepository,
    name: impl AsRef<str>,
) -> Result<Option<WorkspaceResponse>, OxenError> {
    let name = name.as_ref();
    let url = api::endpoint::url_from_repo(remote_repo, &format!("/workspaces?name={name}"))?;
    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<ListWorkspaceResponseView, serde_json::Error> =
        serde_json::from_str(&body);
    match response {
        Ok(val) => {
            if val.workspaces.len() == 1 {
                Ok(Some(val.workspaces[0].clone()))
            } else if val.workspaces.is_empty() {
                Ok(None)
            } else {
                Err(OxenError::basic_str(format!(
                    "expected 1 workspace, got {}",
                    val.workspaces.len()
                )))
            }
        }
        Err(err) => Err(OxenError::basic_str(format!(
            "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn create(
    remote_repo: &RemoteRepository,
    branch_name: impl AsRef<str>,
    workspace_id: impl AsRef<str>,
) -> Result<WorkspaceResponseWithStatus, OxenError> {
    create_with_path(remote_repo, branch_name, workspace_id, Path::new("/"), None).await
}

pub async fn create_with_name(
    remote_repo: &RemoteRepository,
    branch_name: impl AsRef<str>,
    workspace_id: impl AsRef<str>,
    workspace_name: impl AsRef<str>,
) -> Result<WorkspaceResponseWithStatus, OxenError> {
    let workspace_name = workspace_name.as_ref().to_string();
    create_with_path(
        remote_repo,
        branch_name,
        workspace_id,
        Path::new("/"),
        Some(workspace_name),
    )
    .await
}

pub async fn create_with_path(
    remote_repo: &RemoteRepository,
    branch_name: impl AsRef<str>,
    workspace_id: impl AsRef<str>,
    path: impl AsRef<Path>,
    workspace_name: Option<String>,
) -> Result<WorkspaceResponseWithStatus, OxenError> {
    let branch_name = branch_name.as_ref();
    let workspace_id = workspace_id.as_ref();
    let path = path.as_ref();
    let url = api::endpoint::url_from_repo(remote_repo, "/workspaces")?;
    log::debug!("create workspace {}\n", url);

    let body = NewWorkspace {
        branch_name: branch_name.to_string(),
        workspace_id: workspace_id.to_string(),
        // These two are needed for the oxen hub right now, ignored by the server
        resource_path: Some(path.to_str().unwrap().to_string()),
        entity_type: Some("user".to_string()),
        name: workspace_name,
        force: Some(true),
    };

    let client = client::new_for_url(&url)?;
    let res = client.put(&url).json(&body).send().await?;

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("create workspace got body: {}", body);
    let response: Result<WorkspaceResponseView, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(WorkspaceResponseWithStatus {
            id: val.workspace.id,
            name: val.workspace.name,
            commit: val.workspace.commit,
            status: val.status.status_message,
        }),
        Err(err) => Err(OxenError::basic_str(format!(
            "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn delete(
    remote_repo: &RemoteRepository,
    workspace_id: impl AsRef<str>,
) -> Result<WorkspaceResponse, OxenError> {
    let workspace_id = workspace_id.as_ref();
    let url = api::endpoint::url_from_repo(remote_repo, &format!("/workspaces/{workspace_id}"))?;
    log::debug!("delete workspace {}\n", url);

    let client = client::new_for_url(&url)?;
    let res = client.delete(&url).send().await?;

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("delete workspace got body: {}", body);
    let response: Result<WorkspaceResponseView, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(val) => Ok(val.workspace),
        Err(err) => Err(OxenError::basic_str(format!(
            "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

pub async fn clear(remote_repo: &RemoteRepository) -> Result<(), OxenError> {
    let url = api::endpoint::url_from_repo(remote_repo, "/workspaces")?;
    log::debug!("clear workspaces {}\n", url);

    let client = client::new_for_url(&url)?;
    let res = client.delete(&url).send().await?;

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("delete workspace got body: {}", body);
    let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(_) => Ok(()),
        Err(err) => Err(OxenError::basic_str(format!(
            "error parsing response from {url}\n\nErr {err:?} \n\n{body}"
        ))),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::api;
    use crate::command;
    use crate::constants;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::error::OxenError;
    use crate::model::NewCommitBody;
    use crate::opts::DFOpts;
    use crate::repositories;
    use crate::test;

    #[tokio::test]
    async fn test_create_workspace() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            let branch_name = "main";
            let workspace_id = "test_workspace_id";
            let workspace = create(&remote_repo, branch_name, workspace_id).await?;

            assert_eq!(workspace.id, workspace_id);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_create_workspace_with_name() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            let branch_name = "main";
            let workspace_id = "test_workspace_id";
            let workspace_name = "test_workspace_name";
            let workspace =
                create_with_name(&remote_repo, branch_name, workspace_id, workspace_name).await?;

            assert_eq!(workspace.id, workspace_id);
            assert_eq!(workspace.name, Some(workspace_name.to_string()));

            let workspace = get(&remote_repo, &workspace_id).await?;
            assert!(workspace.is_some());
            assert_eq!(
                workspace.as_ref().unwrap().name,
                Some(workspace_name.to_string())
            );
            assert_eq!(workspace.as_ref().unwrap().id, workspace_id);

            let workspace = get_by_name(&remote_repo, &workspace_name).await?;
            assert!(workspace.is_some());
            assert_eq!(
                workspace.as_ref().unwrap().name,
                Some(workspace_name.to_string())
            );
            assert_eq!(workspace.as_ref().unwrap().id, workspace_id);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_workspace_by_name() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            let branch_name = "main";
            let workspace_id = "test_workspace_id";
            let workspace_name = "test_workspace_name";
            create_with_name(&remote_repo, branch_name, workspace_id, workspace_name).await?;

            // Create a second workspace with a different name
            let workspace_id2 = "test_workspace_id2";
            let workspace_name2 = "test_workspace_name2";
            create_with_name(&remote_repo, branch_name, workspace_id2, workspace_name2).await?;

            let workspace = get_by_name(&remote_repo, &workspace_name).await?;
            assert!(workspace.is_some());
            assert_eq!(
                workspace.as_ref().unwrap().name,
                Some(workspace_name.to_string())
            );
            assert_eq!(workspace.as_ref().unwrap().id, workspace_id);

            let workspace2 = get_by_name(&remote_repo, &workspace_name2).await?;
            assert!(workspace2.is_some());
            assert_eq!(
                workspace2.as_ref().unwrap().name,
                Some(workspace_name2.to_string())
            );
            assert_eq!(workspace2.as_ref().unwrap().id, workspace_id2);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_workspace_by_name_does_not_exist() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            let workspace_name = "name_does_not_exist";

            let workspace = get_by_name(&remote_repo, &workspace_name).await?;
            assert!(workspace.is_none());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_get_workspace_by_id_does_not_exist() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            let workspace_id = "id_does_not_exist";

            let workspace = get(&remote_repo, &workspace_id).await?;
            assert!(workspace.is_none());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_clear_workspaces() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            // Create 10 workspaces
            for i in 0..10 {
                create(
                    &remote_repo,
                    DEFAULT_BRANCH_NAME,
                    &format!("test_workspace_{i}"),
                )
                .await?;
            }

            // Clear them
            clear(&remote_repo).await?;

            // Check they are gone
            let workspaces = list(&remote_repo).await?;
            assert_eq!(workspaces.len(), 0);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_list_workspaces() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            let branch_name = "main";
            create(&remote_repo, branch_name, "test_workspace_id").await?;
            create(&remote_repo, branch_name, "test_workspace_id2").await?;

            let workspaces = list(&remote_repo).await?;
            assert_eq!(workspaces.len(), 2);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_list_empty_workspaces() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|_local_repo, remote_repo| async move {
            let workspaces = list(&remote_repo).await?;
            assert_eq!(workspaces.len(), 0);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_delete_workspace() -> Result<(), OxenError> {
        test::run_readme_remote_repo_test(|_local_repo, remote_repo| async move {
            let branch_name = "main";
            let workspace_id = "test_workspace_id";
            let workspace = create(&remote_repo, branch_name, workspace_id).await?;

            assert_eq!(workspace.id, workspace_id);

            let res = delete(&remote_repo, workspace_id).await;
            assert!(res.is_ok());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_commit_fails_if_schema_changed() -> Result<(), OxenError> {
        // Skip if on windows
        if std::env::consts::OS == "windows" {
            return Ok(());
        }

        test::run_training_data_fully_sync_remote(|_, remote_repo| async move {
            let remote_repo_copy = remote_repo.clone();

            test::run_empty_dir_test_async(|repo_dir| async move {
                let cloned_repo =
                    repositories::clone_url(&remote_repo.remote.url, &repo_dir.join("new_repo"))
                        .await?;

                // Remote stage row
                let path = test::test_nlp_classification_csv();

                // Create workspace
                let workspace_id = "my_workspace";
                api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, &workspace_id)
                    .await?;

                // Index the dataset
                repositories::workspaces::df::index(&cloned_repo, workspace_id, &path).await?;

                log::debug!("the path in question is {:?}", path);
                let mut opts = DFOpts::empty();

                opts.add_row =
                    Some("{\"text\": \"I am a new row\", \"label\": \"neutral\"}".to_string());
                repositories::workspaces::df(&cloned_repo, workspace_id, &path, opts).await?;

                // Local add col
                let full_path = cloned_repo.path.join(path);
                let mut opts = DFOpts::empty();
                opts.add_col = Some("is_something:n/a:str".to_string());
                opts.output = Some(full_path.to_path_buf()); // write back to same path
                command::df(&full_path, opts).await?;
                repositories::add(&cloned_repo, &full_path).await?;

                // Commit and push the changed schema
                repositories::commit(&cloned_repo, "Changed the schema 😇")?;
                repositories::push(&cloned_repo).await?;

                // Try to commit the remote changes, should fail
                let body = NewCommitBody {
                    message: "Remotely committing".to_string(),
                    author: "Test User".to_string(),
                    email: "test@oxen.ai".to_string(),
                };
                let result = api::client::workspaces::commit(
                    &remote_repo,
                    DEFAULT_BRANCH_NAME,
                    workspace_id,
                    &body,
                )
                .await;
                assert!(result.is_err());

                // Status should have one modified file
                let remote_status = api::client::workspaces::changes::list(
                    &remote_repo,
                    &workspace_id,
                    Path::new(""),
                    constants::DEFAULT_PAGE_NUM,
                    constants::DEFAULT_PAGE_SIZE,
                )
                .await?;
                assert_eq!(remote_status.modified_files.entries.len(), 1);

                Ok(())
            })
            .await?;

            Ok(remote_repo_copy)
        })
        .await
    }

    #[tokio::test]
    async fn test_remote_commit_staging_behind_main() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            // Create branch behind-main off main
            let new_branch = "behind-main";
            let main_branch = "main";

            let main_path = "images/folder";
            let workspace =
                api::client::workspaces::create(&remote_repo, main_branch, "test_workspace")
                    .await?;
            let identifier = workspace.id;

            api::client::branches::create_from_branch(&remote_repo, new_branch, main_branch)
                .await?;

            // Advance head on main branch, leave behind-main behind
            let path = test::test_img_file();
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &identifier,
                main_path,
                path,
            )
            .await;
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Add to main".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            api::client::workspaces::commit(&remote_repo, main_branch, &identifier, &body).await?;

            let workspace =
                api::client::workspaces::create(&remote_repo, new_branch, "test_workspace").await?;
            let identifier = workspace.id;

            // Add a file to behind-main
            let image_path = test::test_1k_parquet();
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &identifier,
                main_path,
                image_path,
            )
            .await;
            assert!(result.is_ok());

            // Make a commit to behind-main
            let body = NewCommitBody {
                message: "Add behind main".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            api::client::workspaces::commit(&remote_repo, new_branch, &identifier, &body).await?;

            let workspace =
                api::client::workspaces::create(&remote_repo, new_branch, "test_workspace").await?;
            let identifier = workspace.id;

            // Add file at images/folder to behind-main, committed to main
            let image_path = test::test_100_parquet();
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &identifier,
                main_path,
                image_path,
            )
            .await;
            assert!(result.is_ok());

            // Check status: if valid, there should be an entry here for the file at images/folder
            let page_num = constants::DEFAULT_PAGE_NUM;
            let page_size = constants::DEFAULT_PAGE_SIZE;
            let path = Path::new("");

            let remote_status = api::client::workspaces::changes::list(
                &remote_repo,
                &identifier,
                path,
                page_num,
                page_size,
            )
            .await?;

            assert_eq!(remote_status.added_files.entries.len(), 1);
            assert_eq!(remote_status.added_files.total_entries, 1);

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_not_named_workspaces_closing_after_commit() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let workspace_id = "test_workspace_id";
            api::client::workspaces::create(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id)
                .await?;
            let path = test::test_img_file();
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &&workspace_id,
                "",
                path,
            )
            .await;
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Add to main".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            api::client::workspaces::commit(&remote_repo, DEFAULT_BRANCH_NAME, workspace_id, &body)
                .await?;
            let get_result = api::client::workspaces::get(&remote_repo, &workspace_id).await?;

            assert!(get_result.is_none());

            Ok(remote_repo)
        })
        .await
    }

    #[tokio::test]
    async fn test_named_workspaces_not_closing_after_commit() -> Result<(), OxenError> {
        test::run_remote_repo_test_bounding_box_csv_pushed(|_local_repo, remote_repo| async move {
            let workspace_name = "test_workspace_name";
            let workspace_id = "test_workspace_id";
            api::client::workspaces::create_with_name(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                workspace_id,
                workspace_name,
            )
            .await?;
            let path = test::test_img_file();
            let result = api::client::workspaces::files::upload_single_file(
                &remote_repo,
                &workspace_name,
                "",
                path,
            )
            .await;
            assert!(result.is_ok());

            let body = NewCommitBody {
                message: "Add to main".to_string(),
                author: "Test User".to_string(),
                email: "test@oxen.ai".to_string(),
            };
            api::client::workspaces::commit(
                &remote_repo,
                DEFAULT_BRANCH_NAME,
                workspace_name,
                &body,
            )
            .await?;
            let workspace = api::client::workspaces::get(&remote_repo, &workspace_name).await?;
            assert!(workspace.is_some());
            assert_eq!(workspace.as_ref().unwrap().id, workspace_id);
            Ok(remote_repo)
        })
        .await
    }
}
