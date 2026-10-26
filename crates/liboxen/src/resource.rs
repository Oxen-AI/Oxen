pub use crate::core::v_latest::resource::{
    parse_resource_from_path, parse_resource_from_path_async,
};

#[cfg(test)]
mod tests {
    use crate::test;
    use std::path::Path;

    use crate::error::OxenError;
    use crate::model::ParsedResource;
    use crate::repositories;
    use crate::resource;

    #[tokio::test]
    async fn test_parse_resource_from_path() -> Result<(), OxenError> {
        test::run_one_commit_local_repo_test_async(|repo| async move {
            let commit = repositories::commits::head_commit(&repo)?;
            let parse = |path: &Path| -> Result<ParsedResource, OxenError> {
                Ok(resource::parse_resource_from_path(&repo, path)?
                    .unwrap_or_else(|| panic!("{path:?} should resolve")))
            };

            let history = repositories::commits::list(&repo)?;
            assert_eq!(history.first(), Some(&commit), "one commit, the head");
            let resource = parse(Path::new(&format!(
                "{}/annotations/train/one_shot.csv",
                commit.id
            )))?;
            assert_eq!(resource.commit.map(|c| c.id), Some(commit.id.clone()));
            assert_eq!(resource.path, Path::new("annotations/train/one_shot.csv"));

            for branch_name in ["my-branch", "my/crazy/branch/name"] {
                let branch = repositories::branches::create_checkout(&repo, branch_name)?;
                let resource = parse(Path::new(&format!(
                    "{branch_name}/annotations/train/one_shot.csv"
                )))?;
                assert_eq!(resource.commit.map(|c| c.id), Some(branch.commit_id));
                assert_eq!(resource.path, Path::new("annotations/train/one_shot.csv"));
            }

            let branch = repositories::branches::create_checkout(&repo, "my_branch")?;
            let resource = parse(Path::new("my_branch"))?;
            assert_eq!(resource.commit.map(|c| c.id), Some(branch.commit_id));
            assert_eq!(
                resource.path,
                Path::new(""),
                "a bare branch name is the root dir"
            );

            assert_eq!(parse(Path::new("main/"))?.path, Path::new(""));

            let branch_name = "super/complex/branch-name/slashes";
            repositories::branches::create_checkout(&repo, branch_name)?;
            if !cfg!(windows) {
                assert_eq!(
                    parse(Path::new(&format!("{branch_name}/")))?.path,
                    Path::new("")
                );
                let path = Path::new(branch_name).join("folder-new");
                assert_eq!(parse(&path)?.path, Path::new("folder-new"));
                let path = format!("{branch_name}/folder/item.txt");
                assert_eq!(parse(Path::new(&path))?.path, Path::new("folder/item.txt"));
            }

            let branch_name = "feature/v2/experiment";
            let branch = repositories::branches::create_from_head(&repo, branch_name)?;
            let resource = parse(Path::new(branch_name))?;
            assert_eq!(resource.branch.map(|b| b.name), Some(branch.name));
            assert_eq!(
                resource.path,
                Path::new(""),
                "the branch name spans the whole path"
            );

            // Resolving an unknown identifier looks for a workspace before any exists.
            let unknown = Path::new("nonexistent-thing/file.txt");
            assert!(resource::parse_resource_from_path(&repo, unknown)?.is_none());
            assert!(
                resource::parse_resource_from_path_async(&repo, unknown)
                    .await?
                    .is_none()
            );

            let workspace = repositories::workspaces::create(&repo, &commit, "my-workspace", true)?;
            let resource = parse(Path::new(&format!("{}/data.csv", workspace.id)))?;
            assert!(resource.branch.is_none());
            assert!(resource.commit.is_none());
            assert_eq!(resource.workspace.map(|w| w.id), Some(workspace.id));
            assert_eq!(resource.path, Path::new("data.csv"));

            let shared_name = "shared-name";
            let branch = repositories::branches::create_from_head(&repo, shared_name)?;
            repositories::workspaces::create(&repo, &commit, shared_name, false)?;
            let resource = parse(Path::new(&format!("{shared_name}/data.csv")))?;
            assert!(
                resource.workspace.is_none(),
                "a branch wins over a workspace"
            );
            assert_eq!(resource.branch.map(|b| b.name), Some(branch.name));
            assert_eq!(resource.path, Path::new("data.csv"));

            repositories::branches::create_from_head(&repo, &commit.id)?;
            let path_str = format!("{}/some/file.txt", commit.id);
            let path = Path::new(&path_str);
            let resource = parse(path)?;
            assert!(resource.branch.is_none(), "a commit wins over a branch");
            assert_eq!(resource.commit.map(|c| c.id), Some(commit.id.clone()));
            assert_eq!(resource.path, Path::new("some/file.txt"));

            let async_ = resource::parse_resource_from_path_async(&repo, path)
                .await?
                .expect("the async path resolves what the sync path does");
            assert_eq!(async_.commit.map(|c| c.id), Some(commit.id));
            assert_eq!(async_.path, resource.path);

            Ok(())
        })
        .await
    }
}
