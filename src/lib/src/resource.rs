use crate::core::refs::RefReader;
use crate::core::v0_10_0::index::CommitReader;

use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, ParsedResource};
use crate::repositories;

use std::path::{Path, PathBuf};

pub fn parse_resource_from_path(
    repo: &LocalRepository,
    path: &Path,
) -> Result<Option<ParsedResource>, OxenError> {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => parse_resource_from_path_v0_10_0(repo, path),
        MinOxenVersion::V0_19_0 => parse_resource_from_path_v0_19_0(repo, path),
    }
}

pub fn parse_resource_from_path_v0_19_0(
    repo: &LocalRepository,
    path: &Path,
) -> Result<Option<ParsedResource>, OxenError> {
    let mut components = path.components().collect::<Vec<_>>();

    // See if the first component is the commit id
    log::debug!(
        "parse_resource_from_path_v0_19_0 looking for commit id in path {:?}",
        path
    );

    if let Some(first_component) = components.first() {
        let base_path: &Path = first_component.as_ref();
        let maybe_commit_id = base_path.to_str().unwrap();
        // log::debug!("parse_resource looking at component {}", maybe_commit_id);
        if let Some(commit) = repositories::commits::get_by_id(repo, maybe_commit_id)? {
            let mut file_path = PathBuf::new();
            for (i, component) in components.iter().enumerate() {
                if i != 0 {
                    let component_path: &Path = component.as_ref();
                    file_path = file_path.join(component_path);
                }
            }
            log::debug!(
                "parse_resource_from_path_v0_19_0 got commit.id [{}] and filepath [{:?}]",
                commit.id,
                file_path
            );
            return Ok(Some(ParsedResource {
                commit: Some(commit.clone()),
                branch: None,
                path: file_path,
                version: PathBuf::from(commit.id.to_string()),
                resource: path.to_owned(),
            }));
        }
    }

    // See if the component has a valid branch name in it
    // log::debug!("parse_resource looking for branch in path {:?}", path);
    let ref_reader = RefReader::new(repo)?;
    let mut file_path = PathBuf::new();
    while let Some(component) = components.pop() {
        let component_path: &Path = component.as_ref();
        if file_path == PathBuf::new() {
            file_path = component_path.to_path_buf();
        } else {
            file_path = component_path.join(file_path);
        }

        log::debug!(
            "parse_resource_from_path_v0_19_0 got file path [{:?}] with {} remaining components",
            file_path,
            components.len()
        );
        // if we have no components, looking at base dir within that branch
        if components.is_empty() {
            let branch_name = file_path.to_str().unwrap();
            if let Some(branch) = ref_reader.get_branch_by_name(branch_name)? {
                // log::debug!(
                //     "parse_resource got branch [{}] with no file path",
                //     branch_name
                // );

                let commit = repositories::commits::get_by_id(repo, &branch.commit_id)?.unwrap();
                file_path = PathBuf::from("");
                return Ok(Some(ParsedResource {
                    commit: Some(commit),
                    branch: Some(branch.clone()),
                    path: file_path,
                    version: PathBuf::from(branch.name),
                    resource: path.to_owned(),
                }));
            } else {
                return Ok(None);
            }
        }

        let mut branch_path = PathBuf::new();
        for component in components.iter() {
            let component_path: &Path = component.as_ref();
            branch_path = branch_path.join(component_path);
        }

        let branch_name = branch_path.to_str().unwrap();
        log::debug!(
            "parse_resource_from_path_v0_19_0 looking for branch [{}]",
            branch_name
        );
        if let Some(branch) = ref_reader.get_branch_by_name(branch_name)? {
            log::debug!(
                "parse_resource_from_path_v0_19_0 got branch [{}] and filepath [{:?}]",
                branch_name,
                file_path
            );

            let commit = repositories::commits::get_by_id(repo, &branch.commit_id)?.unwrap();
            return Ok(Some(ParsedResource {
                commit: Some(commit),
                branch: Some(branch.clone()),
                path: file_path,
                version: PathBuf::from(branch.name),
                resource: path.to_owned(),
            }));
        }
    }

    Ok(None)
}

pub fn parse_resource_from_path_v0_10_0(
    repo: &LocalRepository,
    path: &Path,
) -> Result<Option<ParsedResource>, OxenError> {
    let mut components = path.components().collect::<Vec<_>>();
    let commit_reader = CommitReader::new(repo)?;

    // See if the first component is the commit id
    log::debug!(
        "parse_resource_from_path_v0_10_0 looking for commit id in path {:?}",
        path
    );

    if let Some(first_component) = components.first() {
        let base_path: &Path = first_component.as_ref();
        let maybe_commit_id = base_path.to_str().unwrap();
        // log::debug!("parse_resource looking at component {}", maybe_commit_id);
        if let Some(commit) = commit_reader.get_commit_by_id(maybe_commit_id)? {
            let mut file_path = PathBuf::new();
            for (i, component) in components.iter().enumerate() {
                if i != 0 {
                    let component_path: &Path = component.as_ref();
                    file_path = file_path.join(component_path);
                }
            }
            log::debug!(
                "parse_resource_from_path_v0_10_0 got commit.id [{}] and filepath [{:?}]",
                commit.id,
                file_path
            );
            return Ok(Some(ParsedResource {
                commit: Some(commit.clone()),
                branch: None,
                path: file_path,
                version: PathBuf::from(commit.id.to_string()),
                resource: path.to_owned(),
            }));
        }
    }

    // See if the component has a valid branch name in it
    // log::debug!("parse_resource looking for branch in path {:?}", path);
    let ref_reader = RefReader::new(repo)?;
    let mut file_path = PathBuf::new();
    while let Some(component) = components.pop() {
        let component_path: &Path = component.as_ref();
        if file_path == PathBuf::new() {
            file_path = component_path.to_path_buf();
        } else {
            file_path = component_path.join(file_path);
        }

        log::debug!(
            "parse_resource_from_path_v0_10_0 got file path [{:?}] with {} remaining components",
            file_path,
            components.len()
        );
        // if we have no components, looking at base dir within that branch
        if components.is_empty() {
            let branch_name = file_path.to_str().unwrap();
            if let Some(branch) = ref_reader.get_branch_by_name(branch_name)? {
                // log::debug!(
                //     "parse_resource got branch [{}] with no file path",
                //     branch_name
                // );

                let commit = commit_reader.get_commit_by_id(&branch.commit_id)?.unwrap();
                file_path = PathBuf::from("");
                return Ok(Some(ParsedResource {
                    commit: Some(commit),
                    branch: Some(branch.clone()),
                    path: file_path,
                    version: PathBuf::from(branch.name),
                    resource: path.to_owned(),
                }));
            } else {
                return Ok(None);
            }
        }

        let mut branch_path = PathBuf::new();
        for component in components.iter() {
            let component_path: &Path = component.as_ref();
            branch_path = branch_path.join(component_path);
        }

        let branch_name = branch_path.to_str().unwrap();
        log::debug!(
            "parse_resource_from_path_v0_10_0 looking for branch [{}]",
            branch_name
        );
        if let Some(branch) = ref_reader.get_branch_by_name(branch_name)? {
            log::debug!(
                "parse_resource_from_path_v0_10_0 got branch [{}] and filepath [{:?}]",
                branch_name,
                file_path
            );

            let commit = commit_reader.get_commit_by_id(&branch.commit_id)?.unwrap();
            return Ok(Some(ParsedResource {
                commit: Some(commit),
                branch: Some(branch.clone()),
                path: file_path,
                version: PathBuf::from(branch.name),
                resource: path.to_owned(),
            }));
        }
    }

    Ok(None)
}

/// Pass in a branch name and maybe get a commit id back
pub fn maybe_get_commit_id_from_branch_name<S: AsRef<str>>(
    repo: &LocalRepository,
    commit_id_or_branch_name: S,
) -> Result<Option<String>, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    ref_reader.get_commit_id_for_branch(commit_id_or_branch_name.as_ref())
}

/// Pass in a commit id or a branch name and resolve it to a
pub fn maybe_get_commit<S: AsRef<str>>(
    repo: &LocalRepository,
    commit_id_or_branch_name: S,
) -> Result<Option<Commit>, OxenError> {
    let commit_reader = CommitReader::new(repo)?;
    if let Some(commit) = commit_reader.get_commit_by_id(&commit_id_or_branch_name)? {
        return Ok(Some(commit));
    }

    match maybe_get_commit_id_from_branch_name(repo, &commit_id_or_branch_name) {
        Ok(Some(commit_id)) => commit_reader.get_commit_by_id(commit_id),
        Ok(None) => Err(OxenError::local_revision_not_found(
            commit_id_or_branch_name.as_ref(),
        )),
        Err(err) => Err(err),
    }
}

pub fn get_commit_or_head<S: AsRef<str>>(
    repo: &LocalRepository,
    commit_id_or_branch_name: Option<S>,
) -> Result<Commit, OxenError> {
    if commit_id_or_branch_name.is_none() {
        return repositories::commits::head_commit(repo);
    }

    match maybe_get_commit(repo, commit_id_or_branch_name.unwrap().as_ref()) {
        Ok(Some(commit)) => Ok(commit),
        _ => repositories::commits::head_commit(repo),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::error::OxenError;
    use crate::repositories;
    use crate::resource;

    #[test]
    fn test_parse_resource_for_commit() -> Result<(), OxenError> {
        crate::test::run_training_data_repo_test_fully_committed(|repo| {
            let history = repositories::commits::list(&repo)?;
            let commit = history.first().unwrap();
            let path_str = format!("{}/annotations/train/one_shot.csv", commit.id);
            let path = Path::new(&path_str);

            match resource::parse_resource_from_path(&repo, path) {
                Ok(Some(resource)) => {
                    assert_eq!(commit.id, resource.commit.unwrap().id);
                    assert_eq!(resource.path, Path::new("annotations/train/one_shot.csv"));
                }
                _ => {
                    panic!("Should return a commit");
                }
            }

            Ok(())
        })
    }

    #[test]
    fn test_parse_resource_for_branch() -> Result<(), OxenError> {
        crate::test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "my-branch";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;

            let path_str = format!("{branch_name}/annotations/train/one_shot.csv");
            let path = Path::new(&path_str);

            match resource::parse_resource_from_path(&repo, path) {
                Ok(Some(resource)) => {
                    println!("Got branch: {branch:?} -> {path:?}");
                    assert_eq!(branch.commit_id, resource.commit.unwrap().id);
                    assert_eq!(resource.path, Path::new("annotations/train/one_shot.csv"));
                }
                _ => {
                    panic!("Should return a branch");
                }
            }

            Ok(())
        })
    }

    #[test]
    fn test_parse_resource_for_long_branch_name() -> Result<(), OxenError> {
        crate::test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "my/crazy/branch/name";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;

            let path_str = format!("{branch_name}/annotations/train/one_shot.csv");
            let path = Path::new(&path_str);

            match resource::parse_resource_from_path(&repo, path) {
                Ok(Some(resource)) => {
                    println!("Got branch: {branch:?} -> {path:?}");
                    assert_eq!(branch.commit_id, resource.commit.unwrap().id);
                    assert_eq!(resource.path, Path::new("annotations/train/one_shot.csv"));
                }
                _ => {
                    panic!("Should return a branch");
                }
            }

            Ok(())
        })
    }

    #[test]
    fn test_parse_resource_for_branch_base_dir() -> Result<(), OxenError> {
        crate::test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "my_branch";
            let branch = repositories::branches::create_checkout(&repo, branch_name)?;

            let path_str = branch_name.to_string();
            let path = Path::new(&path_str);

            match resource::parse_resource_from_path(&repo, path) {
                Ok(Some(resource)) => {
                    println!("Got branch: {branch:?} -> {path:?}");
                    assert_eq!(branch.commit_id, resource.commit.unwrap().id);
                    assert_eq!(resource.path, Path::new(""));
                }
                _ => {
                    panic!("Should return a branch");
                }
            }

            Ok(())
        })
    }

    #[test]
    fn test_parse_resource_from_path_root_dir() -> Result<(), OxenError> {
        crate::test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "main";
            // let branch = repositories::branches::create_checkout(&repo, branch_name)?;

            let path_str = format!("{branch_name}/");
            let path = Path::new(&path_str);

            match resource::parse_resource_from_path(&repo, path) {
                Ok(Some(resource)) => {
                    assert_eq!(resource.path, Path::new(""))
                }
                _ => {
                    panic!("Should return a parsed resource");
                }
            }

            Ok(())
        })
    }

    #[test]
    fn test_parse_resource_from_path_root_dir_complicated_branch() -> Result<(), OxenError> {
        crate::test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "super/complex/branch-name/slashes";
            let _branch = repositories::branches::create_checkout(&repo, branch_name)?;

            let path_str = format!("{branch_name}/");
            let path = Path::new(&path_str);

            if !cfg!(windows) {
                // skip on windows, running on linux
                match resource::parse_resource_from_path(&repo, path) {
                    Ok(Some(resource)) => {
                        assert_eq!(resource.path, Path::new(""))
                    }
                    _ => {
                        panic!("Should return a parsed resource");
                    }
                }
            }

            Ok(())
        })
    }

    #[test]
    fn test_parse_resource_from_path_nonroot_complicated_branch() -> Result<(), OxenError> {
        crate::test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "super/complex/branch-name/slashes";
            let _branch = repositories::branches::create_checkout(&repo, branch_name)?;

            let path = Path::new(branch_name).join("folder-new");

            // should be running server on linux so skip windows
            if !cfg!(windows) {
                match resource::parse_resource_from_path(&repo, &path) {
                    Ok(Some(resource)) => {
                        assert_eq!(resource.path, Path::new("folder-new"))
                    }
                    _ => {
                        panic!("Should return a parsed resource");
                    }
                }
            }

            Ok(())
        })
    }

    #[test]
    fn test_parse_resource_from_path_with_file() -> Result<(), OxenError> {
        crate::test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "super/complex/branch-name/slashes";
            let _branch = repositories::branches::create_checkout(&repo, branch_name)?;

            let path_str = format!("{branch_name}/folder/item.txt");
            let path = Path::new(&path_str);

            if !cfg!(windows) {
                match resource::parse_resource_from_path(&repo, path) {
                    Ok(Some(resource)) => {
                        assert_eq!(resource.path, Path::new("folder/item.txt"))
                    }
                    _ => {
                        panic!("Should return a parsed resource");
                    }
                }
            }

            Ok(())
        })
    }
}
