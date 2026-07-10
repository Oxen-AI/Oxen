use liboxen::api;
use liboxen::command::migrate::ALL_MIGRATIONS;
use liboxen::config::AuthConfig;
use liboxen::constants;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::util;
use liboxen::util::oxen_version::OxenVersion;

use colored::Colorize;

use std::path::{Component, Path, PathBuf};
use std::str::FromStr;

pub fn get_scheme_and_host_or_default() -> Result<(String, String), OxenError> {
    let config = AuthConfig::get_or_create()?;
    let mut default_host = (
        constants::DEFAULT_SCHEME.to_string(),
        constants::DEFAULT_HOST.to_string(),
    );
    if let Some(host) = config.default_host
        && !host.is_empty()
    {
        default_host.1 = host;
    }
    Ok(default_host)
}

pub fn get_scheme_and_host_from_repo(
    repo: &LocalRepository,
) -> Result<(String, String), OxenError> {
    if let Some(remote) = repo.remote() {
        let host_and_scheme = api::client::get_scheme_and_host_from_url(&remote.url)?;
        return Ok(host_and_scheme);
    }

    get_scheme_and_host_or_default()
}

pub async fn check_remote_version(
    scheme: impl AsRef<str>,
    host: impl AsRef<str>,
) -> Result<(), OxenError> {
    // Do the version check in the dispatch because it's only really the CLI that needs to do it
    match api::client::oxen_version::get_remote_version(scheme.as_ref(), host.as_ref()).await {
        Ok(remote_version) => {
            let local_version: &str = constants::OXEN_VERSION;

            if remote_version != local_version {
                let warning = format!("Warning: 🐂 Oxen remote version mismatch.\n\nCLI Version: {local_version}\nServer Version: {remote_version}\n\nPlease visit https://docs.oxen.ai/getting-started/install for installation instructions.\n").yellow();
                eprintln!("{warning}");
            }
        }
        Err(err) => {
            // Don't fully print in case they are just off network and working locally
            log::debug!("Err checking remote version:\n{err}")
        }
    }
    Ok(())
}

pub async fn check_remote_version_blocking(
    scheme: impl AsRef<str>,
    host: impl AsRef<str>,
) -> Result<(), OxenError> {
    match api::client::oxen_version::get_min_oxen_version(scheme.as_ref(), host.as_ref()).await {
        Ok(remote_version) => {
            let local_version: &str = constants::OXEN_VERSION;
            let min_oxen_version = OxenVersion::from_str(&remote_version)?;
            let local_oxen_version = OxenVersion::from_str(local_version)?;

            if local_oxen_version < min_oxen_version {
                return Err(OxenError::OxenUpdateRequired(format!(
                    "Error: Oxen CLI out of date. Pushing to OxenHub requires version >= {min_oxen_version:?}, found version {local_oxen_version:?}.\n\nVisit https://docs.oxen.ai/getting-started/intro for update instructions.\n\nOn Mac:\n\n  brew update\n  brew upgrade oxen\n\nOn Ubuntu:\n\n  For x86-64:\n    wget https://github.com/Oxen-AI/Oxen/releases/latest/download/oxen-linux-x86_64.deb\n    sudo dpkg -i oxen-linux-x86_64.deb\n\n  For ARM64:\n    wget https://github.com/Oxen-AI/Oxen/releases/latest/download/oxen-linux-arm64.deb\n    sudo dpkg -i oxen-linux-arm64.deb"
                ).into()));
            }
        }
        Err(err) => {
            return Err(err);
        }
    }
    Ok(())
}

pub fn check_repo_migration_needed(repo: &LocalRepository) -> Result<(), OxenError> {
    let migrations_needed = {
        let mut migrations_needed = vec![];
        for migration in ALL_MIGRATIONS {
            if migration.is_needed(repo)? {
                migrations_needed.push(migration);
            }
        }
        migrations_needed
    };

    if migrations_needed.is_empty() {
        return Ok(());
    }
    let warning = "\nWarning: 🐂 This repo requires a migration to the latest Oxen version. \n\nPlease run the following to update:".to_string().yellow();
    eprintln!("{warning}\n");
    for migration in migrations_needed {
        eprintln!(
            "{}",
            format!("  oxen migrate up {} .\n", migration.name()).yellow()
        );
    }
    Err(OxenError::MigrationRequired(
        "Error: Migration required".to_string().into(),
    ))
}

/// Resolves a user-supplied path (current-dir-relative or absolute) to a repository-relative path.
pub fn path_relative_to_repo(repo: &LocalRepository, path: &Path) -> Result<PathBuf, OxenError> {
    let current_dir = std::env::current_dir()?;
    path_relative_to_repo_from(&current_dir, &repo.path, path)
}

/// Maps `path` to a `repo_path`-relative path, resolving `.`/`..` against `current_dir`.
fn path_relative_to_repo_from(
    current_dir: &Path,
    repo_path: &Path,
    path: &Path,
) -> Result<PathBuf, OxenError> {
    let joined = current_dir.join(path);
    // Resolve `.`/`..` lexically; canonicalizing would require the file to exist on disk.
    let mut normalized = PathBuf::new();
    for component in joined.components() {
        match component {
            Component::ParentDir => {
                normalized.pop();
            }
            Component::CurDir => {}
            component => normalized.push(component),
        }
    }
    util::fs::path_relative_to_dir(normalized, repo_path)
}

#[cfg(test)]
mod tests {
    use super::path_relative_to_repo_from;
    use std::path::{Path, PathBuf};

    fn assert_repo_relative(current_dir: &str, repo: &str, input: &str, expected: &str) {
        let got =
            path_relative_to_repo_from(Path::new(current_dir), Path::new(repo), Path::new(input))
                .expect("path resolution should succeed for valid inputs");
        assert_eq!(
            got,
            PathBuf::from(expected),
            "cwd={current_dir} repo={repo} input={input}"
        );
    }

    #[test]
    fn maps_normal_relative_and_absolute_paths() {
        // Relative input from the repo root.
        assert_repo_relative("/repo", "/repo", "data.txt", "data.txt");
        assert_repo_relative("/repo", "/repo", "a/b.txt", "a/b.txt");
        // Relative input from a subdirectory picks up the subdirectory prefix.
        assert_repo_relative("/repo/sub", "/repo", "data.txt", "sub/data.txt");
        assert_repo_relative(
            "/repo/sub",
            "/repo",
            "nested/data.txt",
            "sub/nested/data.txt",
        );
        // Absolute input maps straight to repo-relative regardless of the current dir.
        assert_repo_relative("/repo/sub", "/repo", "/repo/a/b.txt", "a/b.txt");
    }

    #[test]
    fn resolves_dot_and_parent_dir_lexically() {
        // `.` is a no-op.
        assert_repo_relative("/repo/sub", "/repo", "./data.txt", "sub/data.txt");
        // `..` climbs out of the current subdirectory.
        assert_repo_relative("/repo/sub", "/repo", "../data.txt", "data.txt");
        // Multiple `..`.
        assert_repo_relative("/repo/a/b", "/repo", "../../data.txt", "data.txt");
        // `..` mixed with normal segments.
        assert_repo_relative("/repo/sub", "/repo", "../other/data.txt", "other/data.txt");
        // `..` embedded in an absolute input.
        assert_repo_relative("/repo/sub", "/repo", "/repo/sub/../data.txt", "data.txt");
    }
}
