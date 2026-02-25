use liboxen::api;
use liboxen::command::migrate::Migrate;
use liboxen::config::AuthConfig;
use liboxen::constants;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::util::oxen_version::OxenVersion;

use colored::Colorize;

use std::collections::HashMap;
use std::str::FromStr;

use liboxen::command::migrate::AddChildCountsToNodesMigration;

pub fn get_scheme_and_host_or_default() -> Result<(String, String), OxenError> {
    let config = AuthConfig::get_or_create()?;
    let mut default_host = (
        constants::DEFAULT_SCHEME.to_string(),
        constants::DEFAULT_HOST.to_string(),
    );
    if let Some(host) = config.default_host {
        if !host.is_empty() {
            default_host.1 = host;
        }
    }
    Ok(default_host)
}

pub fn get_scheme_and_host_from_repo(
    repo: &LocalRepository,
) -> Result<(String, String), OxenError> {
    if let Some(remote) = repo.remote() {
        let host_and_scheme = api::client::get_scheme_and_host_from_url(remote.url)?;
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

pub fn migrations() -> HashMap<String, Box<dyn Migrate>> {
    let mut map: HashMap<String, Box<dyn Migrate>> = HashMap::new();
    map.insert(
        AddChildCountsToNodesMigration.name().to_string(),
        Box::new(AddChildCountsToNodesMigration),
    );
    map
}

pub fn check_repo_migration_needed(repo: &LocalRepository) -> Result<(), OxenError> {
    let migrations = migrations();

    let mut migrations_needed: Vec<Box<dyn Migrate>> = Vec::new();

    for (_, migration) in migrations {
        if migration.is_needed(repo)? {
            migrations_needed.push(migration);
        }
    }

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
