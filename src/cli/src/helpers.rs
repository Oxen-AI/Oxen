use liboxen::api;
use liboxen::command::migrate::CreateMerkleTreesMigration;
use liboxen::command::migrate::Migrate;
use liboxen::command::migrate::UpdateVersionFilesMigration;
use liboxen::config::AuthConfig;
use liboxen::constants;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::util::oxen_version::OxenVersion;

use colored::Colorize;

use std::str::FromStr;

pub fn get_host_or_default() -> Result<String, OxenError> {
    let config = AuthConfig::get_or_create()?;
    let mut default_host = constants::DEFAULT_HOST.to_string();
    if let Some(host) = config.default_host {
        if !host.is_empty() {
            default_host = host;
        }
    }
    Ok(default_host)
}

pub fn get_host_from_repo(repo: &LocalRepository) -> Result<String, OxenError> {
    if let Some(remote) = repo.remote() {
        let host = api::client::get_host_from_url(remote.url)?;
        return Ok(host);
    }
    get_host_or_default()
}

pub async fn check_remote_version(host: impl AsRef<str>) -> Result<(), OxenError> {
    // Do the version check in the dispatch because it's only really the CLI that needs to do it
    match api::client::version::get_remote_version(host.as_ref()).await {
        Ok(remote_version) => {
            let local_version: &str = constants::OXEN_VERSION;

            if remote_version != local_version {
                let warning = format!("Warning: 🐂 Oxen remote version mismatch.\n\nCLI Version: {local_version}\nServer Version: {remote_version}\n\nPlease visit https://docs.oxen.ai/getting-started/install for installation instructions.\n").yellow();
                eprintln!("{warning}");
            }
        }
        Err(err) => {
            eprintln!("Err checking remote version:\n{err}")
        }
    }
    Ok(())
}

pub async fn check_remote_version_blocking(host: impl AsRef<str>) -> Result<(), OxenError> {
    match api::client::version::get_min_oxen_version(host.as_ref()).await {
        Ok(remote_version) => {
            let local_version: &str = constants::OXEN_VERSION;
            let min_oxen_version = OxenVersion::from_str(&remote_version)?;
            let local_oxen_version = OxenVersion::from_str(local_version)?;

            if local_oxen_version < min_oxen_version {
                return Err(OxenError::OxenUpdateRequired(format!(
                    "Error: Oxen CLI out of date. Pushing to OxenHub requires version >= {:?}, found version {:?}.\n\nVisit https://docs.oxen.ai/getting-started/intro for update instructions.",
                    min_oxen_version,
                    local_oxen_version
                ).into()));
            }
        }
        Err(_) => {
            return Err(OxenError::basic_str(
                "Error: unable to verify remote version",
            ));
        }
    }
    Ok(())
}

pub fn check_repo_migration_needed(repo: &LocalRepository) -> Result<(), OxenError> {
    let migrations: Vec<Box<dyn Migrate>> = vec![
        Box::new(UpdateVersionFilesMigration),
        Box::new(CreateMerkleTreesMigration),
    ];

    let mut migrations_needed: Vec<Box<dyn Migrate>> = Vec::new();

    for migration in migrations {
        if migration.is_needed(repo)? {
            migrations_needed.push(migration);
        }
    }

    if migrations_needed.is_empty() {
        return Ok(());
    }
    let warning = "\nWarning: 🐂 This repo requires a quick migration to the latest Oxen version. \n\nPlease run the following to update:".to_string().yellow();
    eprintln!("{warning}\n\n");
    for migration in migrations_needed {
        eprintln!(
            "{}",
            format!("oxen migrate up {} .\n", migration.name()).yellow()
        );
    }
    eprintln!("\n");
    Err(OxenError::MigrationRequired(
        "Error: Migration required".to_string().into(),
    ))
}
