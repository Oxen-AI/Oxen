use super::Migrate;

use std::path::Path;

use crate::core::v0_10_0::index::{CommitReader, SchemaWriter};
use crate::error::OxenError;
use crate::model::LocalRepository;

use crate::repositories;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};

pub struct PropagateSchemasMigration;
impl Migrate for PropagateSchemasMigration {
    fn name(&self) -> &'static str {
        "propagate_schemas"
    }

    fn description(&self) -> &'static str {
        "Propagates schemas to the latest commit"
    }

    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            propagate_schemas_for_all_repos_up(path)?;
        } else {
            let repo = LocalRepository::new(path)?;
            propagate_schemas_up(&repo)?;
        }
        Ok(())
    }

    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            propagate_schemas_for_all_repos_down(path)?;
        } else {
            println!("Running down migration");
            let repo = LocalRepository::new(path)?;
            propagate_schemas_down(&repo)?;
        }
        Ok(())
    }

    fn is_needed(&self, _repo: &LocalRepository) -> Result<bool, OxenError> {
        // Server-side migration, not necessary for autodetection on client
        Ok(false)
    }
}

pub fn propagate_schemas_for_all_repos_up(path: &Path) -> Result<(), OxenError> {
    println!("🐂 Collecting namespaces to migrate...");
    let namespaces = repositories::list_namespaces(path)?;
    let bar = oxen_progress_bar(namespaces.len() as u64, ProgressBarType::Counter);
    println!("🐂 Migrating {} namespaces", namespaces.len());
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        // Show the canonical namespace path
        log::debug!(
            "This is the namespace path we're walking: {:?}",
            namespace_path.canonicalize()?
        );
        let repos = repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            match propagate_schemas_up(&repo) {
                Ok(_) => {}
                Err(err) => {
                    log::error!(
                        "Could not migrate version files for repo {:?}\nErr: {}",
                        repo.path.canonicalize(),
                        err
                    )
                }
            }
        }
        bar.inc(1);
    }

    Ok(())
}

pub fn propagate_schemas_for_all_repos_down(_path: &Path) -> Result<(), OxenError> {
    println!("There are no operations to be run");
    Ok(())
}

pub fn propagate_schemas_up(repo: &LocalRepository) -> Result<(), OxenError> {
    // Traverses commits from BASE to HEAD and write all schemas for all history leading up to HEAD.
    let mut lock_file = repositories::get_lock_file(repo)?;
    let _mutex = repositories::get_exclusive_lock(&mut lock_file)?;

    let reader = CommitReader::new(repo)?;
    let all_commits = reader.list_all_sorted_by_timestamp()?;

    for current_commit in &all_commits {
        for parent_commit_id in &current_commit.parent_ids {
            let Some(parent_commit) = repositories::commits::get_by_id(repo, parent_commit_id)?
            else {
                continue;
            };
            let schemas = repositories::data_frames::schemas::list(repo, &parent_commit)?;
            let schema_writer = SchemaWriter::new(repo, &current_commit.id)?;

            for (path, schema) in schemas {
                if !schema_writer.has_schema(&schema) {
                    schema_writer.put_schema(&schema)?;
                }

                schema_writer.put_schema_for_file(&path, &schema)?;
            }
        }
    }

    Ok(())
}

pub fn propagate_schemas_down(_repo: &LocalRepository) -> Result<(), OxenError> {
    println!("There are no operations to be run");
    Ok(())
}
