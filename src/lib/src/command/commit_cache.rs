//! # oxen commit-cache
//!
//! Compute the cache for a commits, used from the CLI for migrations on commits that
//! were created before the cache was introduced.
//!

use std::path::Path;

use crate::core::v0_10_0::cache;
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::repositories;

/// Run the computation cache on all repositories within a directory
pub async fn compute_cache_on_all_repos(path: &Path, force: bool) -> Result<(), OxenError> {
    let namespaces = repositories::list_namespaces(path)?;
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        let repos = repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            println!("Compute cache for repo {:?}", repo.path);
            match compute_cache(&repo, None, force).await {
                Ok(_) => {
                    println!("Done.");
                }
                Err(err) => {
                    log::error!(
                        "Could not compute cache for repo {:?}\nErr: {}",
                        repo.path,
                        err
                    )
                }
            }
        }
    }

    Ok(())
}

/// Run the computation cache on all repositories within a directory
pub async fn compute_cache(
    repo: &LocalRepository,
    revision: Option<String>,
    force: bool,
) -> Result<(), OxenError> {
    println!(
        "Compute cache for commit given [{revision:?}] on repo {:?}",
        repo.path
    );
    let commits = if let Some(revision) = revision {
        repositories::commits::list_from(repo, &revision)?
    } else {
        Vec::from_iter(repositories::commits::list_all(repo)?.into_iter())
    };
    for commit in commits {
        println!("Compute cache for commit {:?}", commit);
        cache::commit_cacher::run_all(repo, &commit, force)?;
    }
    Ok(())
}
