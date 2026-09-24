//! Recording in the name table the names of repositories already on disk, so a server that held
//! repositories before the table existed answers for them.

use std::collections::HashSet;
use std::fmt::Display;
use std::path::Path;

use super::{NameTable, key, parse_uuid, warn_held_by_another};
use crate::error::OxenError;
use crate::lmdb::store::LmdbStore;
use crate::repositories::held_name_in_config;
use crate::sync_dir;

/// Key recording that a seed covered every repository on the server. The leading NUL cannot occur
/// in a name a repository records, so this collides with no entry and falls inside no namespace's
/// prefix.
const SEEDED_KEY: &[u8] = b"\0seeded";
/// How many repositories or names a warning lists before it gives a count of the rest, so a fleet
/// with nothing recorded costs a line rather than a line per repository.
const NAMED_IN_A_WARNING: usize = 10;

impl NameTable {
    /// Whether a walk has covered every repository on this server.
    fn is_seeded(&self) -> Result<bool, OxenError> {
        self.read(|db, txn| Ok(db.contains(txn, SEEDED_KEY)?))
    }
}

/// What a walk of the server's repositories left the table holding.
#[derive(Debug, PartialEq, Eq)]
pub struct Seeded {
    /// Repositories the table answers for.
    pub covered: usize,
    /// Names this walk recorded, the rest of `covered` having been recorded already.
    pub recorded: usize,
    /// Repositories the table does not answer for: those recording no name of their own, and those
    /// whose name is recorded for a different repository.
    pub uncovered: usize,
    /// Namespace directories that could not be listed, whose repositories the walk did not see.
    pub unlistable: usize,
    /// Entries naming a repository that no walked config records the name for, left in place.
    pub unclaimed: usize,
}

impl Seeded {
    /// Whether the walk covered every repository on the server, which the table then records so a
    /// later start does not walk again.
    pub fn complete(&self) -> bool {
        self.uncovered == 0 && self.unlistable == 0
    }
}

/// Record the name every repository under `sync_dir` holds, unless an earlier walk covered all of
/// them.
///
/// `None` where the table already answers for every repository.
pub fn run_if_incomplete(sync_dir: &Path) -> Result<Option<Seeded>, OxenError> {
    if NameTable::new(sync_dir).is_seeded()? {
        return Ok(None);
    }
    run(sync_dir).map(Some)
}

/// Record the name every repository under `sync_dir` holds in its config, so the table answers for
/// repositories no create, delete, or transfer has passed through.
///
/// The names go in one commit. A repository is left out where the server reads no whole name for
/// it, where the name it holds is recorded for a different repository (of two recording one name,
/// the first in path order holds it), or where its namespace cannot be listed, and the table is
/// then left asking to be walked again rather than recording that it covers the server. A walk
/// records nothing the table already holds, and warns about the entries no config claims without
/// removing them.
pub fn run(sync_dir: &Path) -> Result<Seeded, OxenError> {
    let mut repo_dirs = Vec::new();
    let mut unlistable = 0;
    for namespace_dir in sync_dir::namespace_dirs(sync_dir)? {
        match sync_dir::repo_dirs(&namespace_dir) {
            Ok(in_namespace) => repo_dirs.extend(in_namespace),
            Err(err) => {
                log::warn!(
                    "Leaving the repositories in {namespace_dir:?} out of the name table, since \
                     it cannot be listed: {err}"
                );
                unlistable += 1;
            }
        }
    }
    let mut held = Vec::new();
    let mut nameless = Vec::new();
    for repo_dir in repo_dirs {
        match held_name_in_config(&repo_dir) {
            Some(name) => held.push(name),
            None => nameless.push(repo_dir),
        }
    }
    if !nameless.is_empty() {
        log::warn!(
            "Leaving {} of {} repositories out of the name table, since the server reads no \
             whole name for them. The optional backfill_repo_identity migration records one for \
             a repository addressed by name: {}",
            nameless.len(),
            nameless.len() + held.len(),
            named(nameless.iter().map(|path| path.display()))
        );
    }

    NameTable::new(sync_dir).write(|db, txn| {
        let mut recorded = 0;
        let mut disputed = 0;
        let mut claimed = HashSet::new();
        for (namespace, name, repo_uuid) in &held {
            let key = key(namespace, name);
            match db.get(txn, &key)? {
                None => {
                    db.put(txn, &key, repo_uuid.to_string().as_bytes())?;
                    recorded += 1;
                    claimed.insert(key);
                }
                Some(entry) => match parse_uuid(&entry, namespace, name) {
                    Ok(holder) if holder == *repo_uuid => {
                        claimed.insert(key);
                    }
                    Ok(holder) => {
                        warn_held_by_another(namespace, name, holder, *repo_uuid);
                        disputed += 1;
                    }
                    Err(err) => {
                        log::warn!("Leaving {namespace}/{name} alone: {err}");
                        disputed += 1;
                    }
                },
            }
        }
        let mut unclaimed = Vec::new();
        for key in db.iter_keys(txn)? {
            let key = key?;
            if key != SEEDED_KEY && !claimed.contains(key) {
                unclaimed.push(String::from_utf8_lossy(key).into_owned());
            }
        }
        if !unclaimed.is_empty() {
            log::warn!(
                "Leaving {} of {} names in the name table taken, though no repository's config \
                 records them, as with a repository removed by hand, a create that did not \
                 finish, or a config that cannot be read: {}",
                unclaimed.len(),
                claimed.len() + unclaimed.len(),
                named(unclaimed.iter())
            );
        }
        let seeded = Seeded {
            covered: held.len() - disputed,
            recorded,
            uncovered: nameless.len() + disputed,
            unlistable,
            unclaimed: unclaimed.len(),
        };
        // Written in the same commit as the entries, so a walk that does not finish leaves the
        // table asking to be walked again rather than claiming to hold every name.
        if seeded.complete() {
            db.put(txn, SEEDED_KEY, b"")?;
        } else {
            db.delete(txn, SEEDED_KEY)?;
        }
        Ok(seeded)
    })
}

/// The first [`NAMED_IN_A_WARNING`] of `items`, with a count of however many are left.
fn named(items: impl ExactSizeIterator<Item = impl Display>) -> String {
    let total = items.len();
    let named: Vec<String> = items
        .take(NAMED_IN_A_WARNING)
        .map(|item| item.to_string())
        .collect();
    match total - named.len() {
        0 => named.join(", "),
        rest => format!("{}, and {rest} more", named.join(", ")),
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::config::RepositoryConfig;
    use crate::model::RepoIdentity;
    use crate::test;
    use crate::util;

    /// A repository at `namespace`/`name` under `sync_dir` recording `identity`, built out of a
    /// config alone, since a config is all a walk of the server's repositories reads.
    fn repo_recording(
        sync_dir: &Path,
        namespace: &str,
        name: &str,
        identity: Option<RepoIdentity>,
    ) -> Result<(), OxenError> {
        let repo_dir = sync_dir.join(namespace).join(name);
        util::fs::create_dir_all(util::fs::oxen_hidden_dir(&repo_dir))?;
        RepositoryConfig {
            identity,
            ..Default::default()
        }
        .save(util::fs::config_filepath(&repo_dir))?;
        Ok(())
    }

    /// Repositories come to hold their names in the table by having their configs walked, so a
    /// server holding repositories from before the table existed answers for them. A repository
    /// the server can read no whole name for is left out, as is one whose name a different
    /// repository holds, and either leaves the table asking for another walk rather than reporting
    /// that it covers the server. An entry no config claims is reported and left in place.
    #[test]
    fn a_walk_records_the_name_every_repository_holds() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let cats = RepoIdentity::minted("ox", "cats");
            repo_recording(sync_dir, "ox", "cats", Some(cats.clone()))?;
            repo_recording(
                sync_dir,
                "ox",
                "dogs",
                Some(RepoIdentity::hintless(Uuid::new_v4())),
            )?;
            repo_recording(sync_dir, "cow", "birds", None)?;

            assert_eq!(
                run(sync_dir)?,
                Seeded {
                    covered: 1,
                    recorded: 1,
                    uncovered: 2,
                    unclaimed: 0,
                    unlistable: 0
                },
                "a repository holding half a name, and one holding no identity, are left out"
            );
            let table = NameTable::new(sync_dir);
            assert_eq!(
                table.get("ox", "cats")?,
                Some(cats.repo_uuid),
                "a repository holding a whole name is recorded under it"
            );
            assert!(!table.is_seeded()?);
            assert_eq!(
                run(sync_dir)?,
                Seeded {
                    covered: 1,
                    recorded: 0,
                    uncovered: 2,
                    unclaimed: 0,
                    unlistable: 0
                },
                "a walk records nothing the table already holds"
            );

            let dogs = RepoIdentity::minted("ox", "dogs");
            repo_recording(sync_dir, "ox", "dogs", Some(dogs.clone()))?;
            repo_recording(
                sync_dir,
                "cow",
                "birds",
                Some(RepoIdentity::minted("cow", "birds")),
            )?;
            assert_eq!(
                run(sync_dir)?,
                Seeded {
                    covered: 3,
                    recorded: 2,
                    uncovered: 0,
                    unclaimed: 0,
                    unlistable: 0
                },
                "the repositories left out of one walk are what the next one records"
            );
            assert_eq!(table.get("ox", "dogs")?, Some(dogs.repo_uuid));
            assert!(table.is_seeded()?);
            assert_eq!(
                run_if_incomplete(sync_dir)?,
                None,
                "a table covering every repository is not walked again"
            );

            // A second repository recording a name the first one holds, which is two configs
            // disagreeing rather than something a walk can choose between.
            repo_recording(
                sync_dir,
                "zoo",
                "cats",
                Some(RepoIdentity {
                    repo_uuid: Uuid::new_v4(),
                    ..cats.clone()
                }),
            )?;
            assert_eq!(
                run(sync_dir)?,
                Seeded {
                    covered: 3,
                    recorded: 0,
                    uncovered: 1,
                    unclaimed: 0,
                    unlistable: 0
                },
                "a name a different repository holds is left with it"
            );
            assert_eq!(
                table.get("ox", "cats")?,
                Some(cats.repo_uuid),
                "the repository already recorded under a name keeps it"
            );
            assert!(
                run_if_incomplete(sync_dir)?.is_some(),
                "a table that has stopped covering every repository is walked again"
            );

            util::fs::remove_dir_all(sync_dir.join("cow").join("birds"))?;
            assert_eq!(
                run(sync_dir)?,
                Seeded {
                    covered: 2,
                    recorded: 0,
                    uncovered: 1,
                    unclaimed: 1,
                    unlistable: 0
                },
                "an entry for a repository removed by hand is reported"
            );
            assert!(
                table.get("cow", "birds")?.is_some(),
                "an entry no config claims is left in place"
            );
            Ok(())
        })
    }
}
