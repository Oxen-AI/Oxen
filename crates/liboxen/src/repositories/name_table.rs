//! The server-wide table resolving a repository's recorded name to the UUID its storage is
//! addressed by, and the uniqueness constraint over those names.
//!
//! Entries are derived from the `[identity]` section of each repository's config, which remains the
//! record of who a repository is. A repository that records only one of the two names holds no
//! entry and is reachable by UUID alone.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytesize::ByteSize;
use uuid::Uuid;

use crate::api::requests::RepoNew;
use crate::error::OxenError;
use crate::lmdb::store::LmdbStore;
use crate::lmdb::{LmdbDb, LmdbEnv, LmdbEnvConfig, open_db, open_shared_env};
use crate::sync_dir::NAME_TABLE_DIR;

pub mod seed;

/// The one database in the env, mapping a repository's name to its UUID.
const NAMES_DB_NAME: &str = "names";
/// Single database, so a `max_dbs` of 1 is sufficient.
const MAX_DBS: u32 = 1;
/// Sparse upper bound on the env's mapped size: address space, not committed memory. One entry
/// costs its key (a namespace of at most 50 characters, a separator, and a repository name) plus a
/// 36-byte UUID, so this holds tens of millions of repositories with the B-tree overhead included.
const NAME_TABLE_MAP_SIZE: ByteSize = ByteSize::gib(8);

/// The directory holding the name table under `sync_dir`.
fn name_table_dir(sync_dir: &Path) -> PathBuf {
    sync_dir.join(NAME_TABLE_DIR)
}

/// Resolves `namespace`/`name` to the UUID of the repository recorded under it, and holds the
/// constraint that one name belongs to one repository.
///
/// Names are matched without regard to case, so `ox/Cats` and `ox/cats` are one name.
pub struct NameTable {
    env: Arc<LmdbEnv>,
    db: LmdbDb,
}

impl NameTable {
    /// Open (or share) the name table under `sync_dir`, creating it when it is not there yet.
    pub fn open(sync_dir: &Path) -> Result<Self, OxenError> {
        let config = LmdbEnvConfig::new(MAX_DBS, NAME_TABLE_MAP_SIZE);
        let env = open_shared_env(&name_table_dir(sync_dir), &config)?;
        let db = open_db(&env, NAMES_DB_NAME)?;
        Ok(NameTable { env, db })
    }

    /// The UUID of the repository recorded under `namespace`/`name`.
    pub fn get(&self, namespace: &str, name: &str) -> Result<Option<Uuid>, OxenError> {
        self.read(|db, txn| match db.get(txn, &key(namespace, name))? {
            Some(recorded) => Ok(Some(parse_uuid(&recorded, namespace, name)?)),
            None => Ok(None),
        })
    }

    /// Record `repo_uuid` as the repository named `namespace`/`name`, reporting whether this call
    /// is what recorded it. Claiming a name that repository already holds writes nothing and
    /// reports `false`, so a caller may repeat a claim it is unsure landed.
    ///
    /// # Errors
    /// [`OxenError::RepoAlreadyExists`] when another repository holds that name. The check and the
    /// write share one transaction, so of two callers claiming one name exactly one succeeds.
    pub fn claim(&self, namespace: &str, name: &str, repo_uuid: Uuid) -> Result<bool, OxenError> {
        self.write(|db, txn| {
            let key = key(namespace, name);
            if let Some(recorded) = db.get(txn, &key)? {
                return match parse_uuid(&recorded, namespace, name)? {
                    holder if holder == repo_uuid => Ok(false),
                    _ => Err(already_taken(namespace, name)),
                };
            }
            db.put(txn, &key, repo_uuid.to_string().as_bytes())?;
            Ok(true)
        })
    }

    /// Release the name `repo_uuid` holds as `namespace`/`name`, so another repository may take
    /// it. Writes nothing where that name is held by a different repository, or by none.
    pub fn release(&self, namespace: &str, name: &str, repo_uuid: Uuid) -> Result<(), OxenError> {
        self.write(|db, txn| {
            let key = key(namespace, name);
            if let Some(recorded) = db.get(txn, &key)? {
                let holder = parse_uuid(&recorded, namespace, name)?;
                if holder != repo_uuid {
                    warn_held_by_another(namespace, name, holder, repo_uuid);
                    return Ok(());
                }
            }
            db.delete(txn, &key)?;
            Ok(())
        })
    }

    /// Move the name `repo_uuid` holds as `namespace`/`name` into `to_namespace`, in one commit,
    /// so no moment exists where the name resolves to neither namespace or to both.
    ///
    /// Writes nothing where that name is held by a different repository, or by none.
    ///
    /// # Errors
    /// [`OxenError::RepoAlreadyExists`] when a repository in `to_namespace` already holds `name`.
    pub fn move_to_namespace(
        &self,
        namespace: &str,
        name: &str,
        to_namespace: &str,
        repo_uuid: Uuid,
    ) -> Result<(), OxenError> {
        self.write(|db, txn| {
            let from = key(namespace, name);
            let Some(recorded) = db.get(txn, &from)? else {
                return Ok(());
            };
            let holder = parse_uuid(&recorded, namespace, name)?;
            if holder != repo_uuid {
                warn_held_by_another(namespace, name, holder, repo_uuid);
                return Ok(());
            }
            let to = key(to_namespace, name);
            if to != from && db.contains(txn, &to)? {
                return Err(already_taken(to_namespace, name));
            }
            db.delete(txn, &from)?;
            db.put(txn, &to, &recorded)?;
            Ok(())
        })
    }
}

impl LmdbStore for NameTable {
    fn lmdb_env(&self) -> &LmdbEnv {
        &self.env
    }

    fn lmdb_db(&self) -> &LmdbDb {
        &self.db
    }
}

/// The key a repository named `namespace`/`name` is recorded under, lowercased so one name in two
/// spellings is one entry rather than two repositories.
///
/// Both halves are a single path component or a validated name, so neither can contain the
/// separator and every repository in a namespace shares the prefix `{namespace}/`.
fn key(namespace: &str, name: &str) -> Vec<u8> {
    format!(
        "{}/{}",
        namespace.to_ascii_lowercase(),
        name.to_ascii_lowercase()
    )
    .into_bytes()
}

/// The UUID `recorded` holds, or an error naming the entry that does not hold one.
fn parse_uuid(recorded: &[u8], namespace: &str, name: &str) -> Result<Uuid, OxenError> {
    let recorded = std::str::from_utf8(recorded)
        .map_err(|err| OxenError::internal_error(format!("{namespace}/{name}: {err}")))?;
    Uuid::parse_str(recorded).map_err(|err| {
        OxenError::internal_error(format!(
            "{namespace}/{name} is recorded as '{recorded}', which is not a UUID: {err}"
        ))
    })
}

/// Warns that `namespace`/`name` was left as it was: the repository operating on it is not the one
/// holding it.
fn warn_held_by_another(namespace: &str, name: &str, holder: Uuid, repo_uuid: Uuid) {
    log::warn!("Leaving {namespace}/{name} alone: it names {holder}, not {repo_uuid}");
}

fn already_taken(namespace: &str, name: &str) -> OxenError {
    OxenError::RepoAlreadyExists(Box::new(RepoNew::from_namespace_name(
        namespace, name, None,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test;

    /// One name belongs to one repository, whatever case it arrives in; it follows the repository
    /// into a new namespace; and releasing it hands it to the next claimant. Case folding is
    /// asserted on both halves of the key, since a namespace and a repository name are validated
    /// by different rules. A claim reports whether it is what recorded the name, so a repeat of
    /// one reports nothing recorded. Only the repository holding a name releases or moves it, so an
    /// operation carrying a name another one holds leaves that name alone. The entry then outlives
    /// the env that wrote it, and a value that is not a UUID is reported rather than resolving to
    /// something arbitrary.
    #[test]
    fn a_name_belongs_to_one_repository_and_follows_it() -> Result<(), OxenError> {
        test::run_empty_dir_test(|sync_dir| {
            let table = NameTable::open(sync_dir)?;
            let cats = Uuid::new_v4();
            assert!(table.claim("ox", "cats", cats)?, "a free name is recorded");

            assert_eq!(table.get("ox", "cats")?, Some(cats));
            assert_eq!(
                table.get("OX", "Cats")?,
                Some(cats),
                "a name resolves whatever case it is asked for in"
            );
            assert_eq!(table.get("ox", "dogs")?, None);
            assert_eq!(
                table.get("cow", "cats")?,
                None,
                "the namespace is half of the name, so one repository name is free in another"
            );

            let err = table
                .claim("OX", "CATS", Uuid::new_v4())
                .expect_err("a name one repository holds cannot be claimed by another");
            assert!(
                matches!(err, OxenError::RepoAlreadyExists(_)),
                "expected the conflict the create path reports as 409, got {err:?}"
            );
            assert_eq!(
                table.get("ox", "cats")?,
                Some(cats),
                "a refused claim leaves the holder in place"
            );
            assert!(
                !table.claim("OX", "CATS", cats)?,
                "a repeat of a claim records nothing, and reports so"
            );
            assert_eq!(
                table.get("ox", "cats")?,
                Some(cats),
                "the repository already holding a name may claim it again"
            );

            // A second repository of the same name in another namespace, which is what makes the
            // move below a conflict rather than a rename.
            let impostor = Uuid::new_v4();
            table.claim("cow", "cats", impostor)?;
            let err = table
                .move_to_namespace("ox", "cats", "cow", cats)
                .expect_err("a name the destination holds refuses the move");
            assert!(
                matches!(err, OxenError::RepoAlreadyExists(_)),
                "expected a name conflict, got {err:?}"
            );
            assert_eq!(
                (table.get("ox", "cats")?, table.get("cow", "cats")?),
                (Some(cats), Some(impostor)),
                "a refused move leaves both names as they were"
            );
            table.release("cow", "cats", cats)?;
            table.move_to_namespace("cow", "cats", "zoo", cats)?;
            assert_eq!(
                (table.get("cow", "cats")?, table.get("zoo", "cats")?),
                (Some(impostor), None),
                "a name another repository holds is neither released nor moved out from under it"
            );

            table.move_to_namespace("ox", "cats", "zoo", cats)?;
            assert_eq!(table.get("zoo", "cats")?, Some(cats));
            assert_eq!(
                table.get("ox", "cats")?,
                None,
                "a move frees the name it came from"
            );
            table.move_to_namespace("ZOO", "cats", "ZOO", cats)?;
            assert_eq!(
                table.get("zoo", "cats")?,
                Some(cats),
                "a move into the namespace a repository already sits in leaves it recorded"
            );
            table.move_to_namespace("ox", "cats", "zoo", cats)?;
            assert_eq!(
                table.get("zoo", "cats")?,
                Some(cats),
                "moving a name no repository holds leaves the table alone"
            );

            table.release("ZOO", "cats", cats)?;
            assert_eq!(table.get("zoo", "cats")?, None);
            table.release("zoo", "cats", cats)?;
            let next = Uuid::new_v4();
            table.claim("zoo", "cats", next)?;
            assert_eq!(
                table.get("zoo", "cats")?,
                Some(next),
                "a released name is free for the next repository"
            );

            table.write(|db, txn| {
                db.put(txn, &key("ox", "dogs"), b"not a uuid")?;
                Ok::<(), OxenError>(())
            })?;
            let err = table
                .get("ox", "dogs")
                .expect_err("a value that is not a UUID cannot resolve to a repository");
            assert!(
                err.to_string().contains("ox/dogs"),
                "the error should name the entry it read, got {err}"
            );

            // Closing the env is what makes the read below come off disk rather than out of the
            // handle that wrote it.
            drop(table);
            assert_eq!(
                NameTable::open(sync_dir)?.get("zoo", "cats")?,
                Some(next),
                "an entry outlives the env that recorded it"
            );
            Ok(())
        })
    }
}
