use std::path::PathBuf;

use heed::EnvOpenOptions;

use crate::explore::lmdb_impl::LmdbMerkleDB;
use crate::explore::new_path::AbsolutePath;
use crate::explore::scratch::Repository;

pub fn main() {
    let tmp_path: PathBuf = std::env::temp_dir().join("lmdb_oxen_explore");
    std::fs::create_dir_all(&tmp_path).expect("failed to create temp dir");

    let repo = Repository::from_walk(tmp_path.clone()).expect("failed to create Repository");

    let db_path = tmp_path.join("lmdb_data");
    std::fs::create_dir_all(&db_path).expect("failed to create db directory");
    let db_location = AbsolutePath::new(db_path).expect("failed to create AbsolutePath");

    let options = EnvOpenOptions::new();
    let _db = LmdbMerkleDB::new(repo, db_location, &options).expect("failed to create LmdbMerkleDB");
}
