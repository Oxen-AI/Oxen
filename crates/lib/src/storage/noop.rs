//! THIS FILE WILL BE REMOVED IN THE NEAR FUTURE WHEN WE STOP SERIALIZING `LocalRepository`
//!
//! Placeholder `VersionStore` used to satisfy `LocalRepository::version_store`'s non-Option
//! invariant on `Deserialize`. Wire-shape stubs (e.g. a `LocalRepository` embedded in a
//! `Workspace` payload) don't carry a real backend; this implementation errors on every call so
//! a caller that reaches for the store on a deserialized stub gets a loud, specific failure.
//!
//! Constructor-built `LocalRepository`s always get a real backend via `create_version_store`.

use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::AsyncRead;
use tokio_stream::Stream;

use crate::error::OxenError;
use crate::storage::{LocalFilePath, StorageKind, VersionStore};
use crate::view::versions::CleanCorruptedVersionsResult;

#[derive(Debug)]
pub struct NoopVersionStore;

fn err(op: &'static str) -> OxenError {
    OxenError::VersionStorePlaceholderUsed { operation: op }
}

#[async_trait]
impl VersionStore for NoopVersionStore {
    async fn init(&self) -> Result<(), OxenError> {
        Err(err("init"))
    }

    async fn store_version_from_reader(
        &self,
        _hash: &str,
        _reader: Box<dyn AsyncRead + Send + Unpin>,
        _size: u64,
    ) -> Result<(), OxenError> {
        Err(err("store_version_from_reader"))
    }

    async fn store_version(&self, _hash: &str, _data: &[u8]) -> Result<(), OxenError> {
        Err(err("store_version"))
    }

    async fn store_version_chunk(
        &self,
        _hash: &str,
        _offset: u64,
        _data: Bytes,
    ) -> Result<(), OxenError> {
        Err(err("store_version_chunk"))
    }

    async fn store_version_derived(
        &self,
        _orig_hash: &str,
        _derived_filename: &str,
        _derived_data: &[u8],
    ) -> Result<(), OxenError> {
        Err(err("store_version_derived"))
    }

    async fn get_version_chunk(
        &self,
        _hash: &str,
        _offset: u64,
        _size: u64,
    ) -> Result<Vec<u8>, OxenError> {
        Err(err("get_version_chunk"))
    }

    async fn list_version_chunks(&self, _hash: &str) -> Result<Vec<u64>, OxenError> {
        Err(err("list_version_chunks"))
    }

    async fn combine_version_chunks(&self, _hash: &str) -> Result<(), OxenError> {
        Err(err("combine_version_chunks"))
    }

    async fn get_version_size(&self, _hash: &str) -> Result<u64, OxenError> {
        Err(err("get_version_size"))
    }

    async fn get_version(&self, _hash: &str) -> Result<Vec<u8>, OxenError> {
        Err(err("get_version"))
    }

    async fn get_version_stream(
        &self,
        _hash: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>
    {
        Err(err("get_version_stream"))
    }

    async fn get_version_derived_size(
        &self,
        _orig_hash: &str,
        _derived_filename: &str,
    ) -> Result<u64, OxenError> {
        Err(err("get_version_derived_size"))
    }

    async fn get_version_derived_stream(
        &self,
        _orig_hash: &str,
        _derived_filename: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin>, OxenError>
    {
        Err(err("get_version_derived_stream"))
    }

    async fn derived_version_exists(
        &self,
        _orig_hash: &str,
        _derived_filename: &str,
    ) -> Result<bool, OxenError> {
        Err(err("derived_version_exists"))
    }

    async fn get_version_path(&self, _hash: &str) -> Result<LocalFilePath, OxenError> {
        Err(err("get_version_path"))
    }

    async fn copy_version_to_path(&self, _hash: &str, _dest_path: &Path) -> Result<(), OxenError> {
        Err(err("copy_version_to_path"))
    }

    async fn version_exists(&self, _hash: &str) -> Result<bool, OxenError> {
        Err(err("version_exists"))
    }

    async fn delete_version(&self, _hash: &str) -> Result<(), OxenError> {
        Err(err("delete_version"))
    }

    async fn list_versions(&self) -> Result<Vec<String>, OxenError> {
        Err(err("list_versions"))
    }

    async fn clean_corrupted_versions(
        &self,
        _dry_run: bool,
    ) -> Result<CleanCorruptedVersionsResult, OxenError> {
        Err(err("clean_corrupted_versions"))
    }

    fn storage_kind(&self) -> StorageKind {
        StorageKind::Local
    }
}
