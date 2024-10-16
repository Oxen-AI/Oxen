//! TODO: this is depreciated as of v0.19.0 and should be removed
//!

use crate::core::v0_10_0::index::CommitReader;
use crate::model::data_frame::schema::{DataType, Field};
use crate::model::metadata::to_duckdb_sql::ToDuckDBSql;
use crate::model::LocalRepository;
use crate::model::Schema;
use crate::model::{Commit, CommitEntry};
use crate::repositories;
use crate::util;

use duckdb::types::ToSql;
use std::path::Path;

pub struct DirMetadataItem {
    hash: String,
    directory: String,
    filename: String,
    path: String,
    num_bytes: u64,
    commit_id: String,
    timestamp: String,
    pub data_type: String,
    pub mime_type: String,
    extension: String,
    is_dir: bool,
}

impl DirMetadataItem {
    pub fn schema() -> Schema {
        let fields = vec![
            Field::new("hash", DataType::String.to_string().as_str()),
            Field::new("directory", DataType::String.to_string().as_str()),
            Field::new("filename", DataType::String.to_string().as_str()),
            Field::new("path", DataType::String.to_string().as_str()),
            Field::new("num_bytes", DataType::UInt64.to_string().as_str()),
            Field::new("commit_id", DataType::String.to_string().as_str()),
            Field::new("timestamp", DataType::String.to_string().as_str()),
            Field::new("data_type", DataType::String.to_string().as_str()),
            Field::new("mime_type", DataType::String.to_string().as_str()),
            Field::new("extension", DataType::String.to_string().as_str()),
            Field::new("is_dir", DataType::Boolean.to_string().as_str()),
        ];
        Schema::new(fields)
    }

    pub fn from_dir(dir: &Path, commit: &Commit) -> Self {
        let path = dir.to_string_lossy();
        let mime_type = "directory".to_string();
        let data_type = "directory".to_string();
        let size = 0;
        let directory = dir.parent().unwrap_or(Path::new("")).to_string_lossy();
        log::debug!("trying to get filename for {:?}", dir);
        let filename = dir.file_name().unwrap().to_str().unwrap();
        log::debug!("got filename {:?}", filename);
        let extension = "".to_string();
        let timestamp = commit
            .timestamp
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();

        DirMetadataItem {
            hash: "".to_string(),
            directory: directory.to_string(),
            filename: filename.to_string(),
            path: path.to_string(),
            num_bytes: size,
            commit_id: commit.id.clone(),
            timestamp,
            data_type,
            mime_type,
            extension,
            is_dir: true,
        }
    }

    pub fn from_entry(
        repo: &LocalRepository,
        entry: &CommitEntry,
        commit_reader: &CommitReader,
    ) -> Self {
        let path = util::fs::version_path(repo, entry);
        let mime_type = util::fs::file_mime_type(&path);
        let data_type = util::fs::datatype_from_mimetype(&path, &mime_type);

        // TODO: Handle unwraps more gracefully
        let size = repositories::metadata::get_file_size(&path).unwrap_or(0);
        let dir = entry
            .path
            .parent()
            .unwrap_or(Path::new(""))
            .to_string_lossy();
        let filename = entry.path.file_name().unwrap().to_str().unwrap();
        let entry_path = entry.path.to_string_lossy();
        let extension = util::fs::file_extension(&path);
        let data_type = format!("{:?}", data_type).to_lowercase();
        let commit_id = entry.commit_id.clone();
        let commit = commit_reader.get_commit_by_id(&commit_id).unwrap().unwrap();
        let timestamp = commit
            .timestamp
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();

        DirMetadataItem {
            hash: entry.hash.clone(),
            directory: dir.to_string(),
            filename: filename.to_string(),
            path: entry_path.to_string(),
            num_bytes: size,
            commit_id,
            timestamp,
            data_type,
            mime_type,
            extension,
            is_dir: false,
        }
    }
}

impl ToDuckDBSql for DirMetadataItem {
    fn to_sql(&self) -> Vec<&dyn ToSql> {
        vec![
            &self.hash,
            &self.directory,
            &self.filename,
            &self.path,
            &self.num_bytes,
            &self.commit_id,
            &self.timestamp,
            &self.data_type,
            &self.mime_type,
            &self.extension,
            &self.is_dir,
        ]
    }
}

impl std::fmt::Display for DirMetadataItem {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DirMetadataItem({})", self.path)
    }
}
