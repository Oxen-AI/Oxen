pub mod commit_entry;
pub mod entry_data_type;
pub mod entry_status;
pub mod metadata_entry;
pub mod mod_entry;
pub mod remote_entry;
pub mod staged_entry;
pub mod unsynced_commit_entry;

pub trait ContentHashable {
    fn content_hash(&self) -> String;
}
