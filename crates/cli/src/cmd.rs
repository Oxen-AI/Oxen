use std::collections::HashMap;

use async_trait::async_trait;

pub mod add;
pub use add::AddCmd;

pub mod branch;
pub use branch::BranchCmd;

pub mod checkout;
pub use checkout::CheckoutCmd;

pub mod cat;
pub use cat::CatCmd;

pub mod clean;
pub use clean::CleanCmd;

pub mod clone;
pub use clone::CloneCmd;

pub mod commit;
pub use commit::CommitCmd;

pub mod config;
pub use config::ConfigCmd;

pub mod create_remote;
pub use create_remote::CreateRemoteCmd;

pub mod db;
pub use db::DbCmd;

pub mod delete_remote;
pub use delete_remote::DeleteRemoteCmd;

pub mod df;
pub use df::DFCmd;

pub mod diff;
pub use diff::DiffCmd;

pub mod download;
pub use download::DownloadCmd;

pub mod embeddings;
pub use embeddings::EmbeddingsCmd;

pub mod fetch;
pub use fetch::FetchCmd;

pub mod info;
pub use info::InfoCmd;

pub mod init;
pub use init::InitCmd;

pub mod load;
pub use load::LoadCmd;

pub mod log;
pub use log::LogCmd;

pub mod ls;
pub use ls::LsCmd;

pub mod migrate;
pub use migrate::MigrateCmd;

pub mod moo;
pub use moo::MooCmd;

pub mod merge;
pub use merge::MergeCmd;

pub mod node;
pub use node::NodeCmd;

pub mod pull;
pub use pull::PullCmd;

pub mod push;
pub use push::PushCmd;

pub mod remote;
pub use remote::RemoteCmd;

pub mod restore;
pub use restore::RestoreCmd;

pub mod remote_mode;
pub use remote_mode::RemoteModeCmd;

pub mod rm;
pub use rm::RmCmd;

pub mod save;
pub use save::SaveCmd;

pub mod schemas;
pub use schemas::SchemasCmd;

pub mod tree;
pub use tree::TreeCmd;

pub mod status;
pub use status::StatusCmd;

pub mod upload;
pub use upload::UploadCmd;

pub mod workspace;
pub use workspace::WorkspaceCmd;

pub mod prune;
pub use prune::PruneCmd;

pub mod fsck;
pub use fsck::FsckCmd;

/// Maps the name of a [`RunCmd`] to its implementation.
pub type Runners = HashMap<String, Box<dyn RunCmd>>;

/// Trait for that any oxen CLI comand must implement.
///
/// Oxen CLI programs have a human-readable name, CLI arguments, and a method to trigger the program's
/// main logic given parsed CLI arguments.
///
/// NOTE: Must keep this trait dyn-compatible.
#[async_trait]
pub trait RunCmd {
    /// The name of the CLI program. Used to identify it and display in user-facing messages.
    fn name(&self) -> &str;
    /// The CLI arguments for this program.
    fn args(&self) -> clap::Command;
    /// Execute the CLI program's logic.
    async fn run(&self, args: &clap::ArgMatches) -> Result<(), anyhow::Error>;
}
