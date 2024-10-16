//! Command line option structs.
//!

pub mod add_opts;
pub mod clone_opts;
pub mod count_lines_opts;
pub mod df_opts;
pub mod diff_opts;
pub mod download_opts;
pub mod helpers;
pub mod info_opts;
pub mod ls_opts;
pub mod paginate_opts;
pub mod pull_opts;
pub mod restore_opts;
pub mod rm_opts;
pub mod upload_opts;

pub use crate::opts::add_opts::AddOpts;
pub use crate::opts::clone_opts::CloneOpts;
pub use crate::opts::count_lines_opts::CountLinesOpts;
pub use crate::opts::df_opts::DFOpts;
pub use crate::opts::diff_opts::DiffOpts;
pub use crate::opts::download_opts::DownloadOpts;
pub use crate::opts::info_opts::InfoOpts;
pub use crate::opts::ls_opts::ListOpts;
pub use crate::opts::paginate_opts::PaginateOpts;
pub use crate::opts::pull_opts::PullOpts;
pub use crate::opts::restore_opts::RestoreOpts;
pub use crate::opts::rm_opts::RmOpts;
pub use crate::opts::upload_opts::UploadOpts;
