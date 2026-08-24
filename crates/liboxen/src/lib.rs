//! 🐂 liboxen
//!
//! Fast unstructured data version control.
//!
//! # Examples
//!
//! Instantiating a new repo:
//!
//! ```
//! # #[tokio::main]
//! # async fn main() -> Result<(), liboxen::error::OxenError> {
//! use liboxen::{repositories, test, util};
//!
//! test::run_empty_dir_test_async(|dir| async move {
//!     // Instantiate a new repo
//!     let repo = repositories::init(&dir)?;
//!     // Add a file to the repo
//!     let file = dir.join("file.txt");
//!     util::fs::write_to_path(&file, "Hello World")?;
//!     repositories::add(&repo, &file).await?;
//!     // Commit the file
//!     repositories::commit(&repo, "Added file.txt")?;
//!     Ok(())
//! })
//! .await?;
//! # Ok(())
//! # }
//! ```
//!
//! Push data from local repo to remote repo:
//!
//! Requires a reachable remote, so this example is compiled but not run.
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> Result<(), liboxen::error::OxenError> {
//! use liboxen::command;
//! use liboxen::model::LocalRepository;
//! use liboxen::repositories;
//!
//! // Create LocalRepository from existing repo
//! let mut repo = LocalRepository::from_dir("test_repo")?;
//! // Add a file to the repo
//! repositories::add(&repo, "file.txt").await?;
//! // Commit the file
//! repositories::commit(&repo, "Added file.txt")?;
//! // Set remote
//! let remote_url = "http://0.0.0.0:3000/ox/test_repo";
//! let remote_name = "origin";
//! command::config::set_remote(&mut repo, remote_name, remote_url)?;
//! // Push to remote
//! repositories::push(&repo).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Clone data from remote url
//!
//! Requires a reachable remote, so this example is compiled but not run.
//!
//! ```no_run
//! # #[tokio::main]
//! # async fn main() -> Result<(), liboxen::error::OxenError> {
//! use liboxen::opts::CloneOpts;
//! use liboxen::repositories;
//!
//! let url = "http://0.0.0.0:3000/ox/test_repo";
//! let repo_dir = "test_repo";
//! let opts = CloneOpts::new(url, &repo_dir);
//! let repo = repositories::clone::clone(&opts).await?;
//! # Ok(())
//! # }
//! ```

extern crate approx;
extern crate bytecount;
extern crate bytesize;
// extern crate ffmpeg_next as ffmpeg;
extern crate fs_extra;
extern crate lazy_static;

pub mod api;
pub mod command;
pub mod config;
pub mod constants;
pub mod core;
pub mod error;
pub mod lmdb;
pub mod migrations;
pub mod model;
pub mod namespaces;
pub mod opts;
pub mod repositories;
pub mod request_context;
pub mod resource;
pub mod storage;
#[cfg(any(test, feature = "test-utils"))]
pub mod test;
pub mod util;
pub mod view;
