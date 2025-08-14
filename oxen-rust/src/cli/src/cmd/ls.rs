use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::error::OxenError;
use liboxen::model::{LocalRepository, StagedData, StagedEntry, StagedEntryStatus};
use liboxen::model::staged_data::StagedDataOpts;
use liboxen::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use liboxen::{repositories, api};

use std::path::PathBuf;
use std::collections::HashMap;
use chrono::{Local, TimeZone};
use colored::{ColoredString, Colorize};

use crate::cmd::RunCmd;
pub const NAME: &str = "ls";
pub struct LsCmd;


// TODO: Support options besides the base 'ls' functionality -- file stats, etc; 
#[async_trait]
impl RunCmd for LsCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Print the files of a commit.")
            .arg(
                Arg::new("commit")
                    .long("commit")
                    .short('c')
                    .help("The commit to list files from.")
                    .default_value("HEAD")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("directory")
                    .long("directory")
                    .short('d')
                    .help("The directory from the remote to display")
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        
        let repo = LocalRepository::from_current_dir()?;
        
        // Early exit for non-remote-mode repositories
        let Some(ref workspace_identifier) = repo.workspace_name else {
            return Ok(());
        };

        let commit_id = args
            .get_one::<String>("commit")
            .expect("Must supply commit");

        let root_dir = "".to_string();
        let directory = args
            .get_one::<String>("directory")
            .unwrap_or(&root_dir);

        let commit = if commit_id == "HEAD" {
            repositories::commits::head_commit(&repo)?
        } else {
            let Some(commit) = repositories::commits::get_by_id(&repo, commit_id)? else {
                return Err(OxenError::basic_str(format!(
                    "Commit {} not found",
                    commit_id
                )));
            };
            commit
        };

        let page_size = 1;
        let page_num = 0;
        let directory = PathBuf::from(&directory);

        // Get staged entries from remote
        let remote_repo = api::client::repositories::get_default_remote(&repo).await?;
        let remote_status = api::client::workspaces::changes::list(
            &remote_repo,
            workspace_identifier,
            directory.clone(),
            page_num,
            page_size,
        ).await?;

        let mut status = StagedData::empty();
        status.staged_dirs = remote_status.added_dirs;
        let added_files: HashMap<PathBuf, StagedEntry> =
            HashMap::from_iter(remote_status.added_files.entries.into_iter().map(|e| {
                (
                    PathBuf::from(e.filename()),
                    StagedEntry::empty_status(StagedEntryStatus::Added),
                )
            }));
        let added_mods: HashMap<PathBuf, StagedEntry> =
            HashMap::from_iter(remote_status.modified_files.entries.into_iter().map(|e| {
                (
                    PathBuf::from(e.filename()),
                    StagedEntry::empty_status(StagedEntryStatus::Modified),
                )
            }));
        let staged_removals: HashMap<PathBuf, StagedEntry> =
            HashMap::from_iter(remote_status.removed_files.entries.into_iter().map(|e| {
                (
                    PathBuf::from(e.filename()),
                    StagedEntry::empty_status(StagedEntryStatus::Removed),
                )
            }));
        status.staged_files = added_files
            .into_iter()
            .chain(added_mods)
            .chain(staged_removals)
            .collect();

        
        // Get directory children from latest commit
        let Some(dir_node) = repositories::tree::get_dir_with_children(&repo, &commit, &directory)? else {
            return Ok(())
        };
        log::debug!("dir node: {dir_node:?}");
        let dir_children = repositories::tree::list_files_and_folders(&dir_node)?;
        
        // Print data
        let remote_mode_message = "\nRemote-Mode Repository".green().bold();
        let remote_mode_sub_message = "This is a remote-mode repository. File contents may not be present for all files\n".to_string().normal();
        
        println!("{}", remote_mode_message);
        println!("{}", remote_mode_sub_message);

        let opts = StagedDataOpts::default();
        Self::ls_files_and_folders(&directory, status, &dir_children, &opts);

        Ok(())
    }
}


impl LsCmd {
   
    // Helper function to format a single line of output
    fn format_line(
        node_type: ColoredString,
        name: ColoredString,
        last_modified_date: &str,
        size: String,
    ) -> String {
        // Define column widths for consistent spacing
        const TYPE_WIDTH: usize = 7;
        const NAME_WIDTH: usize = 40;
        const DATE_WIDTH: usize = 24;
        const SIZE_WIDTH: usize = 12;

        // Truncate name if it exceeds NAME_WIDTH and add an ellipsis
        let name_str = name.to_string();
        let formatted_name = if name_str.len() > NAME_WIDTH {
            format!("{}...", &name_str[..NAME_WIDTH - 4]).normal()
        } else {
            name
        };

        format!(
            "{:<width1$}{:<width2$}{:<width3$}{:<width4$}",
            node_type,
            formatted_name,
            last_modified_date,
            size,
            width1 = TYPE_WIDTH,
            width2 = NAME_WIDTH,
            width3 = DATE_WIDTH,
            width4 = SIZE_WIDTH
        )
    }

    fn ls_files_and_folders(_directory: &PathBuf, status: StagedData, files_and_folders: &Vec<MerkleTreeNode>, opts: &StagedDataOpts) {

        let mut files: Vec<(String, StagedEntryStatus)> = vec![];
        let mut dirs: Vec<(String, StagedEntryStatus)> = vec![];

        // Collect files and dirs, and format the lines
        for node in files_and_folders {
            if let EMerkleTreeNode::Directory(dir_node) = &node.node {
                let dir_name = dir_node.name();
                let datetime = Local.timestamp_opt(dir_node.last_modified_seconds(), dir_node.last_modified_nanoseconds()).unwrap();
                let formatted_date = datetime.format(" %m/%d/%Y %I:%M %p").to_string();
                
                let dir_name_with_count = format!(" {} ({} items)", dir_name, dir_node.num_entries());
                let dir_info = Self::format_line(
                    " [Dir] ".to_string().white().bold(),
                    dir_name_with_count.to_string().white().bold(),
                    &formatted_date,
                    "".to_string(), // Size column is empty for directories
                );

                if status.staged_dirs.contains_key(&PathBuf::from(dir_name)) {
                    let staged_dirs = status.staged_dirs.paths.get(&PathBuf::from(dir_name)).unwrap();
                    if let Some(staged_dir) = staged_dirs.iter().next() {
                         dirs.push((dir_info, staged_dir.status.clone()));
                    }
                } else {
                    dirs.push((dir_info, StagedEntryStatus::Unmodified));
                }
            } else if let EMerkleTreeNode::File(file_node) = &node.node {
                let file_name = file_node.name();
                let datetime = Local.timestamp_opt(file_node.last_modified_seconds(), file_node.last_modified_nanoseconds()).unwrap();
                let formatted_date = datetime.format("%m/%d/%Y %I:%M %p").to_string();
                let size_bytes = file_node.num_bytes();

                let file_info = Self::format_line(
                    "[File]".to_string().normal(),
                    file_name.to_string().normal(),
                    &formatted_date,
                    size_bytes.to_string(),
                );

                if status.staged_files.contains_key(&PathBuf::from(file_name)) {
                    let staged_file = status.staged_files.get(&PathBuf::from(file_name)).unwrap();
                    files.push((file_info, staged_file.status.clone()));
                } else {
                    files.push((file_info, StagedEntryStatus::Unmodified));
                }
            }
        }

        let mut outputs: Vec<ColoredString> = vec![];
        let headers: Vec<String> = vec![
            format!(
                "      {:<6} {:<40} {:<24}{:<12}",
                "Type", "Name", "LastModifiedTime", "Size"
            ),
            format!(
                "      {:<6} {:<40} {:<24}{:<12}",
                "----", "----", "----------------", "----"
            ),
        ];

        outputs.push(format!("{}\n", headers[0]).bold());
        outputs.push(format!("{}\n", headers[1]).bold());

        // Sort each alphabetically
        dirs.sort_by(|(a, _), (b, _)| a.cmp(b));
        files.sort_by(|(a, _), (b, _)| a.cmp(b));

        // Format and collect outputs
        Self::collapse_outputs(
            &dirs,
            |(entry, status)| match status {
                StagedEntryStatus::Removed => {
                    vec![
                        "  -  ".red(),
                        format!("{}\n", entry).into()
                    ]
                }
                StagedEntryStatus::Modified => {
                    vec![
                        "  Δ   ".yellow(),
                        format!("{}\n", entry).into()
                    ]
                }
                StagedEntryStatus::Added => {
                    vec![
                        "  +  ".green(),
                        format!("{}\n", entry).into()
                    ]
                }
                StagedEntryStatus::Unmodified => {
                    vec![
                        "     ".into(),
                        format!("{}\n", entry).into()
                    ]
                }
            },
            &mut outputs,
            opts,
        );

        Self::collapse_outputs(
            &files,
            |(entry, status)| match status {
                StagedEntryStatus::Removed => {
                    vec![
                        "  -  ".red(),
                        format!("{}\n", entry).into()
                    ]
                }
                StagedEntryStatus::Modified => {
                    vec![
                        "  Δ   ".yellow(),
                        format!("{}\n", entry).into()
                    ]
                }
                StagedEntryStatus::Added => {
                    vec![
                        "  +   ".green(),
                        format!("{}\n", entry).into()
                    ]
                }
                StagedEntryStatus::Unmodified => {
                    vec![
                        "      ".into(),
                        format!("{}\n", entry).into()
                    ]
                }
            },
            &mut outputs,
            opts,
        );
        
        for output in outputs {
            print!("{output}");
        }

        println!("\n");
    }
    

    fn collapse_outputs<T, F>(
        inputs: &[T],
        to_components: F,
        outputs: &mut Vec<ColoredString>,
        opts: &StagedDataOpts,
    ) where
        F: Fn(&T) -> Vec<ColoredString>,
    {
        log::debug!(
            "collapse_outputs inputs.len(): {} opts: {:?}",
            inputs.len(),
            opts
        );
        if inputs.is_empty() {
            return;
        }

        let total = opts.skip + opts.limit;
        for (i, input) in inputs.iter().enumerate() {
            if i < opts.skip && !opts.print_all {
                continue;
            }
            if i >= total && !opts.print_all {
                break;
            }
            let mut components = to_components(input);
            outputs.append(&mut components);
        }

        if inputs.len() > opts.limit && !opts.print_all {
            let remaining = inputs.len() - opts.limit;
            outputs.push(format!("  ... and {remaining} others\n").normal());
        }
    }
}



/*

        /*
        let added_file_nodes = vec![];
        let modified_file_nodes = vec![];
        let removed_file_nodes = vec![];
        let unmodified_file_nodes = vec![];

        let added_dir_nodes = vec![];
        let modified_dir_nodes = vec![];
        let removed_dir_nodes = vec![];
        let unmodified_dir_nodes = vec![];
        */

                    /*if staged_files.contains_key(&node.path) {
                    let staged_file = status.staged_files.get(&node.path).unwrap();
                    match staged_file.status {
                        StagedEntryStatus::Added => {
                            added_file_nodes.push(format!("{}{}{}", "added: ".green(), file_name.green(), node_info));
                        }
                        StagedEntryStatus::Modified => {
                            modified_file_nodes.push(format!("{}{}{}", "modified: ".yellow(), file_name.yellow(), node_info));
                        }
                        StagedEntryStatus::Removed => {
                            removed_file_nodes.push(format!("{}{}{}", "removed: ".red(), file_name.red(), node_info));
                        }
                        _ => {
                            unmodified_file_nodes.push(format!("{}{}", file_name.white(), node_info));
                        }
                    }
                } else {
                    unmodified_file_nodes.push(format!("{}{}", file_name.white(), node_info));
                }*/



                /*if status.staged_dirs.paths.contains_key(&node.path) {
                    let staged_dir = status.staged_dirs.paths.get(&node.path).unwrap();
                    match staged_dir.status {
                        StagedEntryStatus::Added => {
                            added_dir_nodes.push(format!("{}{} {}", "added: ".green(), dir_name.green(), node_info));
                        }
                        StagedEntryStatus::Modified => {
                            modified_dir_nodes.push(format!("{}{} {}", "modified: ".yellow(), dir_name.yellow(), node_info));
                        }
                        StagedEntryStatus::Removed => {
                            removed_dir_nodes.push(format!("{}{} {}", "removed: ".red(), dir_name.red(), node_info));
                        }
                        _ => {
                            unmodified_dir_nodes.push(format!("{} {}", dir_name.white(), node_info));
                        }
                    }
                } else {
                    unmodified_dir_nodes.push(format!("{} {}", dir_name.white(), node_info));
                }*/

*/