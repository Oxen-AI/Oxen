use std::collections::HashMap;
use std::process::ExitCode;

use crate::cmd::RemoteModeCmd;
use crate::cmd::WorkspaceCmd;
use clap::Command;
use liboxen::model::LocalRepository;
use liboxen::util;
// use env_logger::Env;

pub mod cmd;
pub mod helpers;

const SHORT_ABOUT: &str = "🐂 is the AI and machine learning data management toolchain";

const LONG_ABOUT: &str = "
🐂 is the AI and machine learning data management toolchain

    📖 If this is your first time using Oxen, check out the CLI docs at:
            https://docs.oxen.ai/getting-started/cli

    💬 For more support, or to chat with the Oxen team, join our Discord:
            https://discord.gg/s3tBEn7Ptg
";

#[tokio::main]
async fn main() -> ExitCode {
    util::logging::init_logging();

    let cmds: Vec<Box<dyn cmd::RunCmd>> = vec![
        Box::new(cmd::AddCmd),
        Box::new(cmd::BranchCmd),
        Box::new(cmd::CheckoutCmd),
        Box::new(cmd::CloneCmd),
        Box::new(cmd::CommitCmd),
        Box::new(cmd::ConfigCmd),
        Box::new(cmd::CreateRemoteCmd),
        Box::new(cmd::DbCmd),
        Box::new(cmd::DeleteRemoteCmd),
        Box::new(cmd::DFCmd),
        Box::new(cmd::DiffCmd),
        Box::new(cmd::DownloadCmd),
        Box::new(cmd::FetchCmd),
        Box::new(cmd::EmbeddingsCmd),
        Box::new(cmd::InfoCmd),
        Box::new(cmd::InitCmd),
        Box::new(cmd::LoadCmd),
        Box::new(cmd::LogCmd),
        Box::new(cmd::LsCmd),
        Box::new(cmd::MergeCmd),
        Box::new(cmd::MigrateCmd),
        Box::new(cmd::MooCmd),
        Box::new(cmd::NodeCmd),
        Box::new(cmd::NotebookCmd),
        // Box::new(cmd::PackCmd),
        Box::new(cmd::PullCmd),
        Box::new(cmd::PushCmd),
        Box::new(cmd::RestoreCmd),
        Box::new(cmd::RemoteCmd),
        Box::new(cmd::RmCmd),
        Box::new(cmd::SaveCmd),
        Box::new(cmd::SchemasCmd),
        Box::new(cmd::StatusCmd),
        Box::new(cmd::TreeCmd),
        Box::new(cmd::UploadCmd),
        // Box::new(cmd::UnpackCmd),
        Box::new(cmd::WorkspaceCmd),
    ];

    let mut command = Command::new("oxen")
        .version(liboxen::constants::OXEN_VERSION)
        .about(SHORT_ABOUT)
        .long_about(LONG_ABOUT)
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true);

    // Add all the commands to the command line
    let mut runners: HashMap<String, Box<dyn cmd::RunCmd>> = HashMap::new();
    for cmd in cmds {
        command = command.subcommand(cmd.args());
        runners.insert(cmd.name().to_string(), cmd);
    }

    let is_remote_repo = match LocalRepository::from_current_dir() {
        Ok(repo) => repo.is_remote_mode(),
        Err(_) => false,
    };

    // Parse the command line args and run the appropriate command
    let matches = command.get_matches();
    match matches.subcommand() {
        // TODO: Get these in the help command instead of just falling back
        Some((command, args)) => {
            // Lookup command in runners and run on args
            if let Some(runner) = runners.get(command) {
                // If in a remote-mode repo, re-route to correct command
                if is_remote_repo {
                    match command {
                        // Workspace commands
                        "add" | "df" | "diff" | "rm" => {
                            match WorkspaceCmd::run_subcommands(command, args).await {
                                Ok(_) => {}
                                Err(err) => {
                                    eprintln!("{err}");
                                    return ExitCode::FAILURE;
                                }
                            }

                            return ExitCode::SUCCESS;
                        }
                        // Remote-mode specific commands
                        "commit" | "checkout" | "restore" | "status" => {
                            match RemoteModeCmd::run_subcommands(command, args).await {
                                Ok(_) => {}
                                Err(err) => {
                                    eprintln!("{err}");
                                    return ExitCode::FAILURE;
                                }
                            }
                            return ExitCode::SUCCESS;
                        }
                        // Disallowed commands
                        "embeddings" | "merge" | "push" | "pull" | "schemas" | "workspace" => {
                            eprintln!("Command `oxen {command}` not implemented for remote-mode repositories");
                            return ExitCode::FAILURE;
                        }
                        _ => {} // All other commands behave as normal
                    }
                }

                match runner.run(args).await {
                    Ok(_) => {}
                    Err(err) => {
                        eprintln!("{err}");
                        return ExitCode::FAILURE;
                    }
                }
            } else {
                eprintln!("Unknown command `oxen {command}`");
                return ExitCode::FAILURE;
            }
        }
        _ => unreachable!(), // If all subcommands are defined above, anything else is unreachable!()
    }

    ExitCode::SUCCESS
}
