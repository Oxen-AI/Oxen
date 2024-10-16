use std::collections::HashMap;
use std::process::ExitCode;

use clap::Command;
use liboxen::util;
// use env_logger::Env;

pub mod cmd;
pub mod helpers;

#[tokio::main]
async fn main() -> ExitCode {
    util::logging::init_logging();

    let cmds: Vec<Box<dyn cmd::RunCmd>> = vec![
        Box::new(cmd::AddCmd),
        Box::new(cmd::BranchCmd),
        Box::new(cmd::CheckoutCmd),
        Box::new(cmd::CloneCmd),
        Box::new(cmd::CommitCacheCmd),
        Box::new(cmd::CommitCmd),
        Box::new(cmd::ConfigCmd),
        Box::new(cmd::CreateRemoteCmd),
        Box::new(cmd::DbCmd),
        Box::new(cmd::DeleteRemoteCmd),
        Box::new(cmd::DFCmd),
        Box::new(cmd::DiffCmd),
        Box::new(cmd::DownloadCmd),
        Box::new(cmd::FetchCmd),
        Box::new(cmd::InfoCmd),
        Box::new(cmd::InitCmd),
        Box::new(cmd::LoadCmd),
        Box::new(cmd::LogCmd),
        Box::new(cmd::MergeCmd),
        Box::new(cmd::MigrateCmd),
        Box::new(cmd::MooCmd),
        Box::new(cmd::NodeCmd),
        Box::new(cmd::PackCmd),
        Box::new(cmd::PullCmd),
        Box::new(cmd::PushCmd),
        Box::new(cmd::RestoreCmd),
        Box::new(cmd::ReadLinesCmd),
        Box::new(cmd::RemoteCmd),
        Box::new(cmd::RmCmd),
        Box::new(cmd::SaveCmd),
        Box::new(cmd::SchemasCmd),
        Box::new(cmd::StatusCmd),
        Box::new(cmd::TreeCmd),
        Box::new(cmd::UploadCmd),
        Box::new(cmd::UnpackCmd),
        Box::new(cmd::WorkspaceCmd),
    ];

    let mut command = Command::new("oxen")
        .version(liboxen::constants::OXEN_VERSION)
        .about("🐂 is a machine learning dataset management toolchain")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .allow_external_subcommands(true);

    // Add all the commands to the command line
    let mut runners: HashMap<String, Box<dyn cmd::RunCmd>> = HashMap::new();
    for cmd in cmds {
        command = command.subcommand(cmd.args());
        runners.insert(cmd.name().to_string(), cmd);
    }

    // Parse the command line args and run the appropriate command
    let matches = command.get_matches();
    match matches.subcommand() {
        // TODO: Get these in the help command instead of just falling back
        Some((command, args)) => {
            // Lookup command in runners and run on args
            if let Some(runner) = runners.get(command) {
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
