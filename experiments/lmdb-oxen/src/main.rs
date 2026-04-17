use clap::{Parser, Subcommand};

#[allow(dead_code)]
mod existing;
mod explore;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the existing LMDB implementation
    Existing {
        #[command(subcommand)]
        command: existing::old_main::Commands,
    },
    /// Run the explore implementation
    Explore {
        #[command(subcommand)]
        command: explore::new_main::Commands,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Existing { command } => {
            existing::old_main::run(command).unwrap();
        }
        Command::Explore { command } => {
            explore::new_main::run(command).await;
        }
    }
}
