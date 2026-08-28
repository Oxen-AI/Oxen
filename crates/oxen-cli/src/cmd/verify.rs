use async_trait::async_trait;
use clap::{Arg, ArgMatches, Command};

use crate::helpers::check_repo_migration_needed;
use liboxen::model::LocalRepository;
use liboxen::repositories;
use liboxen::repositories::verify::{Findings, VerifyReport};

use crate::cmd::RunCmd;

pub const NAME: &str = "verify";
pub struct VerifyCmd;

pub fn verify_args() -> Command {
    Command::new(NAME)
        .about("Report commit, merkle tree, and version file corruption. Never modifies the repository")
        .arg(
            Arg::new("json")
                .long("json")
                .help("Print the report as JSON")
                .action(clap::ArgAction::SetTrue),
        )
}

#[async_trait]
impl RunCmd for VerifyCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        verify_args()
    }

    async fn run(&self, args: &ArgMatches) -> Result<(), anyhow::Error> {
        let repository = LocalRepository::from_current_dir()?;
        check_repo_migration_needed(&repository)?;

        let report = repositories::verify::verify_repo(&repository).await?;

        if args.get_flag("json") {
            println!("{}", serde_json::to_string_pretty(&report)?);
        } else {
            print_report(&report);
        }

        // A corrupt repository is a finding, not a failure to run, so the exit code stays 0 and
        // callers branch on the report.
        Ok(())
    }
}

fn print_report(report: &VerifyReport) {
    println!(
        "Checked {} branches, {} commits, {} version files",
        report.branches_checked, report.commits_checked, report.versions_checked
    );

    if report.is_healthy() {
        println!("\nNo problems found.");
        return;
    }

    println!("\nFound {} problem(s):", report.total_findings());
    print_findings(
        "Branches whose head commit is missing",
        &report.dangling_branches,
        |d| format!("{} -> {}", d.branch, d.commit_id),
    );
    print_findings(
        "Commits naming a missing parent",
        &report.dangling_parents,
        |d| format!("{} -> {}", d.commit_id, d.parent_id),
    );
    print_findings(
        "Commits that do not hash to their own id",
        &report.misaddressed_commits,
        |c| format!("{} hashes to {}", c.recorded_id, c.computed_id),
    );
    print_findings("Merkle nodes missing", &report.missing_nodes, |h| h.clone());
    print_findings(
        "Merkle nodes that could not be read",
        &report.unreadable_nodes,
        |u| format!("{}: {}", u.hash, u.error),
    );
    print_findings("Version files missing", &report.missing_versions, |h| {
        h.clone()
    });
    print_findings(
        "Version files that could not be checked",
        &report.unchecked_versions,
        |u| format!("{}: {}", u.hash, u.error),
    );
    print_findings(
        "Version files of the wrong size",
        &report.size_mismatches,
        |m| {
            format!(
                "{} declared {} bytes, stored {} ({})",
                m.path.display(),
                m.declared_bytes,
                m.stored_bytes,
                m.hash
            )
        },
    );
}

fn print_findings<T>(label: &str, findings: &Findings<T>, show: impl Fn(&T) -> String) {
    if findings.is_empty() {
        return;
    }
    println!("\n  {label}: {}", findings.count);
    for item in &findings.sample {
        println!("    {}", show(item));
    }
    let hidden = findings.count - findings.sample.len();
    if hidden > 0 {
        println!("    ... and {hidden} more");
    }
}
