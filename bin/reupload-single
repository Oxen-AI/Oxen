#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "oxenai",
#     "tqdm",
# ]
# ///

"""
Upload an explicit list of files to a remote Oxen repository in a single operation.

Creates one workspace, adds all specified files via a single add_files call,
and commits the workspace. Unlike batch-workspace-upload.py, this script does
not batch or iterate — it performs exactly one add and one commit for the
provided file list.
"""

import argparse
import sys
import time
from pathlib import Path

from oxen import RemoteRepo, Workspace


def positive_int(value: str) -> int:
    """Parse and validate a positive integer with support for k/K, m/M suffixes."""
    value = value.strip().upper()
    multipliers = {"K": 1_000, "M": 1_000_000}
    for suffix, multiplier in multipliers.items():
        if value.endswith(suffix):
            try:
                ivalue = int(float(value[:-1]) * multiplier)
            except ValueError:
                raise argparse.ArgumentTypeError(
                    f"Invalid format: {value}. Use formats like: 100, 10k, 1M"
                )
            if ivalue <= 0:
                raise argparse.ArgumentTypeError(f"{value} must be positive")
            return ivalue
    try:
        ivalue = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid format: {value}. Use formats like: 100, 10k, 1M"
        )
    if ivalue <= 0:
        raise argparse.ArgumentTypeError(f"{value} must be positive")
    return ivalue


def main(
    *,
    repo_name: str,
    scheme: str,
    host: str | None,
    branch: str,
    parent: Path,
    all_files: list[Path],
    message: str,
    dry_run: bool = False,
) -> int:
    # Print plan
    print("\nUpload plan:")
    print(f"  Repository:      {repo_name}")
    if host:
        print(f"  Host:            {scheme}://{host}")
    print(f"  Branch:          {branch}")
    print(f"  Parent dir:      {parent}")
    print(f"  Total files:     {len(all_files)}")
    print(f"  Commit message:  {message}")
    print()

    if dry_run:
        return 0

    overall_start = time.time()

    # Connect to the remote repo, creating it if it doesn't exist
    if host:
        repo = RemoteRepo(repo_name, host=host, scheme=scheme)
    else:
        repo = RemoteRepo(repo_name, scheme=scheme)

    if not repo.exists():
        print(f"Repository '{repo}' not found on remote. Creating...")
        repo.create(empty=True, is_public=True)
        print(f"Created repository '{repo_name}'.")

    fails = [f for f in all_files if not f.is_relative_to(parent)]
    if len(fails) > 0:
        fs = ".\n\t".join([str(f) for f in fails])
        print(
            f"[ERROR] Found {len(fails)} paths that are not relative to the parent: {parent}\n\t{fs}"
        )
        return 1

    print("  Adding files...")
    workspace = Workspace(repo, branch)
    failed = workspace.add_files(parent, all_files)

    if failed:
        print(f"[ERROR] {len(failed)} file(s) failed to upload:")
        for f in failed:
            print(f"  - {f}")
        return 1

    # Commit the workspace
    print(f'  Committing: "{message}"')
    commit = workspace.commit(message)
    overall_elapsed = time.time() - overall_start

    print(f"  Commit {commit.id} ({overall_elapsed:.1f}s)")
    print()
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload files to a remote Oxen repo as a workspace and then commit it.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --repo ox/my-repo --branch main
  %(prog)s --host hub.oxen.ai --repo ox/my-repo --branch main
        """,
    )

    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Oxen server host (e.g., hub.oxen.ai). Uses OXEN_HOST env var or default if omitted.",
    )
    parser.add_argument(
        "--scheme",
        type=str,
        choices=["http", "https"],
        default=None,
        help="Connection scheme. Defaults to http for localhost/127.0.0.1, https otherwise.",
    )
    parser.add_argument(
        "--repo",
        type=str,
        required=True,
        help="Remote repository name (e.g., ox/my-repo)",
    )
    parser.add_argument(
        "--branch",
        type=str,
        default="main",
        help="Branch to commit to",
    )
    parser.add_argument(
        "--message",
        type=str,
        default="Adding files.",
        help="Commit message.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the upload plan without actually uploading anything",
    )
    parser.add_argument(
        "--parent",
        required=True,
        type=Path,
        help="The root of the files being uploaded.",
    )
    parser.add_argument(
        "files",
        nargs="+",
        type=Path,
        help="Local file paths to upload",
    )

    args = parser.parse_args()

    # Pull all arguments into named, typed variables
    arg_host: str | None = args.host
    arg_scheme: str | None = args.scheme
    arg_repo: str = args.repo
    arg_branch: str = args.branch
    arg_message: str = args.message
    arg_dry_run: bool = args.dry_run
    arg_parent: Path = args.parent
    arg_files: list[Path] = args.files

    # Default scheme: http for localhost/127.0.0.1, https otherwise
    if arg_scheme is None:
        host_name = (arg_host or "").split(":")[0]
        if host_name in ("localhost", "127.0.0.1"):
            arg_scheme = "http"
        else:
            arg_scheme = "https"

    # Collect all file paths, verifying each exists
    missing = [f for f in arg_files if not f.exists()]
    if missing:
        for f in missing:
            print(f"Error: file does not exist: {f}")
        sys.exit(1)
    all_files: list[Path] = [f.resolve() for f in arg_files]
    if not all_files:
        print("No files found. Nothing to do.")
        sys.exit(1)

    arg_message = arg_message.strip()
    if len(arg_message) == 0:
        print("Error: cannot have empty commit --message")
        sys.exit(1)

    exit_code = main(
        repo_name=arg_repo,
        scheme=arg_scheme,
        host=arg_host,
        branch=arg_branch,
        parent=arg_parent.resolve(),
        all_files=all_files,
        message=arg_message,
        dry_run=arg_dry_run,
    )
    sys.exit(exit_code)
