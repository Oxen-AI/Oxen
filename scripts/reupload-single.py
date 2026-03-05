#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "oxenai",
#     "tqdm",
# ]
# ///

"""
Upload a directory of files to a remote Oxen repository in batches.

For each batch, creates a new workspace, adds the files via add_files,
commits the workspace, then moves on to the next batch. Repeats until
every file in the source directory has been committed.
"""

import argparse
import sys
import time
from pathlib import Path

from oxen import RemoteRepo, Workspace
from tqdm import tqdm


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


def collect_file_paths(directory: Path) -> list[Path]:
    """Recursively collect all file paths under directory, sorted for determinism."""
    return sorted(f for f in directory.rglob("*") if f.is_file())


def batch_iter(items: list, size: int):
    """Yield successive chunks of `size` from `items`."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


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
    print(f"\nUpload plan:")
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
        print(f"Created repository '{args.repo}'.")

    fails = [f for f in all_files if not f.is_relative_to(parent)]
    if len(fails) > 0:
        fs = ".\n\t".join([str(f) for f in fails])
        print(
            f"[ERROR] Found {len(fails)} paths that are not relative to the parent: {parent}\n\t{fs}"
        )
        return 1

    print("  Adding files...")
    workspace = Workspace(repo, branch)
    workspace.add_files(parent, all_files)

    # Commit the workspace
    print(f'  Committing: "{message}"')
    commit = workspace.commit(message)
    overall_elapsed = time.time() - overall_start

    print(f"  Commit {commit.id} ({overall_elapsed:.1f}s)")
    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload a files to a remote Oxen repo as a workspace and then commit it.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --repo ox/my-repo --branch main
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

    # Default scheme: http for localhost/127.0.0.1, https otherwise
    if args.scheme is None:
        host_name = (args.host or "").split(":")[0]
        if host_name in ("localhost", "127.0.0.1"):
            args.scheme = "http"
        else:
            args.scheme = "https"

    # Collect all file paths, verifying each exists
    missing = [f for f in args.files if not f.exists()]
    if missing:
        for f in missing:
            print(f"Error: file does not exist: {f}")
        sys.exit(1)
    all_files: list[Path] = [f.resolve() for f in args.files]
    if not all_files:
        print("No files found. Nothing to do.")
        sys.exit(1)

    args.message = args.message.strip()
    if len(args.message) == 0:
        print("Error: cannot have empty commit --message")
        sys.exit(1)

    message_template = args.message
    exit_code = main(
        repo_name=args.repo,
        scheme=args.scheme,
        host=args.host,
        branch=args.branch,
        parent=args.parent.resolve(),
        all_files=all_files,
        message=args.message,
        dry_run=args.dry_run,
    )
    sys.exit(exit_code)
