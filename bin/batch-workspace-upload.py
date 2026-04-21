#!/usr/bin/env -S uv run --script
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
    host: str | None,
    scheme: str,
    repo_name: str,
    branch: str,
    directory: str,
    batch_size: int,
    message: str | None,
    dry_run: bool,
) -> None:
    directory_path = Path(directory)
    if not directory_path.is_dir():
        print(f"Error: '{directory_path}' is not a directory or does not exist.")
        sys.exit(1)

    # Collect all file paths
    print(f"Scanning '{directory_path}' for files...")
    all_files = collect_file_paths(directory_path)
    if not all_files:
        print("No files found. Nothing to do.")
        sys.exit(0)

    total_files = len(all_files)
    batches = list(batch_iter(all_files, batch_size))
    total_batches = len(batches)

    message_template: str = message or "batch {batch}/{total_batches}"

    # Print plan
    print("\nUpload plan:")
    print(f"  Repository:    {repo_name}")
    if host:
        print(f"  Host:          {scheme}://{host}")
    print(f"  Branch:        {branch}")
    print(f"  Source:        {directory_path}")
    print(f"  Total files:   {total_files:,}")
    print(f"  Batch size:    {batch_size:,}")
    print(f"  Total batches: {total_batches}")
    print()

    if dry_run:
        for i, batch in enumerate(batches, 1):
            msg = message_template.format(batch=i, total_batches=total_batches)
            print(f'  Batch {i}/{total_batches}: {len(batch):,} files — "{msg}"')
        print("\n(dry-run) No files were uploaded.")
        return

    # Connect to the remote repo, creating it if it doesn't exist
    if host:
        repo = RemoteRepo(repo_name, host=host, scheme=scheme)
    else:
        repo = RemoteRepo(repo_name, scheme=scheme)

    if not repo.exists():
        print(f"Repository '{repo_name}' not found on remote. Creating...")
        repo.create(empty=True, is_public=True)
        print(f"Created repository '{repo_name}'.")

    overall_start = time.time()
    all_failures: list = []

    for i, batch in enumerate(batches, 1):
        msg = message_template.format(batch=i, total_batches=total_batches)
        print(f"--- Batch {i}/{total_batches} ({len(batch):,} files) ---")

        batch_start = time.time()

        # Create a fresh workspace for this batch
        workspace = Workspace(repo, branch)

        # Add all files in the batch, preserving directory structure relative
        # to the source directory.
        print("  Adding files...")
        result = workspace.add_files(directory_path, batch)

        # Handle add_files returning None, a list, or any iterable
        batch_failures: list = []
        if result is not None:
            try:
                batch_failures = list(result)
            except TypeError:
                batch_failures = []

        if batch_failures:
            print(f"ERROR: failed to add these {len(batch_failures)} files:")
            for j, error in enumerate(batch_failures):
                print(f"\t[{j + 1}] {error.path} ({error.hash}): {error.error}")
            all_failures.extend(batch_failures)

        status = workspace.status()
        if status.is_clean():
            batch_elapsed = time.time() - batch_start
            print(
                f"  Workspace is clean. Skipping commit. (batch elapsed: {batch_elapsed:.1f}s)"
            )
            continue

        # Commit the workspace
        print(f'  Committing: "{msg}"')
        commit = workspace.commit(msg)
        batch_elapsed = time.time() - batch_start

        print(f"  Commit {commit.id} ({batch_elapsed:.1f}s)")
        print()

    overall_elapsed = time.time() - overall_start
    successful = total_files - len(all_failures)
    print(
        f"Done. {successful:,}/{total_files:,} files uploaded in {total_batches} batches "
        f"({len(all_failures):,} failed, {overall_elapsed:.1f}s total)."
    )
    if all_failures:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload a directory to a remote Oxen repo in batched workspace commits.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --repo ox/my-repo --branch main --directory ./dataset --batch-size 1000
  %(prog)s --repo ox/my-repo --branch main --directory ./dataset --batch-size 10k
  %(prog)s --host hub.oxen.ai --repo ox/my-repo --branch main --directory /data --batch-size 500
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
        required=True,
        help="Branch to commit to",
    )
    parser.add_argument(
        "--directory",
        type=str,
        required=True,
        help="Local directory containing files to upload",
    )
    parser.add_argument(
        "--batch-size",
        type=positive_int,
        required=True,
        help="Number of files per workspace batch (e.g., 1000, 10k)",
    )
    parser.add_argument(
        "--message",
        type=str,
        default=None,
        help="Commit message template. Use {batch} and {total_batches} for numbering. "
        '(default: "batch {batch}/{total_batches}")',
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the upload plan without actually uploading anything",
    )

    args = parser.parse_args()

    # Pull all arguments into named, typed variables
    arg_host: str | None = args.host
    arg_scheme: str | None = args.scheme
    arg_repo: str = args.repo
    arg_branch: str = args.branch
    arg_directory: str = args.directory
    arg_batch_size: int = args.batch_size
    arg_message: str | None = args.message
    arg_dry_run: bool = args.dry_run

    # Default scheme: http for localhost/127.0.0.1, https otherwise
    if arg_scheme is None:
        host_name = (arg_host or "").split(":")[0]
        if host_name in ("localhost", "127.0.0.1"):
            arg_scheme = "http"
        else:
            arg_scheme = "https"

    main(
        host=arg_host,
        scheme=arg_scheme,
        repo_name=arg_repo,
        branch=arg_branch,
        directory=arg_directory,
        batch_size=arg_batch_size,
        message=arg_message,
        dry_run=arg_dry_run,
    )
