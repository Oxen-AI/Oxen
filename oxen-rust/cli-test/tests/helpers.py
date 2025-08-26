"""
Helper functions for Oxen CLI tests.
"""

import os
import time
import subprocess
from pathlib import Path
from typing import Optional, Tuple


def run_command(cmd: str, cwd: Optional[str] = None, check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command and return the result."""
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}\nError: {result.stderr}")
    return result


def create_test_file(path: str, content: str) -> Path:
    """Create a test file with the given content."""
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content)
    return file_path


def create_csv_file(path: str, headers: list, rows: list) -> Path:
    """Create a CSV file with the given headers and rows."""
    content_lines = [','.join(headers)]
    for row in rows:
        content_lines.append(','.join(str(item) for item in row))
    return create_test_file(path, '\n'.join(content_lines))


def create_remote_repo(repo_name: str, host: str = "localhost:3000", scheme: str = "http") -> str:
    """Create a remote repository and return its URL."""
    run_command(f"oxen create-remote --name {repo_name} --host {host} --scheme {scheme}")
    url = f"{scheme}://{host}/{repo_name}"
    run_command(f"oxen config --set-remote origin {url}")
    return url


def cleanup_remote_repo(repo_name: str, host: str = "localhost:3000", scheme: str = "http"):
    """Delete a remote repository."""
    subprocess.run(
        f"oxen delete-remote --name {repo_name} --host {host} -y --scheme {scheme}",
        shell=True,
        capture_output=True
    )
