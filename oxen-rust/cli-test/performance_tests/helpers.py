"""
Helper functions for Oxen CLI tests.
"""

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


def measure_command_time(cmd: str, cwd: Optional[str] = None) -> Tuple[subprocess.CompletedProcess, float]:
    """
    Run a command and measure its execution time.

    Returns:
        Tuple of (result, elapsed_time_in_seconds)
    """
    start_time = time.time()
    result = run_command(cmd, cwd=cwd)
    elapsed_time = time.time() - start_time
    print(f"Command '{cmd}' took {elapsed_time:.2f} seconds")
    return result, elapsed_time
