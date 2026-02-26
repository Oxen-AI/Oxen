"""
Pytest configuration and shared fixtures for Oxen CLI tests.
"""

import os
import shutil
import subprocess
import tempfile
import secrets
from pathlib import Path
from typing import Generator, Tuple, Optional, List
import pytest
from dotenv import load_dotenv


# Load environment variables
load_dotenv()


def run_system_command(cmd: str, cwd: Optional[str] = None) -> None:
    """Run a system command and raise an exception if it fails."""
    print(f"Running: {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}\nError: {result.stderr}")
    return result


class CLIRunner:
    """Helper class for running CLI commands."""

    def __init__(self, executable: str = "oxen"):
        self.executable = executable

    def run(self, *args, input_text: Optional[str] = None, cwd: Optional[str] = None,
            check: bool = True, timeout: Optional[int] = None) -> subprocess.CompletedProcess:
        """
        Run the CLI with the given arguments.

        Args:
            *args: Command line arguments to pass to the CLI
            input_text: Optional input text to send to the command
            cwd: Optional working directory
            check: If True, raise an exception if the command fails
            timeout: Optional timeout in seconds

        Returns:
            subprocess.CompletedProcess object with stdout, stderr, and returncode
        """
        cmd = [self.executable] + list(args)
        print(f"Running: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            input=input_text,
            cwd=cwd,
            timeout=timeout
        )

        if check and result.returncode != 0:
            raise RuntimeError(
                f"Command failed with exit code {result.returncode}\n"
                f"Command: {' '.join(cmd)}\n"
                f"Stderr: {result.stderr}"
            )

        return result

    def run_raw(self, cmd: str, cwd: Optional[str] = None, check: bool = True) -> subprocess.CompletedProcess:
        """Run a raw shell command."""
        print(f"Running: {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=cwd)
        if check and result.returncode != 0:
            raise RuntimeError(f"Command failed: {cmd}\nError: {result.stderr}")
        return result


@pytest.fixture
def cli_runner() -> CLIRunner:
    """Fixture that provides a CLI runner."""
    return CLIRunner()


@pytest.fixture
def oxen() -> CLIRunner:
    """Alias for cli_runner specifically for oxen CLI."""
    return CLIRunner("oxen")


@pytest.fixture(autouse=True)
def setup_test_environment(tmp_path):
    """Setup test environment before each test."""
    # Configure oxen user
    run_system_command("oxen config --name python-test --email test@oxen.ai")
    # Try to delete any existing test remote repos
    subprocess.run(
        "oxen delete-remote --name ox/performance-test --host localhost:3000 -y --scheme http",
        shell=True,
        capture_output=True
    )

    yield

    # Cleanup after test
    subprocess.run(
        "oxen delete-remote --name ox/performance-test --host localhost:3000 -y --scheme http",
        shell=True,
        capture_output=True
    )


@pytest.fixture
def test_dir(tmp_path) -> Generator[Path, None, None]:
    """
    Create a temporary test directory and change to it.
    Automatically cleans up and returns to original directory after test.
    """
    original_dir = os.getcwd()
    test_path = tmp_path / "test_workspace"
    test_path.mkdir(parents=True, exist_ok=True)
    os.chdir(test_path)

    yield test_path

    os.chdir(original_dir)


@pytest.fixture
def unique_id() -> str:
    """Generate a unique ID for test resources."""
    return secrets.token_hex(4)


@pytest.fixture
def remote_repo_name(unique_id) -> str:
    """Generate a unique remote repository name."""
    return f"ox/test-{unique_id}"


@pytest.fixture
def init_repo(test_dir, oxen) -> Path:
    """Initialize an oxen repository in the test directory."""
    oxen.run("init")
    return test_dir


def measure_time(func):
    """Decorator to measure execution time of a function."""
    import time
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        elapsed = end - start
        print(f"Execution time: {elapsed:.2f} seconds")
        return result, elapsed
    return wrapper