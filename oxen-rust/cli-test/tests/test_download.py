"""
Tests for oxen download command.
Converted from Ruby/RSpec to Python/pytest.
"""

import os
from pathlib import Path
from tests.helpers import create_test_file, create_remote_repo, cleanup_remote_repo


class TestDownloadCommand:
    """Test suite for oxen download command."""

    def test_download_single_file(self, test_dir, oxen, unique_id):
        """Test oxen download works for a single file."""

        # Setup repository
        repo_path = test_dir / f"test-relative-paths-{unique_id}"
        repo_path.mkdir(parents=True, exist_ok=True)
        os.chdir(repo_path)

        oxen.run("init")

        # Create and add file
        create_test_file("hi.txt", "This is a simple text file.\n")
        oxen.run("add", "hi.txt")
        oxen.run("commit", "-m", "add hi.txt")

        # Create remote repository
        remote_repo_name = f"ox/hi-{unique_id}"
        create_remote_repo(remote_repo_name)

        # Push to remote
        oxen.run("push", "origin", "main")

        # Download the file to a different name
        oxen.run(
            "download", remote_repo_name, "hi.txt",
            "-o", "hi2.txt",
            "--host", "localhost:3000",
            "--scheme", "http"
        )

        # Verify downloaded file contents
        downloaded_content = Path("hi2.txt").read_text()
        assert downloaded_content == "This is a simple text file.\n"

        # Cleanup remote repository
        cleanup_remote_repo(remote_repo_name)
