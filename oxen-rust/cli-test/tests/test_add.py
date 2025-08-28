"""
Tests for oxen add command with relative paths.
Converted from Ruby/RSpec to Python/pytest.
"""

import os
from pathlib import Path
from tests.helpers import create_test_file, create_remote_repo, cleanup_remote_repo


class TestAddCommand:
    """Test suite for oxen add command."""

    def test_add_with_relative_paths_from_subdirectories(self, test_dir, oxen):
        """Test oxen add with relative paths from subdirectories."""

        # Initialize repository with capitalized path (testing case sensitivity)
        # Note: macOS is case-insensitive by default, but we test the pattern
        repo_path = test_dir / "Test-Relative-Paths"
        repo_path.mkdir(parents=True, exist_ok=True)
        os.chdir(repo_path)

        oxen.run("init")

        # Create a file in root
        create_test_file("hi.txt", "This is a simple text file.\n")

        # Create nested directory structure
        images_test_path = repo_path / "images" / "test"
        images_test_path.mkdir(parents=True, exist_ok=True)

        # Navigate to nested directory
        os.chdir(images_test_path)

        # Add file using relative path from nested directory
        parent_path = Path("..") / ".."
        file_path = parent_path / "hi.txt"
        oxen.run("add", str(file_path))

        # Create another file in nested directory
        create_test_file("nested.txt", "nested\n")

        # Add file from current directory
        oxen.run("add", "nested.txt")

        # Go back to root
        os.chdir(parent_path)

        # Verify status
        result = oxen.run("status")
        assert result.returncode == 0

        # Verify file contents
        assert Path("hi.txt").read_text() == "This is a simple text file.\n"
        nested_path = Path("images") / "test" / "nested.txt"
        assert nested_path.read_text() == "nested\n"

    def test_add_with_remote_mode_repo(self, test_dir, oxen, unique_id):
        """Test oxen add with remote mode repository."""

        # Setup base repo
        repo_path = test_dir / "test-relative-paths"
        repo_path.mkdir(parents=True, exist_ok=True)
        os.chdir(repo_path)

        oxen.run("init")

        # Create and add first file
        create_test_file("first_file.txt", "first_text\n")
        oxen.run("add", "first_file.txt")
        oxen.run("commit", "-m", "add first_file.txt")

        # Create remote repository
        remote_repo_name = f"ox/hi-{unique_id}"
        local_repo_name = f"hi-{unique_id}"

        remote_url = create_remote_repo(remote_repo_name)

        # Push to remote
        oxen.run("push", "origin", "main")

        # Setup clone directory
        os.chdir("..")
        clone_path = test_dir / "test-clone"
        clone_path.mkdir(parents=True, exist_ok=True)
        os.chdir(clone_path)

        # Clone repo in remote mode
        oxen.run("clone", "--remote", remote_url)
        os.chdir(local_repo_name)

        # Create and add new file
        create_test_file("hi.txt", "This is a simple text file.\n")
        oxen.run("add", "hi.txt")

        # Verify status
        result = oxen.run("status")
        assert result.returncode == 0

        # Verify file contents
        assert Path("hi.txt").read_text() == "This is a simple text file.\n"

        # Cleanup remote repo
        cleanup_remote_repo(remote_repo_name)
