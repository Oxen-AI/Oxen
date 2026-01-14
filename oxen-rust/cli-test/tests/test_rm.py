"""
Tests for oxen rm command.
Converted from Ruby/RSpec to Python/pytest.
"""

import os
from pathlib import Path
from tests.helpers import create_test_file


class TestRmCommand:
    """Test suite for oxen rm command."""

    def test_rm_with_relative_paths_from_subdirectories(self, test_dir, oxen):
        """Test oxen rm with relative paths from subdirectories."""

        # Setup base repo with capitalized path
        repo_path = test_dir / "Test-Relative-Dirs"
        repo_path.mkdir(parents=True, exist_ok=True)
        os.chdir(repo_path)

        oxen.run("init")

        # Create nested directory structure
        images_test_path = repo_path / "images" / "test"
        images_test_path.mkdir(parents=True, exist_ok=True)

        # Create and commit root file
        create_test_file("root.txt", "root file\n")
        oxen.run("add", "root.txt")
        oxen.run("commit", "-m", "adding root file")

        # Create and commit nested file
        nested_file_path = images_test_path / "nested.txt"
        create_test_file(str(nested_file_path), "nested file\n")

        os.chdir(images_test_path)
        oxen.run("add", "nested.txt")
        oxen.run("commit", "-m", "adding nested file")

        # Test removing file from nested directory using relative path
        parent_path = Path("..") / ".."
        root_txt_path = parent_path / "root.txt"
        oxen.run("rm", str(root_txt_path))

        # Test removing local file
        oxen.run("rm", "nested.txt")

        # Go back to root and verify status
        os.chdir(parent_path)
        result = oxen.run("status")
        assert result.returncode == 0

        # Files should be removed from disk
        assert not Path("root.txt").exists()
        nested_path = Path("images") / "test" / "nested.txt"
        assert not nested_path.exists()

    def test_rm_with_file_already_removed_from_disk(self, test_dir, oxen):
        """Test oxen rm with file that's already been removed from disk."""

        # Setup base repo with capitalized path
        repo_path = test_dir / "Test-Relative-Dirs"
        repo_path.mkdir(parents=True, exist_ok=True)
        os.chdir(repo_path)

        oxen.run("init")

        # Create and commit root file
        create_test_file("root.txt", "root file\n")
        oxen.run("add", "root.txt")
        oxen.run("commit", "-m", "adding root file")

        # Remove file from disk before running oxen rm
        Path("root.txt").unlink()

        # Run oxen rm on already-deleted file
        oxen.run("rm", "root.txt")

        # File should not exist on disk
        assert not Path("root.txt").exists()
