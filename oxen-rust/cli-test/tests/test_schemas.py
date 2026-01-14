"""
Tests for oxen schemas add command.
Converted from Ruby/RSpec to Python/pytest.
"""

import os
import json
import subprocess
from pathlib import Path
from tests.helpers import create_csv_file


class TestSchemasCommand:
    """Test suite for oxen schemas commands."""

    def test_schemas_add_with_relative_paths_from_subdirectories(self, test_dir, oxen):
        """Test oxen schemas add with relative paths from subdirectories."""

        # Setup base repo with capitalized path
        repo_path = test_dir / "Test-Relative-Dirs"
        repo_path.mkdir(parents=True, exist_ok=True)
        os.chdir(repo_path)

        oxen.run("init")

        # Create nested directory structure
        data_frames_path = repo_path / "data" / "frames"
        data_frames_path.mkdir(parents=True, exist_ok=True)

        # Create root CSV file
        create_csv_file(
            "root.csv",
            ["id", "image", "description"],
            [
                [1, "/path/to/img1.jpg", "test image 1"],
                [2, "/path/to/img2.jpg", "test image 2"]
            ]
        )

        oxen.run("add", "root.csv")
        oxen.run("commit", "-m", "adding root csv")

        # Create CSV file in nested directory
        test_csv_path = data_frames_path / "test.csv"
        create_csv_file(
            str(test_csv_path),
            ["id", "image", "description"],
            [
                [1, "/path/to/img1.jpg", "test image 1"],
                [2, "/path/to/img2.jpg", "test image 2"]
            ]
        )

        # Navigate to nested directory
        os.chdir(data_frames_path)

        # Add and commit the CSV file
        oxen.run("add", "test.csv")
        oxen.run("commit", "-m", "adding test csv")

        # Prepare metadata for schema
        metadata = {
            "_oxen": {
                "render": {
                    "func": "image"
                }
            }
        }
        json_string = json.dumps(metadata)

        # Add schema to local file
        result = subprocess.run(
            ["oxen", "schemas", "add", "test.csv", "-c", "image", "-m", json_string],
            capture_output=True,
            text=True,
            cwd=str(data_frames_path)
        )
        assert result.returncode == 0, f"Failed to add schema to test.csv: {result.stderr}"

        # Add schema to root file using relative path
        root_path = Path("..") / ".." / "root.csv"
        result = subprocess.run(
            ["oxen", "schemas", "add", str(root_path), "-c", "image", "-m", json_string],
            capture_output=True,
            text=True,
            cwd=str(data_frames_path)
        )
        assert result.returncode == 0, f"Failed to add schema to root.csv: {result.stderr}"

        # Verify schema changes in status
        result = subprocess.run(
            ["oxen", "status"],
            capture_output=True,
            text=True,
            cwd=str(data_frames_path)
        )
        assert result.returncode == 0

        # Check that "Schemas to be committed" appears in status output
        output_lines = result.stdout.split("\n")
        assert any("Schemas to be committed" in line for line in output_lines), \
            f"Expected 'Schemas to be committed' in status output, got:\n{result.stdout}"
