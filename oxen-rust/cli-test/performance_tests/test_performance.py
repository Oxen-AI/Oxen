"""
Performance tests for oxen CLI.
Converted from Ruby/RSpec to Python/pytest.
"""

import os
from pathlib import Path
import pytest
from performance_tests.helpers import run_command, create_test_file, measure_command_time


@pytest.mark.performance
@pytest.mark.timeout(6000)  # 100 minutes timeout for performance tests
class TestPerformance:
    """Performance test suite for oxen CLI operations."""

    def test_oxen_workflow_with_small_file(self, test_dir, oxen):
        """Test oxen init, add, commit, and push with a small file."""

        # Setup repository
        repo_path = test_dir / "test-small-repo"
        repo_path.mkdir(parents=True, exist_ok=True)
        os.chdir(repo_path)

        # Test init performance
        _, init_time = measure_command_time("oxen init")
        assert init_time < 7.0, f"oxen init took {init_time} seconds, expected < 7.0"

        # Create a simple test file (not running the image generation script for now)
        create_test_file("simple.txt", "This is a simple text file.\n")

        # Test add performance
        _, add_time = measure_command_time("oxen add .")
        assert add_time < 77.0, f"oxen add took {add_time} seconds, expected < 77.0"

        # Test commit performance
        _, commit_time = measure_command_time('oxen commit -m "Add small file"')
        assert commit_time < 85.0, f"oxen commit took {commit_time} seconds, expected < 85.0"

        # Create remote repository
        _, create_remote_time = measure_command_time(
            'oxen create-remote --name EloyMartinez/performance-test '
            '--host dev.hub.oxen.ai --scheme https --is_public'
        )
        assert create_remote_time < 7.5, f"oxen create-remote took {create_remote_time} seconds, expected < 7.5"

        # Set remote
        _, set_remote_time = measure_command_time(
            'oxen config --set-remote origin https://dev.hub.oxen.ai/EloyMartinez/performance-test'
        )
        assert set_remote_time < 5.0, f"oxen config --set-remote took {set_remote_time} seconds, expected < 5.0"

        # Test push performance
        _, push_time = measure_command_time("oxen push")
        assert push_time < 1000.0, f"oxen push took {push_time} seconds, expected < 1000.0"

        # Move to parent directory for clone test
        os.chdir("..")

        # Test clone performance
        _, clone_time = measure_command_time(
            "oxen clone https://dev.hub.oxen.ai/EloyMartinez/performance-test"
        )
        assert clone_time < 700.0, f"oxen clone took {clone_time} seconds, expected < 700.0"

        # Navigate to cloned repo and make changes
        cloned_repo_path = test_dir / "performance-test"
        if cloned_repo_path.exists():
            os.chdir(cloned_repo_path)
        else:
            # If clone created it in current directory
            os.chdir("performance-test")

        # Edit the file
        simple_txt_path = Path("simple.txt")
        if simple_txt_path.exists():
            with open(simple_txt_path, "a") as f:
                f.write("This is a simple text file. Edited remotely.\n")
        else:
            create_test_file("simple.txt", "This is a simple text file. Edited remotely.\n")

        # Test add, commit, push for simple edit
        _, add_simple_time = measure_command_time("oxen add simple.txt")
        assert add_simple_time < 5.0, f"oxen add simple.txt took {add_simple_time} seconds, expected < 5.0"

        _, commit_simple_time = measure_command_time('oxen commit -m "Edit simple text file remotely"')
        assert commit_simple_time < 5.0, f"oxen commit took {commit_simple_time} seconds, expected < 5.0"

        _, push_simple_time = measure_command_time("oxen push")
        assert push_simple_time < 300.0, f"oxen push took {push_simple_time} seconds, expected < 300.0"

        # Go back to original repo and pull
        os.chdir("..")
        os.chdir("test-small-repo")

        _, pull_time = measure_command_time("oxen pull")
        assert pull_time < 125.0, f"oxen pull took {pull_time} seconds, expected < 125.0"

        # Test checkout to new branch
        _, checkout_time = measure_command_time("oxen checkout -b second_branch")
        assert checkout_time < 7.0, f"oxen checkout took {checkout_time} seconds, expected < 7.0"

        # Make another change on the new branch
        with open("simple.txt", "a") as f:
            f.write("This is a simple text file. Edited remotely. this is a second change\n")

        _, add_second_time = measure_command_time("oxen add simple.txt")
        assert add_second_time < 5.0, f"oxen add took {add_second_time} seconds, expected < 5.0"

        _, commit_second_time = measure_command_time('oxen commit -m "Edit simple text file remotely second time"')
        assert commit_second_time < 5.0, f"oxen commit took {commit_second_time} seconds, expected < 5.0"

        _, push_second_time = measure_command_time("oxen push")
        assert push_second_time < 20.0, f"oxen push took {push_second_time} seconds, expected < 20.0"

        # Test branch switching
        _, checkout_main_time = measure_command_time("oxen checkout main")
        assert checkout_main_time < 100.0, f"oxen checkout main took {checkout_main_time} seconds, expected < 100.0"

        _, checkout_second_time = measure_command_time("oxen checkout second_branch")
        assert checkout_second_time < 100.0, f"oxen checkout second_branch took {checkout_second_time} seconds, expected < 100.0"

        # Test restore
        _, restore_time = measure_command_time("oxen restore --source main simple.txt")
        assert restore_time < 7.0, f"oxen restore took {restore_time} seconds, expected < 7.0"

        # Switch back to main
        _, checkout_main_time2 = measure_command_time("oxen checkout main")
        assert checkout_main_time2 < 100.0, f"oxen checkout main took {checkout_main_time2} seconds, expected < 100.0"

        # Create multiple commits
        Path("files").mkdir(exist_ok=True)

        for i in range(30):
            # Update README
            with open("README.md", "w") as f:
                f.write(f"\n\nnew Commit {i}\n")

            # Create numbered file
            with open(f"{i}.txt", "w") as f:
                f.write(f"hello {i} world\n")

            run_command("oxen add .")
            run_command(f'oxen commit -m "commit {i}"')
            print(f"Completed commit {i + 1} of 30")

        # Test final push with multiple commits
        _, final_push_time = measure_command_time("oxen push")
        assert final_push_time < 60.0, f"oxen push (30 commits) took {final_push_time} seconds, expected < 60.0"
