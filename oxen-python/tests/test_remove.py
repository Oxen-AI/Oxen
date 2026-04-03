import os
import shutil


def test_remove_file(empty_local_repo):
    """Test removal of a single file at the root."""
    repo = empty_local_repo

    # Create and stage a file
    file_path = os.path.join(repo.path, "file.txt")
    with open(file_path, "w") as f:
        f.write("hello")

    repo.add(file_path)
    status = repo.status()
    assert len(status.added_files()) == 1
    assert len(status.removed_files()) == 0

    repo.commit("added file.txt")
    assert os.path.isfile(file_path)

    # Remove the file from the filesystem and stage the removal
    os.remove(file_path)
    repo.rm("file.txt")
    status = repo.status()
    assert len(status.added_files()) == 1  # staged (for removal)
    assert len(status.removed_files()) == 0  # no unstaged removals

    repo.commit("removed file.txt")

    status = repo.status()
    assert status.is_clean()
    assert not os.path.exists(file_path)

    history = repo.log()
    assert len(history) == 2


def test_remove_dir_depth_1(empty_local_repo):
    """Test removal of a single directory at the root containing a single file."""
    repo = empty_local_repo

    # Create directory with a file
    dir_path = os.path.join(repo.path, "1")
    os.makedirs(dir_path, exist_ok=True)
    file_path = os.path.join(dir_path, "file.txt")
    with open(file_path, "w") as f:
        f.write("hello world!")

    repo.add(file_path)
    status = repo.status()
    assert len(status.added_files()) == 1
    repo.commit("added 1/file.txt")

    assert os.path.isdir(dir_path)
    assert os.path.isfile(file_path)

    # Remove the directory and stage the removal
    shutil.rmtree(dir_path)
    repo.rm("1", recursive=True)
    status = repo.status()
    assert len(status.added_files()) == 1  # staged (for removal)
    assert len(status.removed_files()) == 0

    repo.commit("removed dir 1/")

    status = repo.status()
    assert status.is_clean()
    assert not os.path.exists(dir_path)

    history = repo.log()
    assert len(history) == 2


def test_remove_dir_depth_3(empty_local_repo):
    """Test removal of nested directories (depth 3) and their contents."""
    repo = empty_local_repo

    # Create nested directory structure: 1/2/3/file.txt
    dir_1 = os.path.join(repo.path, "1")
    dir_2 = os.path.join(dir_1, "2")
    dir_3 = os.path.join(dir_2, "3")
    file_path = os.path.join(dir_3, "file.txt")

    os.makedirs(dir_3, exist_ok=True)
    with open(file_path, "w") as f:
        f.write("hello world!")

    repo.add(file_path)
    status = repo.status()
    assert len(status.added_files()) == 1
    repo.commit("added 1/2/3/file.txt")

    assert os.path.isdir(dir_1)
    assert os.path.isdir(dir_2)
    assert os.path.isdir(dir_3)
    assert os.path.isfile(file_path)

    # Remove the top-level directory and stage the removal
    shutil.rmtree(dir_1)
    repo.rm("1", recursive=True)
    status = repo.status()
    assert len(status.added_files()) == 1  # staged (for removal)
    assert len(status.removed_files()) == 0

    repo.commit("removed dir 1/ (depth 3)")

    status = repo.status()
    assert status.is_clean()
    assert not os.path.exists(dir_1)
    assert not os.path.exists(dir_2)
    assert not os.path.exists(dir_3)
    assert not os.path.exists(file_path)

    history = repo.log()
    assert len(history) == 2
