import os
from pathlib import PurePath
from typing import Tuple
from oxen import Repo, RemoteRepo


def test_remote_repo_exists(empty_remote_repo):
    exists = True
    assert empty_remote_repo.exists() == exists


def test_remote_create_checkout_branch(celeba_remote_repo_one_image_pushed):
    _local_repo, remote_repo = celeba_remote_repo_one_image_pushed
    remote_repo.create_checkout_branch("test-branch")
    assert remote_repo.revision == "test-branch"


def test_remote_repo_add(
    celeba_remote_repo_one_image_pushed: Tuple[Repo, RemoteRepo], shared_datadir
):
    _local_repo, remote_repo = celeba_remote_repo_one_image_pushed
    examples_path = str(PurePath("ChatBot", "examples.tsv"))
    full_path = os.path.join(shared_datadir, examples_path)
    remote_repo.add(full_path)
    status = remote_repo.status()
    added_files = status.added_files()
    assert len(added_files) == 1
    assert added_files[0] == "examples.tsv"
    commit = remote_repo.commit("Adding my image to the remote workspace.")
    print(commit)
    assert commit.id is not None
    assert commit.message == "Adding my image to the remote workspace."


def test_remote_repo_add_on_branch(
    celeba_remote_repo_one_image_pushed: Tuple[Repo, RemoteRepo], shared_datadir
):
    _local_repo, remote_repo = celeba_remote_repo_one_image_pushed
    file_path = "examples.tsv"
    relative_path = os.path.join("ChatBot", file_path)
    full_path = os.path.join(shared_datadir, relative_path)
    remote_repo.create_checkout_branch("test-branch")
    remote_repo.add(full_path)
    status = remote_repo.status()
    added_files = status.added_files()
    assert len(added_files) == 1
    assert added_files[0] == "examples.tsv"
    commit = remote_repo.commit("Adding a tsv to the remote workspace.")
    print(commit)
    assert commit.id is not None
    assert commit.message == "Adding a tsv to the remote workspace."

    # make sure the file exists on this branch and is not on the main branch
    assert remote_repo.file_exists(file_path)
    assert remote_repo.file_exists(file_path, "test-branch")
    assert not remote_repo.file_exists(file_path, "main")


def test_remote_repo_branch_exists(
    celeba_remote_repo_one_image_pushed: Tuple[Repo, RemoteRepo], shared_datadir
):
    _local_repo, remote_repo = celeba_remote_repo_one_image_pushed
    remote_repo.create_checkout_branch("test-branch")

    assert remote_repo.branch_exists("main")
    assert remote_repo.branch_exists("test-branch")
    assert not remote_repo.branch_exists("non-existent-branch")


def test_remote_repo_file_has_changes(
    celeba_remote_repo_one_image_pushed: Tuple[Repo, RemoteRepo], shared_datadir
):
    _local_repo, remote_repo = celeba_remote_repo_one_image_pushed
    file_path = "examples.tsv"
    relative_path = os.path.join("ChatBot", file_path)
    full_path = os.path.join(shared_datadir, relative_path)
    remote_repo.create_checkout_branch("test-branch")
    remote_repo.add(full_path)
    remote_repo.commit("Adding my image to the remote workspace.")

    # Make sure the file has no changes
    assert not remote_repo.file_has_changes(full_path, remote_path=file_path)

    # Modify the file
    with open(full_path, "w") as f:
        f.write("blowing\tup\tthe\ttsv\tfile")

    # Check if the file has changes
    assert remote_repo.file_has_changes(full_path, remote_path=file_path)
    assert remote_repo.file_has_changes(
        full_path, remote_path=file_path, revision="test-branch"
    )


def test_remote_repo_file_has_changes_file_does_not_exist(
    celeba_remote_repo_one_image_pushed: Tuple[Repo, RemoteRepo], shared_datadir
):
    _local_repo, remote_repo = celeba_remote_repo_one_image_pushed
    file_path = "non-existent-file.tsv"
    relative_path = os.path.join("ChatBot", file_path)
    full_path = os.path.join(shared_datadir, relative_path)
    remote_repo.create_checkout_branch("test-branch")

    # Make sure the file has no changes
    assert remote_repo.file_has_changes(full_path, remote_path=file_path)


def test_remote_repo_dir_download_with_large_files(
    parquet_files_remote_repo_fully_pushed: Tuple[Repo, RemoteRepo]
):
    local_repo, remote_repo = parquet_files_remote_repo_fully_pushed
    tmp_dir = os.path.join(local_repo.path, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)
    print(tmp_dir)
    # download the directory one/two/three
    download_dir = os.path.join("one", "two", "three")
    remote_repo.download(download_dir, dst=tmp_dir, revision="main")
    download_path = os.path.join(tmp_dir, "one", "two", "three", "wiki_30k.parquet")
    assert os.path.exists(download_path)

    # make sure that the file is the same as the original
    with open(download_path, "rb") as f:
        original_file = f.read()
    with open(
        os.path.join(local_repo.path, "one", "two", "three", "wiki_30k.parquet"), "rb"
    ) as f:
        downloaded_file = f.read()
    assert len(original_file) == len(downloaded_file)
