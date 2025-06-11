import pytest

from oxen import Repo
from oxen.fs import rcount_files_in_repo_dir


# Alias the fixtures just to make it a little easier to read
@pytest.fixture
def local_repo(celeba_local_repo_one_image_committed):
    yield celeba_local_repo_one_image_committed


@pytest.fixture
def remote_repo(empty_remote_repo):
    yield empty_remote_repo


def test_repo_push_pull(local_repo, remote_repo, empty_local_dir):
    # Push one image to remote
    remote_name = "origin"
    local_repo.set_remote(remote_name, remote_repo.url)
    local_repo.push(remote_name)

    # Clone the remote repo to a new local repo
    local_repo_2 = Repo(empty_local_dir)
    local_repo_2.clone(remote_repo.url)

    # There should be one image in the new local repo
    assert 1 == rcount_files_in_repo_dir(local_repo_2, "images")

    # Add the rest of the images in the original local repo
    local_repo.add("images")
