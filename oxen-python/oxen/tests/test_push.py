import pytest


# Alias the fixtures just to make it a little easier to read
@pytest.fixture
def local_repo(celeba_local_repo_one_image_committed):
    yield celeba_local_repo_one_image_committed


@pytest.fixture
def remote_repo(empty_remote_repo):
    yield empty_remote_repo


def test_repo_push(local_repo, remote_repo):
    remote_name = "origin"
    local_repo.set_remote(remote_name, remote_repo.url)
    local_repo.push(remote_name)
