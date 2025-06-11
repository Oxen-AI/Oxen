from oxen import Repo
from oxen.fs import rcount_files_in_repo


def test_repo_clone(celeba_remote_repo_fully_pushed, empty_local_dir):
    # og_local_repo is the original local repo
    # then we pushed to the remote repo
    og_local_repo, remote_repo = celeba_remote_repo_fully_pushed

    # now we clone the remote repo
    # and verify that the local repo is the same as the original
    local_repo = Repo(empty_local_dir)
    local_repo.clone(remote_repo.url)

    print("og_local_repo")
    print(og_local_repo)

    print("local_repo")
    print(local_repo)

    og_count = rcount_files_in_repo(og_local_repo)
    new_count = rcount_files_in_repo(local_repo)

    assert og_count == new_count
