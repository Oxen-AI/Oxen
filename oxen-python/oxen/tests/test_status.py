import os

from oxen import Repo


def test_status_empty(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "CelebA")
    repo = Repo(repo_dir)
    repo.init()
    staged_data = repo.status()
    assert len(staged_data.added_files()) == 0
