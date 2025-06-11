import os
from pathlib import PurePath


def test_branch_exists(celeba_local_repo_no_commits):
    repo = celeba_local_repo_no_commits

    # oxen add
    image_file = str(PurePath("images", "1.jpg"))
    full_path = os.path.join(repo.path, image_file)
    repo.add(full_path)

    # oxen commit
    repo.commit("Add first image")

    # oxen branch_exists
    assert repo.branch_exists("main")
    assert not repo.branch_exists("false")
