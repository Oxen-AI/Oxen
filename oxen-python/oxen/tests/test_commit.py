import os
from pathlib import PurePath


def test_commit_one_file(celeba_local_repo_no_commits):
    repo = celeba_local_repo_no_commits

    # oxen add
    image_file = str(PurePath("images", "1.jpg"))
    full_path = os.path.join(repo.path, image_file)
    repo.add(full_path)

    # oxen commit
    repo.commit("Add first image")

    # oxen log
    history = repo.log()
    assert len(history) == 1


def test_commit_all(celeba_local_repo_no_commits):
    repo = celeba_local_repo_no_commits

    # oxen add images
    repo.add(os.path.join(repo.path, "annotations"))

    # oxen commit
    repo.commit("Add all images")

    # oxen add annotations
    repo.add(os.path.join(repo.path, "annotations"))

    # oxen commit
    repo.commit("Add all annotations")

    # oxen log
    history = repo.log()
    assert len(history) == 2
