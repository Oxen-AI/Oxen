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

    # oxen rm
    repo.rm(full_path)
    status = repo.status()
    assert repr(status) == "PyStagedData(added=0, removed=1, modified=0)"
    assert status.removed_files() == [image_file]
    assert status.added_files() == []

    # oxen restore --staged
    repo.restore(full_path, staged=True)
    status = repo.status()
    assert status.removed_files() == [], "the staged removal is discarded"
    assert status.unstaged_removed_files() == [image_file], "the file is still gone"

    # oxen restore
    repo.restore(full_path)
    status = repo.status()
    assert os.path.exists(full_path), "the working copy is back"
    assert status.unstaged_removed_files() == [], "nothing is left pending"


def test_commit_all(celeba_local_repo_no_commits):
    repo = celeba_local_repo_no_commits

    # oxen add images
    repo.add(os.path.join(repo.path, "images"))

    # oxen commit
    repo.commit("Add all images")

    # oxen add annotations
    repo.add(os.path.join(repo.path, "annotations"))

    # oxen commit
    repo.commit("Add all annotations")

    # oxen log
    history = repo.log()
    assert len(history) == 2
