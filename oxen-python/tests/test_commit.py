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

    # edit a tracked file, leaving the edit unstaged
    labels_file = str(PurePath("annotations", "labels.txt"))
    labels_path = os.path.join(repo.path, labels_file)
    with open(labels_path, "a") as f:
        f.write("hat\n")
    status = repo.status()
    assert status.unstaged_modified_files() == [labels_file]
    assert status.modified_files() == [], "an edit on disk stages nothing"

    # oxen add the edit
    repo.add(labels_path)
    status = repo.status()
    assert status.modified_files() == [labels_file], (
        "modified_files() is the staged set, not the working tree"
    )
    assert status.unstaged_modified_files() == []

    # delete a tracked file, leaving the removal unstaged
    image_file = str(PurePath("images", "1.jpg"))
    os.remove(os.path.join(repo.path, image_file))
    status = repo.status()
    assert status.unstaged_removed_files() == [image_file]
    assert status.removed_files() == [], "a deletion on disk stages nothing"
