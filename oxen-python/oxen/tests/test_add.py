import os

from oxen import Repo
from pathlib import PurePath


def test_add(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "CelebA")
    repo = Repo(repo_dir)
    repo.init()
    annotations_dir = "annotations"
    full_path = os.path.join(repo_dir, annotations_dir)
    repo.add(full_path)
    staged_data = repo.status()

    added_files = staged_data.added_files()
    added_files.sort()

    test_path = PurePath("annotations", "test.csv")
    train_path = PurePath("annotations", "train.csv")
    labels_path = PurePath("annotations", "labels.txt")

    assert set(added_files) == {
        str(test_path),
        str(train_path),
        str(labels_path),
    }
