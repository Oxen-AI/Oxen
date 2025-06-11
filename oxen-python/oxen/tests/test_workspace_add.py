import os
from oxen import RemoteRepo
from oxen import Workspace
from pathlib import PurePath


def test_workspace_add_single_file(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    images_path = str(PurePath("CelebA", "images", "1.jpg"))
    full_path = os.path.join(shared_datadir, images_path)

    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main", "test-workspace")

    workspace.add(full_path, "a-folder")
    status = workspace.status()
    added_files = status.added_files()

    added_path = str(PurePath("a-folder", "1.jpg"))
    assert added_files == [added_path]


def test_workspace_add_root_dir(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    images_path = str(PurePath("CelebA", "images", "3.jpg"))
    full_path = os.path.join(shared_datadir, images_path)

    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main", "test-workspace")

    workspace.add(full_path, "")
    status = workspace.status()
    added_files = status.added_files()

    assert added_files == ["3.jpg"]
