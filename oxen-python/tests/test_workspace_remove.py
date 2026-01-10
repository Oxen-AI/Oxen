import os
from oxen import RemoteRepo, Workspace
from pathlib import PurePath


def test_delete_staged_file(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    images_path = str(PurePath("CelebA", "images", "2.jpg"))
    full_path = os.path.join(shared_datadir, images_path)

    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main")

    workspace.add(full_path, "folder")
    status = workspace.status()
    added_files = status.added_files()
    assert len(added_files) == 1, "Error adding to test remove"

    folder_path = str(PurePath("folder", "2.jpg"))
    workspace.delete_file(folder_path)
    status = workspace.status()
    added_files = status.added_files()
    assert len(added_files) == 0, "File not successfully removed from staging"

def test_remove_file(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main")

    folder_path = str(PurePath("folder", "1.jpg"))
    workspace.delete_file(folder_path)
    status = workspace.status()
    removed_files = status.removed_files()
    assert len(removed_files) == 1, "File not successfully staged as removed"
