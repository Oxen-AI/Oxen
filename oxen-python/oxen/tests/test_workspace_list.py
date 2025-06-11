# test_workspace_commit.py

import os
from oxen import RemoteRepo, Workspace
from pathlib import PurePath


def test_commit_to_new_workspace(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed

    # Create a new workspace on the main branch
    workspace = Workspace(remote_repo, "main")

    # Add a file to the new workspace
    images_1 = str(PurePath("CelebA", "images", "2.jpg"))

    image_path_1 = os.path.join(shared_datadir, images_1)
    workspace.add(image_path_1)
    assert len(remote_repo.list_workspaces()) == 1

    # Commit the changes
    workspace.commit("Adding a new image to the feature branch")
    assert len(remote_repo.list_workspaces()) == 0

    # Create a second workspace and add another file
    workspace2 = Workspace(remote_repo, "main")
    images_2 = str(PurePath("CelebA", "images", "3.jpg"))
    image_path_2 = os.path.join(shared_datadir, images_2)
    workspace2.add(image_path_2)
    workspace2.commit("Adding a new image to the feature branch")
    workspaces = remote_repo.list_workspaces()
    assert len(workspaces) == 0
