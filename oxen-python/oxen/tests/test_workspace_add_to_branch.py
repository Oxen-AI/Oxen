import os
from oxen import Workspace
from pathlib import PurePath


def test_workspace_add_to_branch(celeba_remote_repo_one_image_pushed, shared_datadir):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    remote_repo.create_branch("newbranch")
    remote_repo.checkout("newbranch")

    workspace = Workspace(remote_repo, "newbranch")

    image = str(PurePath("CelebA", "images", "1.jpg"))

    full_path = os.path.join(shared_datadir, image)
    workspace.add(full_path)
    status = workspace.status()

    assert len(status.added_files()) == 1
