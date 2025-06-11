import os
from oxen import RemoteRepo, Workspace
from pathlib import PurePath


def test_commit_to_named_workspace(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed

    # Create a new workspace on the main branch
    workspace = Workspace(remote_repo, branch="main", workspace_name="my-workspace")
    # Add a file to the new workspace

    celeb_path = str(PurePath("CelebA", "images", "2.jpg"))
    image_path = os.path.join(shared_datadir, celeb_path)
    workspace.add(image_path)
    assert len(remote_repo.list_workspaces()) == 1
    # Commit the changes
    workspace.commit("Adding a new image to the feature branch")
    assert len(remote_repo.list_workspaces()) == 1
    workspace = Workspace(remote_repo, branch="main", workspace_name="my-workspace")
    assert workspace.name == "my-workspace"
    assert len(remote_repo.list_workspaces()) == 1
    workspace.delete()
    assert len(remote_repo.list_workspaces()) == 0


def test_named_workspace_naming_collision(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, branch="main", workspace_name="my-workspace")
    workspace = Workspace(remote_repo, branch="main", workspace_name=workspace.id)
    workspace = Workspace(remote_repo, branch="main", workspace_id=workspace.id)
    workspace = Workspace(remote_repo, branch="main", workspace_name=workspace.id)
    assert workspace.name == "my-workspace"
    assert len(remote_repo.list_workspaces()) == 1


def test_named_workspace_iterating_commits(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, branch="main", workspace_name="my-workspace")

    # Create the file inside the shared data directory.
    stock_file_path = os.path.join(shared_datadir, "DownloadTest", "stock.exs")
    os.makedirs(os.path.dirname(stock_file_path), exist_ok=True)
    with open(stock_file_path, "w") as file:
        file.write("")

    # Simulate changes and commits multiple times.
    for i in range(3):
        with open(stock_file_path, "a") as file:
            file.write("something\n")
        workspace.add(stock_file_path)
        workspace.commit("message")

    # Ensure that only one workspace exists.
    assert len(remote_repo.list_workspaces()) == 1
