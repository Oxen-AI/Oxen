from pathlib import Path

from oxen import RemoteRepo, Workspace


def test_workspace_add_bytes_to_folder(
    celeba_remote_repo_one_image_pushed: RemoteRepo,
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main", "test-workspace-add-bytes")

    workspace.add_bytes("hello.txt", b"hello from memory", "a-folder")

    status = workspace.status()
    added_files = status.added_files()

    assert len(added_files) == 1
    assert added_files[0] == str(Path("a-folder") / "hello.txt")


def test_workspace_add_bytes_to_root(
    celeba_remote_repo_one_image_pushed: RemoteRepo,
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main", "test-workspace-add-bytes-root")

    # default dst -> entry lands at the workspace root under its src name
    workspace.add_bytes("greeting.txt", b"hi there")

    status = workspace.status()
    added_files = status.added_files()

    assert len(added_files) == 1
    assert added_files[0] == "greeting.txt"
