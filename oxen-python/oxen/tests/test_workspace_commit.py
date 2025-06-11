import os
import pytest
from oxen import RemoteRepo, Workspace
from pathlib import PurePath


def test_commit_one_file(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    # 1 commit pushed in setup
    assert len(remote_repo.log()) == 1
    images_path = str(PurePath("CelebA", "images", "1.jpg"))
    full_path = os.path.join(shared_datadir, images_path)
    workspace = Workspace(remote_repo, "main")
    workspace.add(full_path)
    workspace.commit("a commit message!")
    assert len(remote_repo.log()) == 2


def test_commit_empty(celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main")

    with pytest.raises(ValueError) as e:
        # empty commits in workspace should raise an error
        workspace.commit("a commit message")
        assert "No changes to commit" in str(e)

    # should still be 1 commit
    assert len(remote_repo.log()) == 1
