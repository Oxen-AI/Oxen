from oxen import RemoteRepo


def test_list_one_branch(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    assert len(remote_repo.branches()) == 1


def test_list_three_branches(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    remote_repo.create_branch("newbranch")
    remote_repo.create_branch("otherbranch")
    assert len(remote_repo.branches()) == 3
