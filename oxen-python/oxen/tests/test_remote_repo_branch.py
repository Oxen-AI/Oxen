from oxen import RemoteRepo


def test_create_new_branch(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    remote_repo.create_branch("hrllo")
    remote_repo.checkout("hrllo")

    assert remote_repo.branch_exists("hrllo")
    assert len(remote_repo.branches()) == 2


def test_delete_branch(celeba_remote_repo_one_image_pushed: RemoteRepo):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    remote_repo.create_checkout_branch("shiny-testing-branch")
    assert remote_repo.branch_exists("shiny-testing-branch")
    assert len(remote_repo.branches()) == 2

    remote_repo.delete_branch("shiny-testing-branch")
    assert not remote_repo.branch_exists("shiny-testing-branch")
    assert len(remote_repo.branches()) == 1
