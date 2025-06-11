# def test_download_directory_with_slash(
#     celeba_remote_repo_fully_pushed, empty_local_dir
# ):
#     # _local_repo is the original local repo
#     # remote_repo is the remote repo we pushed to
#     _local_repo, remote_repo = celeba_remote_repo_fully_pushed

#     # download the annotations directory
#     remote_repo.download("annotations/")


def test_download_directory_without_slash(
    celeba_remote_repo_fully_pushed, empty_local_dir
):
    # _local_repo is the original local repo
    # remote_repo is the remote repo we pushed to
    _local_repo, remote_repo = celeba_remote_repo_fully_pushed

    # download the annotations directory
    remote_repo.download("annotations")
