from oxen.repo import Repo


def init(
    path: str = "./",
):
    """
    Initialize a [Repo](/python-api/repo) at the given path.

    Args:
        path: `str`
            The path to initialize the repo at.
     Returns:
        [Repo](/python-api/repo)
            A Repo object that can be used to interact with the repo.
    """
    # Init Repo
    repo = Repo(path)
    return repo.init()
