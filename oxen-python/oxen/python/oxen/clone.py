from typing import Optional
from oxen.repo import Repo


def clone(
    repo_id: str,
    path: Optional[str] = None,
    host: str = "hub.oxen.ai",
    branch: str = "main",
    scheme: str = "https",
    filters: Optional[str | list[str]] = None,
    all=False,
):
    """
    Clone a repository

    Args:
        repo_id: `str`
            Name of the repository in the format 'namespace/repo_name'.
            For example 'ox/chatbot'
        path: `Optional[str]`
            The path to clone the repo to. Defaults to the name of the repository.
        host: `str`
            The host to connect to. Defaults to 'hub.oxen.ai'
        branch: `str`
            The branch name id to clone. Defaults to 'main'
        scheme: `str`
            The scheme to use. Defaults to 'https'
        all: `bool`
            Whether to clone the full commit history or not. Default: False
        filters: `str | list[str] | None`
            Filter down the set of directories you want to clone. Useful if
            you have a large repository and only want to make changes to a
            specific subset of files. Default: None
     Returns:
        [Repo](/python-api/repo)
            A Repo object that can be used to interact with the cloned repo.
    """
    # Get path from repo_name if not provided
    # Get repo name from repo_id
    repo_name = repo_id.split("/")[-1]
    if path is None:
        path = repo_name

    if repo_id.startswith("http://") or repo_id.startswith("https://"):
        # Clone repo
        repo = Repo(path)
        repo.clone(repo_id, branch=branch, all=all, filters=filters)
    else:
        # Verify repo_id format
        if "/" not in repo_id:
            raise ValueError(f"Invalid repo_id format: {repo_id}")
        # Get repo url
        repo_url = f"{scheme}://{host}/{repo_id}"
        # Clone repo
        repo = Repo(path)
        repo.clone(repo_url, branch=branch, all=all, filters=filters)
    return repo
