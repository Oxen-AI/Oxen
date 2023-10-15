from typing import Optional
from oxen.local_repo import LocalRepo


def clone(
    repo_id: str,
    path: Optional[str] = None,
    host: str = "hub.oxen.ai",
    branch: str = "main",
    protocol: str = "https",
    shallow=False,
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
        protocol: `str`
            The protocol to use. Defaults to 'https'
        shallow: `bool`
            Whether to do a shallow clone or not. Default: False
        all: `bool`
            Whether to clone the full commit history or not. Default: False
     Returns:
        [LocalRepo](/python-api/local_repo)
            A LocalRepo object that can be used to interact with the cloned repo.
    """
    # Verify repo_id format
    if not "/" in repo_id:
        raise ValueError(f"Invalid repo_id format: {repo_id}")
    # Get repo name from repo_id
    repo_name = repo_id.split("/")[1]
    # Get path from repo_name if not provided
    if path is None:
        path = repo_name
    # Get repo url
    repo_url = f"{protocol}://{host}/{repo_id}"
    # Clone repo
    local_repo = LocalRepo(path)
    local_repo.clone(repo_url, branch=branch, shallow=shallow, all=all)
    return local_repo
