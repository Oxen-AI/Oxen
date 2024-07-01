import os

from typing import Optional
from typing import List, Tuple
from .oxen import PyRemoteRepo, remote


def get_repo(name: str, host: str = "hub.oxen.ai"):
    """
    Get a RemoteRepo object for the specified name. For example 'ox/CatDogBBox'.

    Args:
        name: `str`
            Name of the repository in the format 'namespace/repo_name'.
        host: `str`
            The host to connect to. Defaults to 'hub.oxen.ai'
    Returns:
        [RemoteRepo](/python-api/remote_repo)
    """
    return remote.get_repo(name, host)


def create_repo(
    name: str,
    description="",
    is_public: bool = True,
    host: str = "hub.oxen.ai",
    scheme: str = "https",
    files: List[Tuple[str, str]] = [],
):
    """
    Create a new repository on the remote server.

    Args:
        name: `str`
            Name of the repository in the format 'namespace/repo_name'.
        description: `str`
            Description of the repository.
            Only applicable to [OxenHub](https://oxen.ai).
        is_public: `bool`
            Whether the repository is public or private.
            Only applicable to [OxenHub](https://oxen.ai).
        host: `str`
            The host to connect to. Defaults to 'hub.oxen.ai'
        scheme: `str`
            The scheme to use for the remote url. Default: 'https'
        files: `List[Tuple[str, str]]`
            A list of tuples containing the path to the file and the contents
            of the file that you would like to seed the repository with.
    Returns:
        [RemoteRepo](/python-api/remote_repo)
    """
    return remote.create_repo(name, description, is_public, host, scheme, files)


class RemoteRepo:
    """
    The RemoteRepo class allows you to interact with an Oxen repository
    without downloading the data locally.

    ## Examples

    ### Add & Commit Files

    Adding and committing a file to a remote workspace.

    ```python
    from oxen import RemoteRepo

    repo = RemoteRepo("ox/CatDogBBox")
    repo.add("/path/to/image.png")
    status = repo.status()
    print(status.added_files())
    repo.commit("Adding my image to the remote workspace.")
    ```

    ### Downloading Specific Files

    Grab a specific file revision and load it into pandas.

    ```python
    from oxen import RemoteRepo
    import pandas as pd

    # Connect to the remote repo
    repo = RemoteRepo("ox/CatDogBBox")
    # Specify the version of the file you want to download
    branch = repo.get_branch("my-pets")
    # Download takes a file or directory a commit id
    repo.download("annotations", revision=branch.commit_id)
    # Once you have the data locally, use whatever library you want to explore the data
    df = pd.read_csv("annotations/train.csv")
    print(df.head())
    ```
    """

    def __init__(
        self,
        path: str,
        host: str = "hub.oxen.ai",
        revision: str = "main",
        scheme: str = "https",
    ):
        """
        Create a new RemoteRepo object to interact with.

        Args:
            path: `str`
                Name of the repository in the format 'namespace/repo_name'.
                For example 'ox/chatbot'
            host: `str`
                The host to connect to. Defaults to 'hub.oxen.ai'
            revision: `str`
                The branch name or commit id to checkout. Defaults to 'main'
            scheme: `str`
                The scheme to use for the remote url. Default: 'https'
        """
        self._repo = PyRemoteRepo(path, host, revision, scheme)

    def __repr__(self):
        return f"RemoteRepo({self._repo.url()})"

    def create(self, empty: bool = False, is_public: bool = False):
        """
        Will create the repo on the remote server.

        Args:
            empty: `bool`
                Whether to create an empty repo or not. Default: False
            is_public: `bool`
                Whether the repository is public or private. Default: False
        """
        self._repo.create(empty, is_public)

    def exists(self) -> bool:
        """
        Checks if this remote repo exists on the server.
        """
        return self._repo.exists()

    def delete(self):
        """
        Delete this remote repo from the server.
        """
        self._repo.delete()

    def checkout(self, revision: str, create=False):
        """
        Switches the remote repo to the specified revision.

        Args:
            revision: `str`
                The name of the branch or commit id to checkout.
            create: `bool`
                Whether to create a new branch if it doesn't exist. Default: False
        """
        if create:
            return self._repo.create_branch(revision)

        self._repo.checkout(revision)

    def ls(
        self, directory: Optional[str] = None, page_num: int = 1, page_size: int = 100
    ):
        """
        Lists the contents of a directory in the remote repo.

        Args:
            directory: `str`
                The directory to list. If None, will list the root directory.
            page_num: `int`
                The page number to return. Default: 1
            page_size: `int`
                The number of items to return per page. Default: 100
        """
        if directory is None:
            return self._repo.ls("", page_num, page_size)

        return self._repo.ls(directory, page_num, page_size)

    def download(
        self, src: str, dst: Optional[str] = None, revision: Optional[str] = None
    ):
        """
        Download a file or directory from the remote repo.

        Args:
            src: `str`
                The path to the remote file
            dst: `str | None`
                The path to the local file. If None, will download to
                the same path as `src`
            revision: `str | None`
                The branch or commit id to download. Defaults to `self.revision`
        """
        if dst is None:
            dst = src
            # create parent dir if it does not exist
            directory = os.path.dirname(dst)
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)

        if revision is None:
            self._repo.download(src, dst, self.revision)
        else:
            self._repo.download(src, dst, revision)

    def log(self):
        """
        Get the commit history for a remote repo
        """
        return self._repo.log()

    def branches(self):
        """
        List all branches for a remote repo
        """
        return self._repo.list_branches()

    def get_branch(self, branch: str):
        """
        Return a branch by name on this repo, if exists

        Args:
            branch: `str`
                The name of the branch to return
        """
        return self._repo.get_branch(branch)

    def create_branch(self, branch: str):
        """
        Return a branch by name on this repo,
        creating it from the currently checked out branch if it doesn't exist

        Args:
            branch: `str`
                The name to assign to the created branch
        """
        return self._repo.create_branch(branch)

    def create_checkout_branch(self, branch: str):
        """
        Create a new branch from the currently checked out branch,
        and switch to it

        Args:
            branch: `str`
                The name to assign to the created branch
        """
        self.create_branch(branch)
        return self.checkout(branch)

    @property
    def namespace(self) -> str:
        """
        The namespace for the repo.
        """
        return self._repo.namespace()

    @property
    def name(self) -> str:
        """
        The name of the repo.
        """
        return self._repo.name()

    @property
    def identifier(self):
        """
        The namespace/name of the repo.
        """
        return f"{self.namespace}/{self.name}"

    @property
    def url(self) -> str:
        """
        The remote url for the repo.
        """
        return self._repo.url()

    @property
    def revision(self) -> str:
        """
        The branch or commit id for the repo
        """
        return self._repo.revision()
