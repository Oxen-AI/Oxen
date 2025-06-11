import os
from typing import Optional

from oxen import PyRepo


class Repo:
    """
    The Repo class that allows you to interact with your local oxen repo.

    ## Examples

    ### Init, Add, Commit and Push

    Adding and committing a file to a remote workspace.

    ```python
    import os
    from oxen import Repo

    # Initialize the Oxen Repository in a CatsAndDogs directory
    directory = "CatsAndDogs"
    repo = Repo(directory)
    repo.init()
    repo.add("images")
    repo.commit("Adding all the images")
    # Replace <namespace> and <repo_name> with your values
    repo.set_remote("origin", "https://hub.oxen.ai/<namespace>/<repo_name>")
    repo.push()
    ```
    """

    def __init__(self, path: str = "", mkdir=False):
        """
        Create a new Repo object. Use .init() to initialize a new oxen repository,
        or pass the path to an existing one.

        Args:
            path: `str`
                Path to the main working directory of your oxen repo.
            mkdir: `bool`
                Whether to create the directory if one doesn't exist. Default: False
        """
        # Check if the path exists, and convert to absolute path
        if path:
            path = os.path.abspath(path)
            if not os.path.exists(path) and mkdir:
                os.makedirs(path)

        self._repo = PyRepo(path)

    def __repr__(self):
        return f"Repo({self.path})"

    def init(self):
        """
        Initializes a new oxen repository at the path specified in the constructor.
        Will create a .oxen folder to store all the versions and metadata.
        """
        self._repo.init()
        return self

    def clone(
        self,
        url: str,
        branch: str = "main",
        all=False,
        filters: Optional[str | list[str]] = None,
    ):
        """
        Clone repository from a remote url.

        Args:
            url: `str`
                The url of the remote repository. ex) https://hub.oxen.ai/ox/chatbot
            branch: `str`
                The name of the branch to clone. Default: main
            all: `bool`
                Whether to clone the full commit history or not. Default: False
            filters: `str | list[str] | None`
                Filter down the set of directories you want to clone. Useful if
                you have a large repository and only want to make changes to a
                specific subset of files. Default: None
        """
        if isinstance(filters, str):
            filters = [filters]
        return self._repo.clone(url, branch, all, filters)

    def branches(self):
        """
        List all branches for a repo
        """
        return self._repo.list_branches()

    def branch(self, name: str, delete=False):
        """ """
        return self._repo.branch(name, delete)

    def branch_exists(self, name: str):
        """ """
        return self._repo.branch_exists(name)

    def checkout(self, revision: str, create=False):
        """
        Checkout a branch or commit id.

        Args:
            revision: `str`
                The name of the branch or commit id to checkout.
            create: `bool`
                Whether to create a new branch if it doesn't exist. Default: False
        """
        self._repo.checkout(revision, create)

    def add(self, path: str):
        """
        Stage a file or directory to be committed.
        """
        # Check if the path exists
        if not os.path.exists(path):
            # try repo.path + path
            path = os.path.join(self.path, path)

        # Convert to absolute path before adding
        path = os.path.abspath(path)
        if not os.path.exists(path):
            raise Exception(f"Path {path} does not exist.")

        self._repo.add(path)

    def add_schema_metadata(self, path: str, column_name: str, metadata: str):
        """
        Add schema to the local repository
        """
        self._repo.add_schema_metadata(path, column_name, metadata)

    def rm(self, path: str, recursive=False, staged=False):
        """
        Remove a file or directory from being tracked.
        This will not delete the file or directory.

        Args:
            path: `str`
                The path to the file or directory to remove.
            recursive: `bool`
                Whether to remove the file or directory recursively. Default: False
            staged: `bool`
                Whether to remove the file or directory from the staging area.
                Default: False
            remote: `bool`
                Whether to remove the file or directory from a remote workspace.
                Default: False
        """
        self._repo.rm(path, recursive, staged)

    def status(self):
        """
        Check the status of the repo. Returns a StagedData object.
        """
        return self._repo.status()

    def commit(self, message: str):
        """
        Commit the staged data in a repo with a message.

        Args:
            message: `str`
                The commit message.
        """
        return self._repo.commit(message)

    def log(self):
        """
        Get the commit history for a repo.
        """
        return self._repo.log()

    def set_remote(self, name: str, url: str):
        """
        Map a name to a remote url.

        Args:
            name: `str`
                The name of the remote. Ex) origin
            url: `str`
                The url you want to map the name to. Ex) https://hub.oxen.ai/ox/chatbot
        """
        self._repo.set_remote(name, url)

    def create_remote(self, name: str):
        self._repo.create_remote(name)

    def push(
        self, remote_name: str = "origin", branch: str = "main", delete: bool = False
    ):
        """
        Push data to a remote repo from a local repo.

        Args:
            remote_name: `str`
                The name of the remote to push to.
            branch: `str`
                The name of the branch to push to.
        """
        return self._repo.push(remote_name, branch, delete)

    def pull(self, remote_name: str = "origin", branch: str = "main", all=False):
        """
        Pull data from a remote repo to a local repo.

        Args:
            remote_name: `str`
                The name of the remote to pull from.
            branch: `str`
                The name of the branch to pull from.
            all: `bool`
                Whether to pull all data from branch history or not. Default: False
        """
        return self._repo.pull(remote_name, branch, all)

    @property
    def path(self):
        """
        Returns the path to the repo.
        """
        return self._repo.path()

    @property
    def current_branch(self):
        """
        Returns the current branch.
        """
        return self._repo.current_branch()

    def merge(self, branch: str):
        """
        Merge a branch into the current branch.
        """
        return self._repo.merge(branch)
