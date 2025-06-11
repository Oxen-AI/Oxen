import os

from typing import Optional
from typing import List, Tuple
from .oxen import PyRemoteRepo, remote, PyCommit
from . import user as oxen_user
from .workspace import Workspace


def get_repo(name: str, host: str = "hub.oxen.ai", scheme: str = "https"):
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
    py_repo = remote.get_repo(name, host, scheme)

    if py_repo is None:
        raise ValueError(f"Repository {name} not found")

    repo_id = f"{py_repo.namespace()}/{py_repo.name()}"
    return RemoteRepo(repo_id, py_repo.host, py_repo.revision, py_repo.scheme)


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
    py_repo = remote.create_repo(name, description, is_public, host, scheme, files)
    repo_id = f"{py_repo.namespace()}/{py_repo.name()}"
    return RemoteRepo(repo_id, py_repo.host, "main", py_repo.scheme)


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
        repo_id: str,
        host: str = "hub.oxen.ai",
        revision: str = "main",
        scheme: str = "https",
    ):
        """
        Create a new RemoteRepo object to interact with.

        Args:
            repo_id: `str`
                Name of the repository in the format 'namespace/repo_name'.
                For example 'ox/chatbot'
            host: `str`
                The host to connect to. Defaults to 'hub.oxen.ai'
            revision: `str`
                The branch name or commit id to checkout. Defaults to 'main'
            scheme: `str`
                The scheme to use for the remote url. Default: 'https'
        """
        self._repo = PyRemoteRepo(repo_id, host, revision, scheme)
        # An internal workspace gets created on the first add() call
        self._workspace = None

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
            self._repo.create_branch(revision)

        return self._repo.checkout(revision)

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

    def scan(self, directory: Optional[str] = None, page_size: int = 100):
        """
        Generator over the contents of a directory in the remote repo

        Args:
            directory: `str`
                The directory to list. If None, will list the root directory
            page_size: `int`
                The number of items to return per page. Default: 100
        """
        if directory is None:
            directory = ""

        current_page = 1

        while True:
            contents = self._repo.ls(
                directory, page_num=current_page, page_size=page_size
            )

            if not contents.entries:
                return

            yield from contents.entries

            if current_page >= contents.total_pages:
                return

            current_page += 1

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

    def get_file(self, src: str, revision: Optional[str] = None):
        """
        Get a file from the remote repo.

        Args:
            src: `str`
                The path to the remote file
            revision: `str | None`
                The branch or commit id to download. Defaults to `self.revision`
        """
        if revision is None:
            return self._repo.get_file(src, self.revision)
        else:
            return self._repo.get_file(src, revision)

    def create_workspace(
        self, branch: Optional[str] = None, workspace_name: Optional[str] = None
    ):
        """
        Create a new workspace in the remote repo. If the workspace already exists, it will just be returned.

        Args:
            branch: `str | None`
                The branch to create the workspace on. Defaults to `self.revision`
            workspace_name: `str | None`
                The named workspace to use when adding the file. If None, will create a temporary workspace

        Returns:
            [Workspace](/python-api/workspace)
        """
        if branch is None or branch == "":
            branch = self.revision

        if self._workspace is None:
            self._workspace = Workspace(self, branch, workspace_name=workspace_name)
            print(
                f"Workspace '{self._workspace.id}' created from commit '{self._workspace.commit_id}'"
            )
            self._repo.set_commit_id(self._workspace.commit_id)
            return self._workspace
        elif (
            self._workspace.branch == branch and self._workspace.name == workspace_name
        ):
            # workspace already exists
            return self._workspace
        else:
            raise ValueError(
                "A different workspace is already open for this repo, commit or delete it first"
            )

    def delete_workspace(self):
        """
        Delete the current workspace in the remote repo.
        """
        if self._workspace is not None:
            self._workspace.delete()
            self._workspace = None

    def add(
        self,
        src: str,
        dst: Optional[str] = "",
        branch: Optional[str] = None,
        workspace_name: Optional[str] = None,
    ):
        """
        Stage a file to a workspace in the remote repo.

        Args:
            src: `str`
                The path to the local file to upload
            dst: `str | None`
                The directory to upload the file to. If None, will upload to the root directory.
            branch: `str | None`
                The branch to upload the file to. Defaults to `self.revision`
            workspace_name: `str | None`
                The named workspace to use when adding the file. If None, will create a temporary workspace

        Returns:
            [Workspace](/python-api/workspace)
        """
        # If the workspace already exists, this is a no-op
        self.create_workspace(branch, workspace_name)
        self._workspace.add(src, dst)
        return self._workspace

    def status(self):
        """
        Get the status of the workspace.
        """
        if self._workspace is None:
            raise ValueError("No workspace found. Please call add() first.")

        return self._workspace.status()

    def commit(self, message: str, branch: Optional[str] = None):
        """
        Commit the workspace to the remote repo.

        Args:
            message: `str`
                The message to commit with
            branch: `str | None`
                The branch to commit to. Defaults to the branch the workspace was created on.
        """
        if self._workspace is None:
            raise ValueError("No workspace found. Please call add() first.")

        commit = self._workspace.commit(message, branch)
        self._repo.set_commit_id(commit.id)

        # If it's not a named workspace, it's deleted after commit
        if self._workspace.name is None:
            self._workspace = None
        return commit

    def upload(
        self,
        src: str,
        commit_message: str,
        file_name: Optional[str] = None,
        dst_dir: Optional[str] = "",
        branch: Optional[str] = None,
    ):
        """
        Upload a file to the remote repo.

        Args:
            src: `str`
                The path to the local file to upload
            file_name: `str | None`
                The name of the file to upload. If None, will use the name of the file in `src`
            dst_dir: `str | None`
                The directory to upload the file to. If None, will upload to the root directory.
            branch: `str | None`
                The branch to upload the file to. Defaults to `self.revision`
        """
        if branch is None:
            branch = self.revision
        if file_name is None:
            file_name = os.path.basename(src)
        user = oxen_user.current_user()

        self._repo.put_file(branch, dst_dir, src, file_name, commit_message, user)

    def metadata(self, path: str):
        """
        Get the metadata for a file in the remote repo.
        """
        return self._repo.metadata(path)

    def file_exists(self, path: str, revision: Optional[str] = None):
        """
        Check if a file exists in the remote repo.

        Args:
            path: `str`
                The path to the file to check
            revision: `str`
                The revision to check against, defaults to `self.revision`
        """

        if revision is None:
            revision = self.revision

        return self._repo.file_exists(path, revision)

    def file_has_changes(
        self, local_path: str, remote_path: str = None, revision: str = None
    ):
        """
        Check if a local file has changed compared to a remote revision

        Args:
            local_path: `str`
                The local path to the file to check
            remote_path: `str`
                The remote path to the file to check, will default to `local_path` if not provided
            revision: `str`
                The revision to check against, defaults to `self.revision`
        """

        if remote_path is None:
            remote_path = local_path

        if revision is None:
            revision = self.revision

        # If the file doesn't exist on the remote repo, it's a new file, hence has changes
        if not self.file_exists(remote_path, revision):
            return True

        return self._repo.file_has_changes(local_path, remote_path, revision)

    def log(
        self,
        revision: Optional[str] = None,
        path: Optional[str] = None,
        page_num: int = 1,
        page_size: int = 10,
    ):
        """
        Get the commit history for a remote repo

        Args:
            revision: `str | None`
                The revision to get the commit history for. Defaults to `self.revision`
            path: `str | None`
                The path to the file to get the commit history for. Defaults to
                None, which will return the commit history for the entire repo
            page_num: `int`
                The page number to return. Defaults to 1
            page_size: `int`
                The number of items to return per page. Defaults to 10
        """
        if revision is None:
            revision = self.revision

        return self._repo.log(revision, path, page_num, page_size)

    def branch_exists(self, name: str) -> bool:
        """
        Check if a branch exists in the remote repo.

        Args:
            name: `str`
                The name of the branch to check
        """
        return self._repo.branch_exists(name)

    def branch(self):
        """
        Get the current branch for a remote repo
        """
        return self.get_branch(self.revision)

    def branches(self):
        """
        List all branches for a remote repo
        """
        return self._repo.list_branches()

    def list_workspaces(self):
        """
        List all workspaces for a remote repo
        """
        return self._repo.list_workspaces()

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
        print(f"Creating branch '{branch}' from commit '{self._repo.commit_id}'")
        return self._repo.create_branch(branch)

    def delete_branch(self, branch: str):
        """
        Delete a branch from the remote repo.

        Args:
            branch: `str`
                The name of the branch to delete
        """
        return self._repo.delete_branch(branch)

    def create_checkout_branch(self, branch: str):
        """
        Create a new branch from the currently checked out branch,
        and switch to it

        Args:
            branch: `str`
                The name to assign to the created branch
        """
        if not self.branch_exists(branch):
            self.create_branch(branch)
        return self.checkout(branch)

    def merge(self, base_branch: str, head_branch: str):
        """
        Merge the head branch into the base branch on the remote repo.

        Args:
            base_branch: `str`
                The base branch to merge into
            head_branch: `str`
                The head branch to merge
        """
        commit = self._repo.merge(base_branch, head_branch)
        return commit

    def mergeable(self, base_branch: str, head_branch: str):
        """
        Check if a branch is mergeable into another branch.

        Args:
            base_branch: str
                The target branch to merge into
            head_branch: str
                The source branch to merge from
        """
        return self._repo.mergeable(base_branch, head_branch)

    def diff(
        self,
        base: str | PyCommit,
        head: str | PyCommit,
        path: str,
    ):
        """
        Get the diff between two refs on the remote repo.

        Args:
            base: `str`
                The base ref to diff (branch or commit)
            head: `str`
                The head ref to diff (branch or commit)
            path: `str`
                The path to the file to diff
        """
        diff = self._repo.diff_file(str(base), str(head), path)
        if diff.format == "text":
            return diff.text
        else:
            raise NotImplementedError(
                "Only text diffs are supported in RemoteRepo right now"
            )

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
        return self._repo.revision
