import os

from typing import Optional, TYPE_CHECKING

from .oxen import PyWorkspace, PyCommit

# Use TYPE_CHECKING for type hints to avoid runtime circular imports
if TYPE_CHECKING:
    from .remote_repo import RemoteRepo


class Workspace:
    """
    The Workspace class allows you to interact with an Oxen workspace
    without downloading the data locally.

    Workspaces can be created off a branch and is tied to the commit id of the branch
    at the time of creation.

    You can commit a Workspace back to the same branch if the branch has not
    advanced, otherwise you will have to commit to a new branch and merge.

    ## Examples

    ### Adding Files to a Workspace

    Create a workspace from a branch.

    ```python
    from oxen import RemoteRepo
    from oxen import Workspace

    # Connect to the remote repo
    repo = RemoteRepo("ox/CatDogBBox")

    # Create the workspace
    workspace = Workspace(repo, "my-branch")

    # Add a file to the workspace
    workspace.add("my-image.png")

    # Print the status of the workspace
    status = workspace.status()
    print(status.added_files())

    # Commit the workspace
    workspace.commit("Adding my image to the workspace.")
    ```
    """

    def __init__(
        self,
        repo: "RemoteRepo",
        branch: str,
        workspace_id: Optional[str] = None,
        workspace_name: Optional[str] = None,
        path: Optional[str] = None,
    ):
        """
        Create a new Workspace.

        Args:
            repo: `PyRemoteRepo`
                The remote repo to create the workspace from.
            branch: `str`
                The branch name to create the workspace from. The workspace
                will be tied to the commit id of the branch at the time of creation.
            workspace_id: `Optional[str]`
                The workspace id to create the workspace from.
                If left empty, will create a unique workspace id.
            workspace_name: `Optional[str]`
                The name of the workspace. If left empty, the workspace will have no name.
            path: `Optional[str]`
                The path to the workspace. If left empty, the workspace will be created in the root of the remote repo.
        """
        self._repo = repo
        if not self._repo.revision == branch:
            self._repo.create_checkout_branch(branch)
        try:
            self._workspace = PyWorkspace(
                repo._repo, branch, workspace_id, workspace_name, path
            )
        except ValueError as e:
            print(e)
            # Print this error in red
            print(
                f"\033[91mMake sure that you have write access to `{repo.namespace}/{repo.name}`\033[0m\n"
            )
            raise e

    def __repr__(self):
        return f"Workspace(id={self._workspace.id()}, branch={self._workspace.branch()}, commit_id={self._workspace.commit_id()})"

    @property
    def id(self):
        """
        Get the id of the workspace.
        """
        return self._workspace.id()

    @property
    def name(self):
        """
        Get the name of the workspace.
        """
        return self._workspace.name()

    @property
    def branch(self):
        """
        Get the branch that the workspace is tied to.
        """
        return self._workspace.branch()

    @property
    def commit_id(self):
        """
        Get the commit id of the workspace.
        """
        return self._workspace.commit_id()

    @property
    def repo(self) -> "RemoteRepo":
        """
        Get the remote repo that the workspace is tied to.
        """
        return self._repo

    def status(self, path: str = ""):
        """
        Get the status of the workspace.

        Args:
            path: `str`
                The path to check the status of.
        """
        return self._workspace.status(path)

    def add(self, src: str, dst: str = ""):
        """
        Add a file to the workspace

        Args:
            src: `str`
                The path to the local file to be staged
            dst: `str`
                The path in the remote repo where the file will be added
        """
        # Add a file to the workspace
        if os.path.isdir(src):
            paths = []
            for dir_path, _, files in os.walk(src):
                for file_name in files:
                    path = os.path.join(dir_path, file_name)
                    paths.append(path)
            self._workspace.add_many(paths, dst)
        else:
            # Add a single file
            self._workspace.add(src, dst)

    def rm(self, path: str):
        """
        Remove a file from the workspace

        Args:
            path: `str`
                The path to the file on workspace to be removed
        """
        self._workspace.rm(path)

    def commit(
        self,
        message: str,
        branch_name: Optional[str] = None,
    ) -> PyCommit:
        """
        Commit the workspace to a branch

        Args:
            message: `str`
                The message to commit with
            branch_name: `Optional[str]`
                The name of the branch to commit to. If left empty, will commit to the branch
                the workspace was created from.
        """
        if branch_name is None:
            branch_name = self._workspace.branch()
        return self._workspace.commit(message, branch_name)

    def delete(self):
        """
        Delete the workspace
        """
        self._workspace.delete()
