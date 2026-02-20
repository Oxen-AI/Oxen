from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Iterator, Optional

from .oxen import PyCommit, PyWorkspace

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
    ) -> None:
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
            raise

    def __repr__(self) -> str:
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
    def commit_id(self) -> str:
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

    def add(
        self, src: str | Iterable[str] | Path | Iterable[Path], dst: str = ""
    ) -> None:
        """
        Add files to the workspace.

        Accepts a single file, a single directory, or multiple of either.

        Recursively walks directories to add all accessible files. Preserves
        relative path to destination when adding multiple files from a directory.

        Args:
            src: `str` | Iterable[str] | Path | Iterable[Path]
                The path(s) to the local file(s) to be staged.
            dst: `str`
                The path in the remote repo where the file(s) will be added.

        Raises:
            ValueError if the provided input is not a valid filepath, an invalid
            directory, or it points to an empty directory, or is a collection
            of empty directories.
        """

        if isinstance(src, str) or isinstance(src, Path):
            p_src = Path(src)
            if not p_src.exists():
                raise ValueError(f"Provided single path that does not exist: {src}")
            paths: list[str] = list(map(str, _filepaths_from(p_src)))
        else:

            def stringify_ok(fpath: str | Path) -> str:
                if not Path(fpath).exists():
                    raise ValueError(
                        f"Provided an input path that does not exist: {fpath}"
                    )
                return str(fpath)

            paths = [
                stringify_ok(fpath)
                for path in src
                for fpath in _filepaths_from(Path(path))
            ]

        if len(paths) == 0:
            if isinstance(src, str) or isinstance(src, Path):
                raise ValueError(f"{src} is not a valid filepath nor directory.")
            else:
                raise ValueError(
                    "No valid filepaths provided: adding nothing to a workspace is invalid."
                )
        self._workspace.add(paths, dst)

    def add_files(
        self,
        base_dir: str | Path,
        paths: Iterable[str] | Iterable[Path],
    ) -> None:
        """
        A workspace add that preserves relative paths of files that share a common base.

        Unlike `add`, which places files into a flat destination directory,
        this method uses each file's path relative to the supplied base directory as
        its staging path on the server.

        The `base_dir` serves as a stand-in for the root of the remote repository. The key
        use of `add_files` is to import a large file tree into an existing repository.

        For example, a file at
        ``repo/data/images/cat.jpg`` will be staged as ``data/images/cat.jpg``.

        Args:
            base_dir: `str` | `Path`
                The base directory: all added files share this as an ancestor.
            paths: `Iterable[str]` | `Iterable[Path]`
                The file paths to add. Can be absolute or relative to the
                base directory. Each path must point to an existing file.

        Raises:
            ValueError: If no valid file paths are provided.
        """
        base_dir = Path(base_dir).absolute()
        resolved: list[str] = []
        for p in paths:
            p = Path(p)
            abs_path = p if p.is_absolute() else (base_dir / p).absolute()
            resolved.append(str(abs_path))
        self._workspace.add_files(str(base_dir), resolved)

    def add_bytes(self, src: str, buf: bytes, dst: str = "") -> None:
        """
        Adds from a memory buffer to the workspace

        Args:
            src: `str`
                The relative path to be used as the entry's name in the workspace
            buf: `bytes`
                The memory buffer to be read from for this entry
            dst: `str`
                The path in the remote repo where the file will be added
        """

        self._workspace.add_bytes(src, buf, dst)

    def rm(self, path: str) -> None:
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

    def delete(self) -> None:
        """
        Delete the workspace
        """
        self._workspace.delete()


def _filepaths_from(path: Path) -> Iterator[Path]:
    """Yields all files from the given path.

    Yield the input if its a valid filepath.
    If its a directory, yields all files from the directory and and sub-directories.
    """
    if path.is_file():
        yield path
    elif path.is_dir():
        for something_under in path.rglob("*"):
            if something_under.is_file():
                yield something_under


# def _assert_file_in_base(base_dir: Path, p: Path):
#     """ValueError if `p` doesn't have `base_dir` as an ancesor or isn't a file.

#     Assumes that `base_dir` (1) is a directory and (2) is a resolved path.
#     """
#     if not p.is_absolute():
#         p = base_dir / p
#     else:
#         p = p.resolve()
#         if not p.is_relative_to(base_dir):
#             raise ValueError(f"Absolute path is not under base_dir ({base_dir}): {p}")
#     if not p.is_file():
#         raise ValueError(f"Path is not a file: {p}")
