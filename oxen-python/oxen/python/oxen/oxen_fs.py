from __future__ import annotations

import logging
import os
import tempfile
from typing import Optional

import fsspec
from fsspec.utils import infer_storage_options

from .remote_repo import RemoteRepo
from .oxen import PyEntry

logger = logging.getLogger(__name__)


class OxenFS(fsspec.AbstractFileSystem):
    """
    OxenFS is a filesystem interface for Oxen repositories that implements the
    [fsspec](https://filesystem-spec.readthedocs.io/en/latest/) protocol. This
    allows you to interact with Oxen repositories using familiar filesystem
    operations and integrate with other compatible libraries like Pandas.

    ## Basic Usage

    ### Creating a Filesystem Instance

    ```python
    import oxen

    # For Oxen Hub repositories
    fs = oxen.OxenFS("ox", "Flowers")

    # For local oxen-server
    fs = oxen.OxenFS("ox", "test-repo", host="localhost:3000", scheme="http")
    ```

    ### Reading Files

    ```python
    with fs.open("data/train.csv") as f:
        content = f.read()
    ```

    ### Writing Files

    You must have write access to the repository to write files. See:
    https://docs.oxen.ai/getting-started/python#private-repositories

    OxenFS will automatically commit the file to the repository when the
    context is exited (or the file is closed some other way). New
    directories are automatically created as needed.

    ```python
    # Write with custom commit message
    with fs.open("data/test.txt", mode="wb", commit_message="Added test.txt") as f:
        f.write("Hello, world!")

    # You can also set/update the commit message inside the context
    with fs.open("data/test.txt", mode="wb") as f:
        f.commit_message = "Updated test.txt"
        f.write("Hello, world again!")
    ```

    #### Writing file objects

    If you're integrating Oxen in a situation where you already have a file object,
    you can save it to your repo by using `shutil.copyfileobj` like this:

    ```python
    import shutil

    file_object_from_somewhere = open("data.csv")

    with fs.open("train/data.csv", mode="wb") as output_file:
        output_file.commit_message = "Copy from a file object"
        shutil.copyfileobj(file_object_from_somewhere, output_file)
    ```

    ## Integration with Third Party Libraries (Pandas, etc.)

    OxenFS works seamlessly with Pandas and other fsspec-compatible libraries using
    the URL format: `oxen://namespace:repo@revision/path/to/file`

    ### Reading Data

    These will work with Pandas `{to,from}_{csv,parquet,json,etc.}` functions.

    ```python
    import pandas as pd

    # Read parquet directly from Oxen repository
    df = pd.read_parquet("oxen://openai:gsm8k@main/gsm8k_test.parquet")
    ```

    ### Writing Data

    ```python
    # Write DataFrame directly to Oxen repository
    df.to_csv("oxen://ox:my-repo@main/data/test.csv", index=False)
    ```

    ## Notes
    - Only binary read ("rb") and write ("wb") modes are currently supported
        - But writing will automatically encode strings to bytes
    - Does not yet support streaming files. All operations use temporary local files.
    """

    def __init__(
        self,
        namespace: str,
        repo: str,
        host: str = "hub.oxen.ai",
        revision: str = "main",
        scheme: str = "https",
        **kwargs,
    ):
        """
        Initialize the OxenFS instance.

        Args:
            namespace: `str`
                The namespace of the repository.
            repo: `str`
                The name of the repository.
            host: `str`
                The host to connect to. Defaults to 'hub.oxen.ai'
            revision: `str`
                The branch name or commit id to checkout. Defaults to 'main'
            scheme: `str`
                The scheme to use for the remote url. Default: 'https'
        """
        super().__init__(**kwargs)
        self.namespace = namespace
        self.repo_name = repo
        self.revision = revision
        self.scheme = scheme
        self.host = host
        self.repo = RemoteRepo(f"{namespace}/{repo}", host, revision, scheme)
        if not self.repo.exists():
            raise ValueError(f"Repo {namespace}/{repo} not found on host {host}")
        logger.debug(f"Initialized OxenFS for {namespace}/{repo}@{revision} on {host}")

    def __repr__(self):
        return f"OxenFS(namespace='{self.namespace}', repo='{self.repo_name}', revision='{self.revision}', host='{self.host}', scheme='{self.scheme}')"

    def exists(self, path: str) -> bool:
        return self.repo.metadata(path) is not None

    def isfile(self, path: str) -> bool:
        metadata = self.repo.metadata(path)
        return metadata is not None and not metadata.is_dir

    def isdir(self, path: str) -> bool:
        metadata = self.repo.metadata(path)
        return metadata is not None and metadata.is_dir

    def ls(self, path: str = "", detail: bool = False):
        """
        List the contents of a directory.

        Args:
            path: `str`
                The path to list the contents of.
            detail: `bool`
                If True, return a list of dictionaries with detailed metadata.
                Otherwise, return a list of strings with the filenames.
        """
        logger.debug(f"OxenFS.ls: '{path}'")
        metadata = self.repo.metadata(path)
        if not metadata:
            return []
        if metadata.is_dir:
            entries = self.repo.ls(path)
            return [
                self._metadata_entry_to_ls_entry(entry, detail) for entry in entries
            ]
        else:
            return [self._metadata_entry_to_ls_entry(metadata, detail)]

    @staticmethod
    def _metadata_entry_to_ls_entry(entry: PyEntry, detail: bool = False):
        if detail:
            return {
                "name": entry.path,
                "type": "directory" if entry.is_dir else "file",
                "size": entry.size,
                "hash": entry.hash,
            }
        else:
            return entry.path

    def _open(self, path: str, mode: str = "rb", **kwargs):
        """
        Open a file in the OxenFS backend.

        This is normally called through `OxenFS.open()` or `fsspec.open()`.
        """
        if mode == "rb":
            return self._open_read(path, **kwargs)
        if mode == "wb":
            return self._open_write(path, **kwargs)
        else:
            raise ValueError(
                "Unsupported file mode. Only rb and wb modes are supported"
            )

    def _open_read(self, path: str, **kwargs):
        logger.debug(f"Opening file {path} for reading")
        metadata = self.repo.metadata(path)
        if metadata.is_dir:
            raise ValueError("Cannot open directories")
        tmp_file = tempfile.NamedTemporaryFile()
        dst_path = tmp_file.file.name
        self.repo.download(path, dst_path)
        logger.debug(f"Downloaded file {path} to temp file {dst_path}")
        return open(dst_path, "rb")

    def _open_write(
        self,
        path: str,
        commit_message: Optional[str] = None,
        **kwargs,
    ):
        path = os.path.normpath(path)
        logger.debug(f"Opening file {path} for writing")
        target_dir = os.path.dirname(path)
        file_name = os.path.basename(path).strip()
        if file_name == "" or file_name == ".":
            raise ValueError("File name cannot be empty")
        try:
            metadata = self.repo.metadata(target_dir)
            if metadata and not metadata.is_dir:
                raise ValueError("target_dir cannot be an existing file")
        except ValueError as e:
            if "not found" in str(e):
                # If the directory does not exist, it will be created on the server
                pass
            else:
                raise e

        return OxenFSFileWriter(self.repo, file_name, target_dir, commit_message)

    @classmethod
    def _strip_protocol(cls, path):
        opts = infer_storage_options(path)
        if "username" not in opts:
            return super()._strip_protocol(path)
        return opts["path"].lstrip("/")

    @staticmethod
    def _get_kwargs_from_urls(path):
        opts = infer_storage_options(path)
        if "username" not in opts:
            return {}
        out = {"namespace": opts["username"], "repo": opts["password"]}
        if opts["host"]:
            out["revision"] = opts["host"]
        return out


class OxenFSFileWriter:
    """
    A file writer for the OxenFS backend.

    This is normally called through `OxenFS.open()` or `fsspec.open()`.
    """

    def __init__(
        self,
        repo: RemoteRepo,
        path: str,
        target_dir: str = "",
        commit_message: Optional[str] = None,
    ):
        self.repo = repo
        self.path = path
        self.commit_message = commit_message or "Auto-commit from OxenFS"
        self.target_dir = target_dir
        self._tmp_file = tempfile.NamedTemporaryFile()
        self.closed = False
        logger.debug(f"Initialized OxenFSFileWriter for {path} in '{target_dir}'")

    def __enter__(self) -> OxenFSFileWriter:
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            logger.error(
                f"Error writing to {self.repo} {self.path}: {exc_type} {exc_value} {traceback}"
            )

        self.close()
        # Don't suppress exceptions
        return False

    def write(self, data: str | bytes):
        """
        Write string or binary data to the file.
        """
        if isinstance(data, str):
            data = data.encode("utf-8")
        self._tmp_file.write(data)

    def flush(self):
        """
        Flush the file to disk.
        """
        self._tmp_file.flush()

    def tell(self):
        """
        Return the current position of the file.
        """
        return self._tmp_file.tell()

    def seek(self, offset: int, whence: int = os.SEEK_SET):
        """
        Seek to a specific position in the file.
        """
        self._tmp_file.seek(offset, whence)

    def commit(self, commit_message: Optional[str] = None):
        """
        Commit the file to the remote repo.
        """
        logger.debug(f"Committing file {self.path} to dir '{self.target_dir}'")
        self.repo.upload(
            self._tmp_file.name,
            commit_message=commit_message or self.commit_message,
            file_name=self.path,
            dst_dir=self.target_dir,
        )
        logger.info(f"Committed file {self.path} to dir '{self.target_dir}'")

    def close(self):
        """
        Close the file writer. This will commit the file to the remote repo.
        """
        if self.closed:
            return
        logger.debug(
            f"Closing OxenFSFileWriter for {self.path} in dir '{self.target_dir}'"
        )
        self.flush()
        self.commit()
        self._tmp_file.close()
        self.closed = True
        logger.debug(
            f"Closed OxenFSFileWriter for {self.path} in dir '{self.target_dir}'"
        )
