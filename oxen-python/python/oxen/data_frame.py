from oxen.workspace import Workspace
from oxen.remote_repo import RemoteRepo
from .oxen import PyWorkspaceDataFrame
import json
from typing import List, Union, Optional


class DataFrame:
    """
    The DataFrame class allows you to perform CRUD operations on a remote data frame.

    If you pass in a [Workspace](/concepts/workspaces) or a [RemoteRepo](/concepts/remote-repos) the data is indexed into DuckDB on an oxen-server without downloading the data locally.

    ## Examples

    ### CRUD Operations

    Index a data frame in a workspace.

    ```python
    from oxen import DataFrame

    # Connect to and index the data frame
    # Note: This must be an existing file committed to the repo
    #       indexing may take a while for large files
    data_frame = DataFrame("datasets/SpamOrHam", "data.tsv")

    # Add a row
    row_id = data_frame.insert_row({"category": "spam", "message": "Hello, do I have an offer for you!"})

    # Get a row by id
    row = data_frame.get_row_by_id(row_id)
    print(row)

    # Update a row
    row = data_frame.update_row(row_id, {"category": "ham"})
    print(row)

    # Delete a row
    data_frame.delete_row(row_id)

    # Get the current changes to the data frame
    status = data_frame.diff()
    print(status.added_files())

    # Commit the changes
    data_frame.commit("Updating data.csv")
    ```
    """

    def __init__(
        self,
        remote: Union[str, RemoteRepo, Workspace],
        path: str,
        host: str = "hub.oxen.ai",
        branch: str = "main",
        scheme: str = "https",
    ):
        """
        Initialize the DataFrame class. Will index the data frame
        into duckdb on init.

        Will throw an error if the data frame does not exist.

        Args:
            remote: `str`, `RemoteRepo`, or `Workspace`
                The workspace or remote repo the data frame is in.
            path: `str`
                The path of the data frame file in the repository.
            host: `str`
                The host of the oxen-server. Defaults to "hub.oxen.ai".
            branch: `str`
                The branch of the remote repo. Defaults to "main".
            scheme: `str`
                The scheme of the remote repo. Defaults to "https".
        """
        if isinstance(remote, str):
            remote_repo = RemoteRepo(remote, host, branch, scheme)
            self._workspace = Workspace(remote_repo, branch, path=path)
        elif isinstance(remote, RemoteRepo):
            self._workspace = Workspace(remote, branch, path=path)
        elif isinstance(remote, Workspace):
            self._workspace = remote
        else:
            raise ValueError("Invalid remote type. Must be a string, RemoteRepo, or Workspace")
        self._path = path

        # this will return an error if the data frame file does not exist
        self.data_frame = PyWorkspaceDataFrame(self._workspace._workspace, path)
        self.filter_keys = ["_oxen_diff_hash", "_oxen_diff_status", "_oxen_row_id"]

    def __repr__(self):
        name = f"{self._workspace._repo.namespace}/{self._workspace._repo.name}"
        return f"DataFrame(repo={name}, path={self._path})"

    def size(self) -> (int, int):
        """
        Get the size of the data frame. Returns a tuple of (rows, columns)
        """
        return self.data_frame.size()

    def page_size(self) -> int:
        """
        Get the page size of the data frame for pagination in list() command.

        Returns:
            The page size of the data frame.
        """
        return self.data_frame.page_size()

    def total_pages(self) -> int:
        """
        Get the total number of pages in the data frame for pagination in list() command.

        Returns:
            The total number of pages in the data frame.
        """
        return self.data_frame.total_pages()

    def list_page(self, page_num: int = 1) -> List[dict]:
        """
        List the rows within the data frame.

        Args:
            page_num: `int`
                The page number of the data frame to list. We default to page size of 100 for now.

        Returns:
            A list of rows from the data frame.
        """
        results = self.data_frame.list(page_num)
        # convert string to dict
        # this is not the most efficient but gets it working
        data = json.loads(results)
        data = self._filter_keys_arr(data)
        return data

    def insert_row(self, data: dict):
        """
        Insert a single row of data into the data frame.

        Args:
            data: `dict`
                A dictionary representing a single row of data.
                The keys must match a subset of the columns in the data frame.
                If a column is not present in the dictionary,
                it will be set to an empty value.

        Returns:
            The id of the row that was inserted.
        """
        # convert dict to json string
        # this is not the most efficient but gets it working
        data = json.dumps(data)
        return self.data_frame.insert_row(data)

    def get_row_by_id(self, id: str):
        """
        Get a single row of data by id.

        Args:
            id: `str`
                The id of the row to get.

        Returns:
            A dictionary representing the row.
        """
        data = self.data_frame.get_row_by_id(id)
        # convert string to dict
        # this is not the most efficient but gets it working
        data = json.loads(data)
        # filter out .oxen.diff.hash and .oxen.diff.status and _oxen_row_id
        data = self._filter_keys_arr(data)

        if len(data) == 0:
            return None
        return data[0]

    def update_row(self, id: str, data: dict):
        """
        Update a single row of data by id.

        Args:
            id: `str`
                The id of the row to update.
            data: `dict`
                A dictionary representing a single row of data.
                The keys must match a subset of the columns in the data frame.
                If a column is not present in the dictionary,
                it will be set to an empty value.

        Returns:
            The updated row as a dictionary.
        """
        data = json.dumps(data)
        result = self.data_frame.update_row(id, data)
        result = json.loads(result)
        result = self._filter_keys_arr(result)
        return result

    def delete_row(self, id: str):
        """
        Delete a single row of data by id.

        Args:
            id: `str`
                The id of the row to delete.
        """
        return self.data_frame.delete_row(id)

    def restore(self):
        """
        Unstage any changes to the schema or contents of a data frame
        """
        self.data_frame.restore()

    def commit(self, message: str, branch: Optional[str] = None):
        """
        Commit the current changes to the data frame.

        Args:
            message: `str`
                The message to commit the changes.
            branch: `str`
                The branch to commit the changes to. Defaults to the current branch.
        """
        self._workspace.commit(message, branch)

    def _filter_keys(self, data: dict):
        """
        Filter out the keys that are not needed in the dataset.
        """
        # TODO: why do we use periods vs underscores...?
        # filter out .oxen.diff.hash and .oxen.diff.status and _oxen_row_id
        # from each element in the list of dicts
        return {k: v for k, v in data.items() if k not in self.filter_keys}

    def _filter_keys_arr(self, data: List[dict]):
        """
        Filter out the keys that are not needed in the dataset.
        """
        return [self._filter_keys(d) for d in data]
