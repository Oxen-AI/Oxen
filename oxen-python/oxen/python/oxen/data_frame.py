from oxen.workspace import Workspace
from oxen.remote_repo import RemoteRepo
from .oxen import PyWorkspaceDataFrame, PyColumn
import json
from typing import List, Union, Optional
import os
import polars as pl
from oxen import df_utils


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
        branch: Optional[str] = None,
        scheme: str = "https",
        workspace_name: Optional[str] = None,
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
            branch: `Optional[str]`
                The branch of the remote repo. Defaults to None.
            scheme: `str`
                The scheme of the remote repo. Defaults to "https".
        """
        if isinstance(remote, str):
            remote_repo = RemoteRepo(remote, host=host, scheme=scheme)
            if branch is None:
                branch = remote_repo.branch().name
            self._workspace = Workspace(
                remote_repo, branch, path=path, workspace_name=workspace_name
            )
        elif isinstance(remote, RemoteRepo):
            if branch is None:
                branch = remote.branch().name
            self._workspace = Workspace(
                remote, branch, path=path, workspace_name=workspace_name
            )
        elif isinstance(remote, Workspace):
            self._workspace = remote
        else:
            raise ValueError(
                "Invalid remote type. Must be a string, RemoteRepo, or Workspace"
            )
        self._path = path
        # this will return an error if the data frame file does not exist
        try:
            self.data_frame = PyWorkspaceDataFrame(self._workspace._workspace, path)
        except Exception as e:
            print(e)
            self.data_frame = None
        self.filter_keys = ["_oxen_diff_hash", "_oxen_diff_status", "_oxen_row_id"]

    def __repr__(self):
        name = f"{self._workspace._repo.namespace}/{self._workspace._repo.name}"
        return f"DataFrame(repo={name}, path={self._path})"

    def workspace_url(self, host: str = "oxen.ai", scheme: str = "https") -> str:
        """
        Get the url of the data frame.
        """
        return f"{scheme}://{host}/{self._workspace._repo.namespace}/{self._workspace._repo.name}/workspaces/{self._workspace.id}/file/{self._path}"

    def size(self) -> tuple[int, int]:
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

    def insert_row(self, data: dict, workspace: Optional[Workspace] = None):
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

        repo = self._workspace.repo
        if not repo.file_exists(self._path):
            tmp_file_path = self._write_first_row(data)
            # Add the file to the repo
            dirname = os.path.dirname(self._path)
            repo.add(tmp_file_path, dst=dirname)
            repo.commit("Adding data frame at " + self._path)
            # This is a temporary hack that allows us to reference the resulting workspace by the
            # same name as the original workspace. Ideally, we should just be able to create a df
            # inside a workspace without a commit
            if workspace is None:
                self._workspace = Workspace(
                    repo, self._workspace.branch, path=self._path
                )
            else:
                if workspace.status().is_clean():
                    workspace.delete()
                else:
                    workspace.commit("commit data to open new workspace")
                self._workspace = Workspace(
                    repo,
                    workspace.branch,
                    path=self._path,
                    workspace_name=workspace.name,
                )
            self.data_frame = PyWorkspaceDataFrame(
                self._workspace._workspace, self._path
            )
            results = self.data_frame.list(1)
            results = json.loads(results)
            print(results)
            return results[0]["_oxen_id"]
        else:
            # convert dict to json string
            # this is not the most efficient but gets it working
            data = json.dumps(data)
            return self.data_frame.insert_row(data)

    def get_columns(self) -> List[PyColumn]:
        """
        Get the columns of the data frame.
        """
        # filter out the columns that are in the filter_keys list
        columns = [
            c for c in self.data_frame.get_columns() if c.name not in self.filter_keys
        ]
        return columns

    def add_column(self, name: str, data_type: str):
        """
        Add a column to the data frame.
        """
        return self.data_frame.add_column(name, data_type)

    def _write_first_row(self, data: dict):
        """
        Write the first row of the data frame to disk, based on the file extension and the input data.
        """
        # get the filename from the path logs/data_frame_name.csv -> data_frame_name.csv
        basename = os.path.basename(self._path)
        # write the data to a temp file that we will add to the repo
        tmp_file_path = os.path.join("/tmp", basename)
        # Create a polars data frame from the input data
        df = pl.DataFrame(data)
        # Save the data frame to disk
        df_utils.save(df, tmp_file_path)
        # Return the path to the file
        return tmp_file_path

    # TODO: Allow `where_from_str` to be passed in so user could write their own where clause
    def where_sql_from_dict(self, attributes: dict, operator: str = "AND") -> str:
        """
        Generate the SQL from the attributes.
        """
        # df is the name of the data frame
        sql = ""
        i = 0
        for key, value in attributes.items():
            # only accept string and numeric values
            if not isinstance(value, (str, int, float, bool)):
                raise ValueError(f"Invalid value type for {key}: {type(value)}")

            # if the value is a str put it in quotes
            if isinstance(value, str):
                value = f"'{value}'"
            sql += f"{key} = {value}"
            if i < len(attributes) - 1:
                sql += f" {operator} "
            i += 1
        return sql

    def select_sql_from_dict(
        self, attributes: dict, columns: Optional[List[str]] = None
    ) -> str:
        """
        Generate the SQL from the attributes.
        """
        # df is the name of the data frame
        sql = "SELECT "
        if columns is not None:
            sql += ", ".join(columns)
        else:
            sql += "*"
        sql += " FROM df WHERE "
        sql += self.where_sql_from_dict(attributes)
        return sql

    def get_embeddings(
        self, attributes: dict, column: str = "embedding"
    ) -> List[float]:
        """
        Get the embedding from the data frame.
        """
        sql = self.select_sql_from_dict(attributes, columns=[column])
        result = self.data_frame.sql_query(sql)
        result = json.loads(result)
        embeddings = [r[column] for r in result]
        return embeddings

    def is_nearest_neighbors_enabled(self, column="embedding"):
        """
        Check if the embeddings column is indexed in the data frame.
        """
        return self.data_frame.is_nearest_neighbors_enabled(column)

    def enable_nearest_neighbors(self, column: str = "embedding"):
        """
        Index the embeddings in the data frame.
        """
        self.data_frame.enable_nearest_neighbors(column)

    def query(
        self,
        sql: Optional[str] = None,
        find_embedding_where: Optional[dict] = None,
        embedding: Optional[list[float]] = None,
        sort_by_similarity_to: Optional[str] = None,
        page_num: int = 1,
        page_size: int = 10,
    ):
        """
        Sort the data frame by the embedding.
        """

        if sql is not None:
            result = self.data_frame.sql_query(sql)
        elif find_embedding_where is not None and sort_by_similarity_to is not None:
            find_embedding_where = self.where_sql_from_dict(find_embedding_where)
            result = self.data_frame.nearest_neighbors_search(
                find_embedding_where, sort_by_similarity_to, page_num, page_size
            )
        elif embedding is not None and sort_by_similarity_to is not None:
            result = self.data_frame.sort_by_embedding(
                sort_by_similarity_to, embedding, page_num, page_size
            )
        else:
            raise ValueError(
                "Must provide either sql or find_embedding_where as well as sort_by_similarity_to"
            )

        return json.loads(result)

    def nearest_neighbors_search(
        self, find_embedding_where: dict, sort_by_similarity_to: str = "embedding"
    ):
        """
        Get the nearest neighbors to the embedding.
        """
        result = self.data_frame.nearest_neighbors_search(
            find_embedding_where, sort_by_similarity_to
        )
        result = json.loads(result)
        return result

    def get_by(self, attributes: dict):
        """
        Get a single row of data by attributes.
        """
        # Write the SQL from the attributes
        sql = self.select_sql_from_dict(attributes)

        # convert dict to json string
        data = self.data_frame.sql_query(sql)
        data = json.loads(data)
        return data

    def get_row(self, idx: int):
        """
        Get a single row of data by index.

        Args:
            idx: `int`
                The index of the row to get.

        Returns:
            A dictionary representing the row.
        """
        result = self.data_frame.get_row_by_idx(idx)
        result = json.loads(result)
        return result

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
