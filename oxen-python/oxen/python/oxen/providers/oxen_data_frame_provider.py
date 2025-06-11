from oxen.providers.dataset_path_provider import DatasetPathProvider
from oxen import RemoteRepo
from typing import List
import json


class OxenDataFrameProvider(DatasetPathProvider):
    """
    An implementation for providing data by path and index

    It grabs rows of data from the oxen server.
    """

    def __init__(
        self, repo: RemoteRepo, paths: List[str], columns: List[str] | None = None
    ):
        """
        Initialize

        Parameters
        ----------
        repo : RemoteRepo
            The oxen repository you are loading data from
        paths : List[str]
            The paths to the data files needed to load the dataset
        columns : List[str] | None
            The columns of the dataset (default: None)
        """

        if len(paths) == 0:
            raise ValueError("Paths must not be empty")

        self._repo = repo
        self._paths = paths
        self._columns = columns

    @property
    def paths(self):
        return self._paths

    def size(self, path) -> int:
        """Get the size of the dataframe at the given path"""
        # width x height
        return self._repo.get_df_size(path)

    def slice(self, path, start, end):
        """
        Get a slice of the dataframe at the given path

        Parameters
        ----------
        path : str
            The path to the dataframe
        start : int
            The start index
        end : int
            The end index
        """
        data = self._repo.get_df_slice(path, start, end)
        json_data = json.loads(data)
        return json_data
