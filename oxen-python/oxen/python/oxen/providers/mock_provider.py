from oxen.providers.dataset_path_provider import DatasetPathProvider
import time


class MockPathProvider(DatasetPathProvider):
    """
    A mock implementation for providing data by path and index

    It generates mock data with the given columns and number of rows
    for the set of paths.
    """

    def __init__(
        self,
        paths=["path_1.csv", "path_2.csv"],
        num_rows=1024,
        columns=["path", "x", "y"],
        download_time=0.1,  # mock a slow download
    ):
        self._paths = paths
        self._num_rows = num_rows
        self._columns = columns
        self._download_time = download_time
        self._setup()

    def _setup(self):
        self._data_frame_paths = {}
        for i, path in enumerate(self._paths):
            self._data_frame_paths[path] = self._make_data_frame(i)

    def _make_data_frame(self, i):
        df = []
        for j in range(self._num_rows):
            row = {}
            for col in self._columns:
                idx = i * self._num_rows + j
                row[col] = f"{col}_{idx}"
            df.append(row)
        return df

    @property
    def paths(self):
        return self._paths

    def size(self, path) -> int:
        """Get the size of the dataframe at the given path"""
        if path not in self._data_frame_paths:
            # Make sure the path exists
            return 0, 0

        if len(self._data_frame_paths[path]) == 0:
            # Make sure the path has data
            return 0, 0

        # width x height
        return len(self._data_frame_paths[path][0]), len(self._data_frame_paths[path])

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
        # mock a slow download
        time.sleep(self._download_time)
        return self._data_frame_paths[path][start:end]
