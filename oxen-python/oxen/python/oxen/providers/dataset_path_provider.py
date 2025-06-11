class DatasetPathProvider:
    """An interface for providing data by path and index"""

    @property
    def paths(self):
        """Get the paths to the data files"""
        raise NotImplementedError

    def size(self, path) -> int:
        """Get the size of the dataframe at the given path"""
        raise NotImplementedError

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
        raise NotImplementedError
