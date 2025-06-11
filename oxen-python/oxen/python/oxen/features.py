from enum import Enum


class Feature(Enum):
    NUMERIC = 1
    TABULAR = 2
    TEXT = 3
    IMAGE = 4
    AUDIO = 5
    VIDEO = 6

    def __init__(self, name, dtype):
        """
        A feature is a column in a dataset.
        It can be numeric, tabular, text, image, audio, or video.

        Parameters
        ----------
        name: str
            The column name
        dtype: One of: Feature.NUMERIC, Feature.TABULAR, Feature.TEXT,
               Feature.IMAGE, Feature.AUDIO, Feature.VIDEO
        """
        self._name = name
        self._dtype = dtype

    @property
    def name(self) -> str:
        return self._name

    @property
    def dtype(self) -> str:
        return self._dtype


class Features:
    """
    Feature is a class that represents the features you
    want to load into a dataset. For example the input
    and output columns of a dataset.
    """

    def __init__(self, features: list[Feature]):
        """
        Create a set of features from a list of columns.

        Parameters
        ----------
        features : list[Feature]
            The columns to load from the dataset, and their respective types.
        """
        self.features = features

    def feature_names(self) -> list[str]:
        """
        Returns a list of the feature names.
        """
        return [feature.name for feature in self.features]
