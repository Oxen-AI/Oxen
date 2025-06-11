from oxen.providers.dataset_path_provider import DatasetPathProvider
from oxen.providers.oxen_data_frame_provider import OxenDataFrameProvider
from oxen import RemoteRepo

from typing import List, Union, Optional
from collections import deque
from tqdm import tqdm

import threading
import time
import os


def load_dataset(
    repo: Union[RemoteRepo, str],
    paths: Optional[Union[str, List[str]]] = None,
    directory: Optional[str] = None,
    features: Optional[List[str]] = None,
    host: Optional[str] = None,
):
    """
    Load a dataset from a repo.

    Parameters
    ----------
    repo : Repo
        The oxen repository you are loading data from
        can be a local or a remote repo
    paths : str | List[str] | None
        A path or set of paths to the data files needed to load the dataset.
        all paths must be data frames.
    directory : str | None
        The directory to stream the data from.
        Must be a directory of files with type data frame.
        Can be used instead of paths.
        (default: None)
    features : List[str] | None
        The columns of the dataset (default: None)
    """
    if isinstance(paths, str):
        paths = [paths]

    if isinstance(repo, str):
        repo = RemoteRepo(repo, host=host)

    # If they supplied a directory, list all the files in the directory to get paths
    if directory is not None:
        # list all the files in the directory
        paths = repo.ls(directory)

        # prepend the directory to the paths
        paths = [os.path.join(directory, path.filename) for path in paths]

    if paths is None:
        raise ValueError("Must provide either paths or directory")

    provider = OxenDataFrameProvider(repo, paths, features)
    dataset = StreamingDataset(provider, features)
    return dataset


class StreamingDataset:
    """
    StreamingDataset object constructs a dataset from a remote repo.
    It can be used to load data into a dataloader.
    """

    def __init__(
        self,
        provider: DatasetPathProvider,
        features=None,
        num_buffers=3,
        buffer_size=128,
        sleep_interval=0.1,
    ):
        """
        Create a new RemoteRepo object to interact with.

        Parameters
        ----------
        provider : DatasetPathProvider
            The implementation of fetching data from a path and index
        features : List[str] | None
            The features of the dataset, columns, dtypes, etc.
        paths : str | List[str]
            The paths to the data files needed to load the dataset
        """
        self._provider = provider
        self._features = features

        # Get the paths from the provider
        self._paths = provider.paths

        # Compute overall size of the dataset
        print(f"Computing dataset size for {len(self._paths)} files...")
        self._path_sizes = [self._provider.size(path) for path in tqdm(self._paths)]
        # print(f"path sizes... {self._path_sizes}")
        # Culmulative sum of the path sizes
        self._culm_sizes = [
            sum([size[1] for size in self._path_sizes[: i + 1]])
            for i in range(len(self._path_sizes))
        ]
        # print(f"Culmulative: {self._culm_sizes}")

        # Update width and height based on features
        if self._features is None:
            width = self._path_sizes[0][0]
        else:
            width = len(self._features)
        height = sum([size[1] for size in self._path_sizes])
        self._size = width, height
        print(f"Dataset size {self._size}")

        # We are going to use a set of in memory buffers to pre-fetch data
        # from the API. This is to avoid having to make a request for every
        # row we want to load.
        # n_buffers is how many slices ahead we will load into memory
        self._n_buffers = num_buffers
        self._buffers = deque([])

        # print(f"Fetching {self._n_buffers} buffers...")

        # Which path file we are on
        self._path_idx = 0

        # How far into the whole dataset we have fetched
        self._fetch_idx = 0

        # How far into the current buffer we have fetched
        self._buffer_idx = 0

        # we will fetch the data in chunks of this size
        self._buffer_size = buffer_size

        # Fill the buffers with data
        # * kick off background thread to fill the buffers
        # * then wait until a buffer frees up to fetch the next one
        self._sleep_interval = sleep_interval  # seconds
        thread = threading.Thread(target=self._start_bg_collection, args=())
        thread.daemon = True
        thread.start()

    def __repr__(self):
        return f"StreamingDataset({self._provider}, {self._paths})"

    def __str__(self):
        return f"StreamingDataset({self._provider}, {self._paths})"

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    # Total abstracted size of the dataset
    @property
    def size(self):
        return self._size

    # For iterating over the dataset
    def __len__(self):
        return self._size[1]

    # For iterating over the dataset
    def __getitem__(self, idx):
        # print(f"StreamingDataset.__getitem__ {idx}")

        if idx >= self._size[1]:
            raise IndexError(
                f"Index {idx} out of range for dataset of size {self._size}"
            )

        # Make sure we have data in the first two buffers
        # we want the second one to be filled in case
        # we've exhausted the first one
        while len(self._buffers) < 1 or self._buffer_idx >= len(self._buffers[0]):
            # We will be filling this in a background thread
            time.sleep(self._sleep_interval)

            # If we have exhausted the first buffer, pop it,
            # and reset the buffer index
            if len(self._buffers) > 1 and self._buffer_idx >= len(self._buffers[0]):
                self._buffers.popleft()
                self._buffer_idx = 0

        # Offset is the row we are at in the data frame
        buffer = self._buffers[0]

        # extract the features from the data frame if there are some
        item = {}
        if self._features is None:
            item = buffer[self._buffer_idx]
        else:
            buffer = buffer[self._buffer_idx]
            item = {}
            for feature in self._features:
                val = buffer[feature]
                item[feature] = val

        # Increment the buffer index
        self._buffer_idx += 1

        return item

    def _start_bg_collection(self):
        # This is run in a background thread to fill the buffers
        # print("Start data collection...")

        # initialize the buffers
        while True:
            # print(f"Initializing buffer {len(self._buffers)}...")
            if self._path_idx >= len(self._paths):
                # We have exhausted all the paths
                print("No more paths to fetch")
                return

            # print(f"Fetching buffer {len(self._buffers)} < {self._n_buffers}")

            if len(self._buffers) < self._n_buffers:
                self._buffers.append(self._fetch_next_buffer())
            else:
                time.sleep(self._sleep_interval)

    def _fetch_next_buffer(self):
        # fetch the next buffer from the API
        path_idx = self._path_idx
        path = self._paths[path_idx]
        start = self._fetch_idx
        end = self._fetch_idx + self._buffer_size

        # If we are not on the first path, we need to offset the start and end
        if path_idx > 0:
            culm_size = self._culm_sizes[path_idx - 1]
            start = start - culm_size
            end = end - culm_size

        buffer = self._provider.slice(path, start, end)
        self._fetch_idx += len(buffer)

        # If we have exhausted the current path, move to the next one
        if self._fetch_idx >= self._culm_sizes[path_idx]:
            self._path_idx += 1

        return buffer
