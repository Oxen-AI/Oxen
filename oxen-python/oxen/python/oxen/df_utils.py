"""
The `df_utils` module provides a consistent interface for loading data frames and saving them to disk.

Supported types: csv, parquet, json, jsonl, arrow

Example usage:

```python
import os
from oxen import df_utils

# load a data frame
df = df_utils.load("path/to/data.csv")

# save a data frame
df_utils.save(df, "path/to/save.csv")
```
"""

from .oxen import df_utils

import os
from polars import DataFrame


def load(
    path: os.PathLike,
):
    """
    Reads a file into a data frame. The file format is inferred from the file extension.

    Supported types: csv, parquet, json, jsonl, arrow

    Args:
        path: `os.PathLike`
            The path to the file to read.
    """
    return df_utils.load(path)


def save(
    data_frame: DataFrame,
    path: os.PathLike,
):
    """
    Saves a data frame to a file. The file format is inferred from the file extension.

    Args:
        data_frame: `DataFrame`
            The polars data frame to save.
        path: `os.PathLike`
            The path to save the data frame to.
    """
    return df_utils.save(data_frame, path)
