"""
Oxen can be used to compare data frames and return a tabular diff.

There is more information about the diff in the
[Diff Getting Started Documentation](/concepts/diffs).

For example comparing two data frames will give you an output data frame,
where the `.oxen.diff.status` column shows if the row was `added`, `removed`,
or `modified`.

```
shape: (6, 7)
+-------------+-----+-----+-------+--------+-------------+-------------------+
| file        | x   | y   | width | height | label.right | .oxen.diff.status |
| ---         | --- | --- | ---   | ---    | ---         | ---               |
| str         | i64 | i64 | i64   | i64    | str         | str               |
+-------------+-----+-----+-------+--------+-------------+-------------------+
| image_0.jpg | 0   | 0   | 10    | 10     | cat         | modified          |
| image_1.jpg | 1   | 2   | 10    | 20     | null        | removed           |
| image_1.jpg | 200 | 100 | 10    | 20     | dog         | added             |
| image_2.jpg | 4   | 10  | 20    | 20     | null        | removed           |
| image_3.jpg | 4   | 10  | 20    | 20     | dog         | added             |
| image_4.jpg | 10  | 10  | 10    | 10     | dog         | added             |
+-------------+-----+-----+-------+--------+-------------+-------------------+
```

## Usage

```python
import os
import oxen

result = oxen.diff("dataset_1.csv", "dataset_2.csv")
print(result.get())
```

"""

from ..oxen import PyDiff
from ..oxen import diff as py_diff

from oxen import df_utils
from oxen.diff.tabular_diff import TabularDiff
from oxen.diff.text_diff import TextDiff

import os
from typing import Optional


def diff(
    path: os.PathLike,
    to: Optional[os.PathLike] = None,
    repo_dir: Optional[os.PathLike] = None,
    revision_left: Optional[str] = None,
    revision_right: Optional[str] = None,
    output: Optional[os.PathLike] = None,
    keys: list[str] = [],
    compares: list[str] = [],
):
    """
    Compares data from two paths and returns a diff respecting the type of data.

    Args:
        path: `os.PathLike`
            The path to diff. If `to` is not provided,
            this will compare the data frame to the previous commit.
        to: `os.PathLike`
            An optional second path to compare to.
            If provided this will be the right side of the diff.
        repo_dir: `os.PathLike`
            The path to the oxen repository. Must be provided if `compare_to` is
            not provided, or if `revision_left` or `revision_right` is provided.
            If not provided, the repository will be searched for in the current
            working directory.
        revision_left: `str`
            The left revision to compare. Can be a commit hash or branch name.
        revision_right: `str`
            The right revision to compare. Can be a commit hash or branch name.
        output: `os.PathLike`
            The path to save the diff to. If not provided, the diff will not be saved.
        keys: `list[str]`
            Only for tabular diffs. The keys to compare on.
            This is used to join the two data frames.
            Keys will be combined and hashed to create a identifier for each row.
        compares: `list[str]`
            Only for tabular diffs. The compares to compare on.
            This is used to compare the values of the two data frames.
    """
    result = py_diff.diff_paths(
        path, keys, compares, to, repo_dir, revision_left, revision_right
    )
    if output:
        df_utils.save(result, output)
    return Diff(result)


class Diff:
    """
    Diff class wraps many types of diffs and provides a consistent interface.
    For example the diff can be tabular or text. Eventually we will extend this
    to support other types of diffs such as images, audio, etc.
    """

    def __init__(self, py_diff: PyDiff):
        self._py_diff = py_diff

    def __repr__(self) -> str:
        return f"Diff(format={self.format})"

    @property
    def format(self) -> str:
        """
        Returns the format of the diff. Ie. tabular, text, etc.
        """
        return self._py_diff.format

    @property
    def tabular(self) -> Optional[TabularDiff]:
        """
        Returns the tabular diff if the diff is tabular.
        """
        if self.format == "tabular":
            return TabularDiff(self._py_diff.tabular)
        return None

    @property
    def text(self) -> Optional[TextDiff]:
        """
        Returns the text diff if the diff is text.
        """
        if self.format == "text":
            return TextDiff(self._py_diff.text)
        return None

    def get(self):
        """
        Resolves the diff type and returns the appropriate diff object.
        """
        format = self._py_diff.format
        if "tabular" == format:
            return TabularDiff(self._py_diff.tabular)
        elif "text" == format:
            return TextDiff(self._py_diff.text)
        else:
            raise ValueError("The diff type is unknown.")
