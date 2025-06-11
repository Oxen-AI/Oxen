from ..oxen import PyTextDiff, PyChangeType

from oxen.diff.line_diff import LineDiff


class TextDiff:
    """
    A class representing a text diff.
    """

    def __init__(self, diff: PyTextDiff):
        self._diff = diff

    def __repr__(self) -> str:
        return f"TextDiff(num_added={self.num_added}, num_removed={self.num_removed})"

    def __str__(self) -> str:
        # iterate over lines and print them with a + or - prefix
        return "\n".join([f"{line.value}" for line in self._diff.lines])

    @property
    def num_added(self) -> int:
        """
        Returns the number of added lines in the diff.
        """
        # count the number of added lines
        return self._count_lines(PyChangeType.Added)

    @property
    def num_removed(self) -> int:
        """
        Returns the number of removed lines in the diff.
        """
        # count the number of removed lines
        return self._count_lines(PyChangeType.Removed)

    @property
    def lines(self) -> list[LineDiff]:
        """
        Returns the contents of the diff as a polars data frame.
        """
        # map the PyLineDiff to LineDiff
        return [LineDiff(line) for line in self._diff.lines]

    def _count_lines(self, modification: PyChangeType) -> int:
        return len(
            [line for line in self._diff.lines if line.modification == modification]
        )
