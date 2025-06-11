from ..oxen import PyLineDiff, PyChangeType

from oxen.diff.change_type import ChangeType


class LineDiff:
    """
    A class representing a change in a line of text.
    """

    def __init__(self, diff: PyLineDiff):
        self._diff = diff

    def __repr__(self) -> str:
        return (
            f"LineDiff(modification={self._diff.modification}, text={self._diff.text})"
        )

    @property
    def modification(self) -> ChangeType:
        """
        Returns the modification of the line diff.
        """
        mod_type = self._diff.modification
        if PyChangeType.Added == mod_type:
            return ChangeType.ADDED
        elif PyChangeType.Removed == mod_type:
            return ChangeType.REMOVED
        elif PyChangeType.Modified == mod_type:
            return ChangeType.MODIFIED
        elif PyChangeType.Unchanged == mod_type:
            return ChangeType.UNCHANGED
        else:
            raise ValueError(f"Invalid modification: {mod_type}")

    @property
    def text(self) -> str:
        """
        Returns the text of the line diff.
        """
        return self._diff.text
