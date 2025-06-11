from enum import Enum


class ChangeType(Enum):
    """
    An enum representing the type of change in a diff.
    """

    ADDED = "Added"
    REMOVED = "Removed"
    MODIFIED = "Modified"
    UNCHANGED = "Unchanged"
