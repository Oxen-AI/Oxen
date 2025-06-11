import os
from oxen import Repo


def rcount_files_in_dir(directory: str) -> int:
    """
    Counts the number of files in a repo recursively.

    Parameters
    ----------
    directory : str
        The directory to count the number of files in.
    """
    return sum([len(files) for _, _, files in os.walk(directory)])


def rcount_files_in_dir_ignore_oxen(directory: str) -> int:
    """
    Counts the number of files in a directory recursively, ignoring the .oxen directory.

    Parameters
    ----------
    directory : str
        The directory to count the number of files in.
    """
    total = 0
    for root, _, files in os.walk(directory):
        if ".oxen" in root:
            continue
        total += len(files)
    return total


def rcount_files_in_repo(repo: Repo) -> int:
    """
    Recursively counts the number of files in a repo ignoring the .oxen directory.

    Parameters
    ----------
    repo : Repo
        The repository to count the number of files in.
    """
    return rcount_files_in_dir_ignore_oxen(repo.path)


def rcount_files_in_repo_dir(repo: Repo, directory: str) -> int:
    """
    Recursively counts the number of files in a directory repo within a repo.

    Parameters
    ----------
    repo : Repo
        The repository to count the number of files in.
    directory : str
        The directory to start the count in, relative to the repo.
    """
    return rcount_files_in_dir_ignore_oxen(os.path.join(repo.path, directory))
