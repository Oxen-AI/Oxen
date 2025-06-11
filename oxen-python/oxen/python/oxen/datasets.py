import logging
from typing import Optional

from oxen import RemoteRepo

logger = logging.getLogger(__name__)


def load_dataset(repo_id: str, path: str, fmt: str = "hugging_face", revision=None):
    """
    Load a dataset from an Oxen repository into memory using the HuggingFace datasets library.

    Args:
        repo_id: `str`
            The namespace/repo_name of the oxen repository to load the dataset from
        path: `str` | Sequence[str]
            The path to the dataset we want to load
        fmt: `str`
            The format of the data files. Currently only "hugging_face" is supported.
        revision: `str` | None
            The commit id or branch name of the version of the data to download

    Example:
    ```python
    from oxen.datasets import load_dataset
    dataset = load_dataset("datasets/gsm8k", "train.jsonl")
    # use datasets functions as you normally would
    dataset.shuffle()[:10]
    ```
    """
    logger.info(
        f"Loading dataset {repo_id} from {path} with format {fmt} and revision {revision}"
    )

    if fmt == "hugging_face":
        # Download the data from Oxen.ai
        download(repo_id, path, revision=revision)
        # Use the Hugging Face datasets library to load the data
        return _load_hf(path)
    else:
        raise ValueError(f"Unsupported load format: {fmt}")


def _load_hf(path: str):
    from datasets import load_dataset as hf_load_dataset

    if path.endswith(".csv"):
        return hf_load_dataset("csv", data_files=path)
    elif path.endswith(".json") or path.endswith(".jsonl"):
        return hf_load_dataset("json", data_files=path)
    elif path.endswith(".parquet"):
        return hf_load_dataset("parquet", data_files=path)
    else:
        raise ValueError(f"Unsupported file extension: {path}")


def download(
    repo_id: str, path: str, revision=None, dst=None, host="hub.oxen.ai", scheme="https"
):
    """
    Download files or directories from a remote Oxen repository.

    Args:
        repo_id: `str`
            The namespace/repo_name of the oxen repository to load the dataset from
        path: `str`
            The path to the data files
        revision: `str | None`
            The commit id or branch name of the version of the data to download
        dst: `str | None`
            The path to download the data to.
        host: `str`
            The host to download the data from.
        scheme: `str`
            The scheme to download the data with. (default: "https")
    """

    repo = RemoteRepo(repo_id, host=host, scheme=scheme)
    repo.download(path, revision=revision, dst=dst)


def upload(
    repo_id: str, path: str, message: str, branch: Optional[str] = None, dst: str = ""
):
    """
    Upload files or directories to a remote Oxen repository.

    Args:
        repo_id: `str`
            The namespace/repo_name of the oxen repository to upload the dataset to
        path: `str`
            The path to the data files
        message: `str`
            The commit message to use when uploading the data
        branch: `str | None`
            The branch to upload the data to. If None, the `main` branch is used.
        dst: `str | None`
            The directory to upload the data to.
    """

    repo = RemoteRepo(repo_id)
    if branch is not None:
        repo.checkout(branch)

    repo.add(path, dst=dst)
    return repo.commit(message)
