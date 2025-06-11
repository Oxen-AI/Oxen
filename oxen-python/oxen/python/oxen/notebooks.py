import os

from typing import Optional, Union
from .oxen import PyRemoteRepo, py_notebooks
from oxen import RemoteRepo


def start(
    repo: Union[str, RemoteRepo],
    notebook: str,
    host: str = "hub.oxen.ai",
    scheme: str = "https",
    branch: Optional[str] = None,
    base_image: Optional[str] = None,
    mode: Optional[str] = None,
    cpu_cores: Optional[int] = None,
    memory_mb: Optional[int] = None,
    timeout_secs: Optional[int] = None,
    gpu_model: Optional[str] = None,
    notebook_base_image_id: Optional[str] = None,
    build_script: Optional[str] = None,
    script_args: Optional[str] = None,
):
    """
    Start a notebook

    Args:
        repo: `str`
            The namespace/repo_name of the oxen repository to start the notebook in or a PyRemoteRepo object
        notebook: `str`
            The id/path of the notebook to start
        host: `str`
            The host to connect to. Defaults to 'hub.oxen.ai'
        scheme: `str`
            The scheme to connect to. Defaults to 'https'
        branch: `str`
            The branch to start the notebook in. Defaults to 'main'
        base_image: `str`
            The base image to start the notebook from.
        mode: `str`
            The mode to start the notebook in. Defaults to 'edit'
        cpu_cores: `int`
            The number of CPU cores to start the notebook with. Defaults to 2
        memory_mb: `int`
            The amount of memory to start the notebook with. Defaults to 2048
        timeout_secs: `int`
            The timeout to start the notebook with. Defaults to 3600
    """
    if isinstance(repo, RemoteRepo):
        py_repo = repo._repo
    else:
        py_repo = PyRemoteRepo(repo, host, branch, scheme)

    if py_repo is None:
        raise ValueError(f"Repository {py_repo.namespace}/{py_repo.name} not found")

    return py_notebooks.py_start_notebook(
        py_repo,
        notebook,
        branch,
        base_image,
        mode,
        cpu_cores,
        memory_mb,
        timeout_secs,
        gpu_model,
        notebook_base_image_id,
        build_script,
        script_args,
    )


def stop(
    repo: Union[str, RemoteRepo, None] = None,
    notebook_id: Optional[str] = None,
    host: str = "hub.oxen.ai",
    scheme: str = "https",
):
    """
    Stop a notebook
    """
    if notebook_id is None and "OXEN_NOTEBOOK_ID" in os.environ:
        notebook_id = os.environ.get("OXEN_NOTEBOOK_ID")

    if repo is None and "OXEN_REPO_NAME" in os.environ:
        repo_id = os.environ.get("OXEN_REPO_NAME")
        repo = RemoteRepo(repo_id, host, scheme)

    if isinstance(repo, RemoteRepo):
        py_repo = repo._repo
    else:
        py_repo = PyRemoteRepo(repo, host=host, scheme=scheme)

    if notebook_id is None:
        raise ValueError("No notebook id provided")

    return py_notebooks.py_stop_notebook(py_repo, notebook_id)
