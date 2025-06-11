# 🐂 🐍 Oxen Python Interface 

The Oxen python interface makes it easy to integrate Oxen datasets directly into machine learning dataloaders or other data pipelines.

## Repositories

There are two types of repositories one can interact with, a `Repo` and a `RemoteRepo`.


## Local Repo

To fully clone all the data to your local machine, you can use the `Repo` class.

```python
import oxen

repo = oxen.Repo("path/to/repository")
repo.clone("https://hub.oxen.ai/ox/CatDogBBox")
```

If there is a specific version of your data you want to access, you can specify the `branch` when cloning.

```python
repo.clone("https://hub.oxen.ai/ox/CatDogBBox", branch="my-pets")
```

Once you have a repository locally, you can perform the same operations you might via the command line, through the python api.

For example, you can checkout a branch, add a file, commit, and push the data to the same remote you cloned it from.

```python
import oxen

repo = oxen.Repo("path/to/repository")
repo.clone("https://hub.oxen.ai/ox/CatDogBBox")
repo.checkout()
```

## Remote Repo

If you don't want to download the data locally, you can use the `RemoteRepo` class to interact with a remote repository on OxenHub.

```python
import oxen 

repo = RemoteRepo("https://hub.oxen.ai/ox/CatDogBBox")
```

To stage and commit files to a specific version of the data, you can `checkout` an existing branch or create a new one.

```python
repo.create_branch("dev")
repo.checkout("dev")
```

You can then stage files to the remote repository by specifying the file path and destination directory.

```python
repo.add("new-cat.png", "images") # Stage to images/new-cat.png on remote
repo.commit("Adding another training image")
```

Note that no "push" command is required here, since the above code creates a commit directly on the remote branch.

