
from oxen import RemoteRepo
from oxen import Workspace
from oxen import DataFrame

print("Creating Remote Repo")
repo = RemoteRepo("ox/LLM-Dataset", "localhost:3001", scheme="http")

print("Creating Workspace")
workspace = Workspace(repo, "main")

print("Creating DataFrame")
df = DataFrame(workspace, "openhermes_train.parquet")

print("Indexing DataFrame")
df.index()
