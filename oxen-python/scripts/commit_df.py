from oxen import RemoteRepo
from oxen import RemoteDataset
import sys

if len(sys.argv) != 2:
    print("Usage: python3 commit_df.py <commit_message>")
    sys.exit(1)

msg = sys.argv[1]

repo = RemoteRepo("ox/LLM-Dataset", "localhost:3001", scheme="http")

file_name = "prompts.parquet"

dataset = RemoteDataset(repo, file_name)
commit_id = dataset.commit(msg)
print("Committed row with id: ", commit_id)
