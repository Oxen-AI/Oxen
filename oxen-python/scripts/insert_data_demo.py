from oxen import RemoteRepo
from oxen import RemoteDataset


print("Creating Remote Repo")
repo = RemoteRepo("ox/hi", "localhost:3001", scheme="http")

# Index the dataset
# from oxen.remote_dataset import index_dataset
# index_dataset(repo, "prompts.jsonl")

print("Creating Remote Dataset")
# Gets dataset if exists
dataset = RemoteDataset(repo, "prompts.jsonl")

size = dataset.size()
print("size: ", size)

results = dataset.list_page(1)
for result in results:
    print(result)

print("Inserting One Row")
id = dataset.insert_row({"prompt": "can I get from python?", "response": "yes you can!"})
# id = dataset.insert_row({"should_fail": "can I get from python?"})
print(id)

# id = "2ea5f604-be1a-4304-87b7-4c50f7f51a0c"
print("Get that row back")
row = dataset.get_row_by_id(id)
print(row)

# Modify the row
print("Update the row!")
result = dataset.update_row(id, {"prompt": "Pt 2: can I get from python?", "response": "yes you can!"})
print(result)

print("Get that row back again")
row = dataset.get_row_by_id(id)
print(row)

print("Deleting row")
dataset.delete_row(id)
