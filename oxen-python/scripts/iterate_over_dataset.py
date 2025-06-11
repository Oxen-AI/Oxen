
from oxen.streaming_dataset import load_dataset

# load the dataset
# dataset = load_dataset("ox/wikipedia-smol", directory="data", host="localhost:3001")
dataset = load_dataset("datasets/Wikipedia", directory="data", host="staging.hub.oxen.ai")

for i, item in enumerate(dataset):
    print(item['text'])

    if i > 1000:
        break
