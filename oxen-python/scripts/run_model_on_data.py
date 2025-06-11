from oxen import RemoteRepo
from oxen import DataFrame
import openai
import tqdm


print("Creating Remote Repo")
repo = RemoteRepo("ox/LLM-Dataset", "localhost:3001", scheme="http")

print("Creating Remote Dataset")
# Gets dataset if exists
dataset = DataFrame(repo, "prompts.parquet")

size = dataset.size()
print("size: ", size)

# Create openai client
client = openai.Client()
model = "gpt-4o"

results = dataset.list_page(1)
for result in tqdm.tqdm(results):
    print(result)
    prompt = result["instruction"]
    context = result["context"]

    prompt = f"Context: {context}\n\nInstruction: {prompt}"

    completion = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    response = completion.choices[0].message.content
    print("Assistant: " + response)

