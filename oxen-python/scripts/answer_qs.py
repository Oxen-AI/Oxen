from oxen import RemoteRepo
from oxen import RemoteDataset
import openai
import tqdm

# Create openai client
client = openai.Client()
model = "gpt-4o"

print("Connecting to RemoteRepo")
repo = RemoteRepo("ox/LLM-Dataset", "localhost:3001", scheme="http")
workspace_id = "03882aca-08b6-46f6-85fc-67e3369343f4"
dataset = RemoteDataset(repo, "Impossible-Questions.tsv", workspace_id=workspace_id)

results = dataset.list_page(1)
for result in tqdm.tqdm(results):
    print(result)
    prompt = result["Prompt"]

    completion = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You are a helpful assistant that has snarky responses to unanswerable questions"},
            {"role": "user", "content": prompt}
        ]
    )
    print("Assistant: " + completion.choices[0].message.content)
    response = completion.choices[0].message.content

    dataset.update_row(result["_oxen_id"], {"Response": response})
