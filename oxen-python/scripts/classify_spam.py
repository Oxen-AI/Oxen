
from oxen import DataFrame
import openai
import tqdm


print("Creating Remote Repo")
df = DataFrame("ox/SpamOrHam", "data.tsv", host="staging.hub.oxen.ai")
# df = DataFrame("ox/SpamOrHam", "data.tsv", host="localhost:3001", scheme="http")

size = df.size()
print("size: ", size)

instruction = "Classify the data into either spam or ham depending on whether it looks like SMS spam or not. Respond in all lower case."

# Create openai client
client = openai.Client()
model = "gpt-4o"

results = df.list_page(1)
for result in tqdm.tqdm(results):
    print(result)
    text = result["text"]

    completion = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": instruction},
            {"role": "user", "content": text}
        ]
    )
    print("Assistant: " + completion.choices[0].message.content)
    response = completion.choices[0].message.content

    is_correct = response == result["category"]

    df.update_row(result["_oxen_id"], {"prediction": response, "model": model, "is_correct": is_correct})
