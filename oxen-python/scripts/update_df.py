from oxen import DataFrame
import time

# Loop over 10 questions
questions = [
    {"input": "What is the capital of France?", "instruction": "Answer the question plz.", "output": "Paris"},
    {"input": "What is the capital of Germany?", "instruction": "Answer the question plz.", "output": "Berlin"},
    {"input": "What is the capital of Italy?", "instruction": "Answer the question plz.", "output": "Rome"},
    {"input": "What is the capital of Spain?", "instruction": "Answer the question plz.", "output": "Madrid"},
    {"input": "What is the capital of Portugal?", "instruction": "Answer the question plz.", "output": "Lisboa"},
    {"input": "What is the capital of Switzerland?", "instruction": "Answer the question plz.", "output": "Bern"},
    {"input": "What is the capital of Austria?", "instruction": "Answer the question plz.", "output": "Vienna"},
    {"input": "What is the capital of Poland?", "instruction": "Answer the question plz.", "output": "Warsaw"},
    {"input": "What is the capital of Turkey?", "instruction": "Answer the question plz.", "output": "Ankara"},
    {"input": "What is the capital of Belgium?", "instruction": "Answer the question plz.", "output": "Brussels"},
    {"input": "What is the capital of Netherlands?", "instruction": "Answer the question plz.", "output": "Amsterdam"},
    {"input": "What is the capital of Luxembourg?", "instruction": "Answer the question plz.", "output": "Luxembourg"},
    {"input": "What is the capital of Ireland?", "instruction": "Answer the question plz.", "output": "Dublin"},
]

init_times = []
insert_times = []
commit_times = []

for i, question in enumerate(questions):
    print("==== Question", i, "====")
    start = time.time()
    df = DataFrame("ox/prompts", "fine-tune.jsonl", host="localhost:3001", scheme="http")
    print(f"Connected to DataFrame with workspace {df._workspace.id}")
    init_time = time.time() - start
    print("Init time: ", init_time)
    init_times.append(init_time)
    
    start = time.time()
    id = df.insert_row(question)
    print("Inserted row with id: ", id)
    insert_time = time.time() - start
    print("Insert time: ", insert_time)
    insert_times.append(insert_time)

    start = time.time()
    commit_id = df.commit("Added question: " + question["input"])
    print("Committed row with id: ", commit_id)
    commit_time = time.time() - start
    print("Commit time: ", commit_time)
    commit_times.append(commit_time)

# print average times
print("Average init time: ", sum(init_times) / len(init_times))
print("Average insert time: ", sum(insert_times) / len(insert_times))
print("Average commit time: ", sum(commit_times) / len(commit_times))
