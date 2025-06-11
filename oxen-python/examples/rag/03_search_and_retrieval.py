import oxen
import os
import argparse
import pandas as pd
from typing import List
from oxen import Workspace, DataFrame, RemoteRepo


HTTP_HOST = "localhost:3001"
HTTP_SCHEME = "http"

def load_data(
    repo_id: str,
    questions_path: str,
    branch: str,
    data_dir: str
):
    questions_file = os.path.join(data_dir, questions_path)
    if not os.path.exists(questions_file):
        parent = os.path.dirname(questions_file)
        if not os.path.exists(parent):
            os.makedirs(parent)

        print(f"Downloading {questions_path}")
        oxen.datasets.download(
            repo_id,
            questions_path,
            dst=questions_file,
            revision=branch,
            host=HTTP_HOST,
            scheme=HTTP_SCHEME
        )
    
    df = pd.read_parquet(questions_file)
    return df
    

def fetch_results(documents_df: DataFrame, embedding: List[float]):
    results = documents_df.query(
        embedding=embedding,
        sort_by_similarity_to="chunk_embeddings"
    )
    return results

def evaluate(
    repo_id: str,
    questions_path: str,
    documents_path: str,
    branch: str,
    data_dir: str,
    output_path: str
):
    remote_repo = RemoteRepo(repo_id, host=HTTP_HOST, scheme=HTTP_SCHEME)
    workspace = Workspace(remote_repo, branch=branch)
    documents_df = DataFrame(workspace, documents_path)
    column = "chunk_embeddings"
    if not documents_df.is_nearest_neighbors_enabled(column=column):
        print("Enabling nearest neighbors for column", column)
        documents_df.enable_nearest_neighbors(column=column)

    df = load_data(repo_id, questions_path, branch, data_dir)
    print(df)

    correct = 0
    incorrect = 0
    results = []
    for idx, row in df.iterrows():
        print(f"\n\nQuestion {idx}: {row['question']}")
        answer = row["answer"]
        embedding = row["question_embeddings"]
        documents = fetch_results(documents_df, embedding)
        found_answer = False
        print(f"Looking for answer '{answer}' in {len(documents)} documents")
        documents_str = ""
        for jdx, doc in enumerate(documents):
            print(f"Document {jdx}: {doc['chunk']}")
            documents_str += f"Document Id: {doc['document_id']}\nTitle: {doc['title']}\nText: {doc['chunk']}\n\n"
            if answer.lower() in doc["chunk"].lower():
                print(f"Answer '{answer}' found in document {jdx}")
                correct += 1
                found_answer = True

        if not found_answer:
            print(f"Answer '{answer}' not found in any document")
            incorrect += 1
        print("\n\n")

        print(f"Correct: {correct}, Incorrect: {incorrect}")
        print(f"Accuracy: {correct / (correct + incorrect)}")
        results.append({
            "id": row["id"],
            "question": row["question"],
            "answer": answer,
            "search_results": documents_str
        })

    results_df = pd.DataFrame(results)
    results_df.to_parquet(output_path, index=False)

def main():
    parser = argparse.ArgumentParser(description='Evaluate a set of questions and answers.')
    parser.add_argument('-r', '--repository', help='The id of the repository ie: hallo/RAG')
    parser.add_argument('-q', '--questions', help='Path to file with the questions and answers')
    parser.add_argument('-d', '--documents', help='Path to file with the documents')
    parser.add_argument('-b', '--branch', help="The branch to fetch the file from", default="main")
    parser.add_argument('--data-dir', help='Where to store all the output data', default="data")
    parser.add_argument('-o', '--output', help='Path to file to store the output', default="output.parquet")

    args = parser.parse_args()

    evaluate(
        args.repository,
        args.questions,
        args.documents,
        args.branch,
        args.data_dir,
        args.output
    )


if __name__ == "__main__":
    main()