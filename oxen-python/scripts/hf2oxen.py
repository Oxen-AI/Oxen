
import argparse
import os
from huggingface_hub import HfApi
from datasets import load_dataset
from oxen.remote_repo import create_repo, get_repo
from oxen import Repo
import urllib.request

def human_size(bytes, units=[' bytes','KB','MB','GB','TB', 'PB', 'EB']):
    """ Returns a human readable string representation of bytes """
    return str(bytes) + units[0] if bytes < 1024 else human_size(bytes>>10, units[1:])

def get_dataset_info(dataset_name):
    import requests
    # headers = {"Authorization": f"Bearer {API_TOKEN}"}
    headers = {}
    API_URL = f"https://datasets-server.huggingface.co/info?dataset={dataset_name}"
    def query():
        response = requests.get(API_URL, headers=headers)
        return response.json()
    data = query()
    return data

def get_repo_info(dataset_name):
    api = HfApi()

    info = api.repo_info(dataset_name, repo_type="dataset")
    print(info)
    print(info.description)

    info = get_dataset_info(dataset_name)
    print(info)

    print("\n\n")
    print("="*80)
    
    sizes = [0]
    subsets = []
    description = ""
    if 'dataset_info' in info:
        subsets = info['dataset_info'].keys()
        for key in subsets:
            info_obj = info['dataset_info'][key]
            if 'size_in_bytes' in info_obj:
                size_in_bytes = info_obj['size_in_bytes']
            else:
                size_in_bytes = info_obj['dataset_size']
            print(f"\n====\n{key}: {human_size(size_in_bytes)}")
            sizes.append(size_in_bytes)
            
            subset_description = info_obj['description'].strip()
            print(subset_description)
            if description == "":
                description = subset_description

    sum_sizes = sum(sizes)
    print(f"Dataset Total Size: {human_size(sum_sizes)}")
    print("="*80)
    print("\n\n")

    print(f"\n\nDescription:\n\n{description}\n\n")

    return {"size": sum_sizes, "description": description, "subsets": subsets}

def download_dataset_subsets(dataset_name, subsets, local_repo, data_dir, commit=None):
    if len(subsets) == 0:
        # if we failed to get subsets, just try the default subset
        subsets = ["default"]

    for subset in subsets:
        branch_name = subset

        if len(subsets) == 1:
            if commit:
                print(f"\nCalling load_dataset('{dataset_name}', revision='{commit.commit_id}')...\n")
                hf_dataset = load_dataset(dataset_name, revision=commit.commit_id)
            else:
                print(f"\nCalling load_dataset('{dataset_name}')...\n")
                hf_dataset = load_dataset(dataset_name)
            branch_name = "main"
        else:
            branch_names = [branch.name for branch in local_repo.branches()]
            print(f"Branches: {branch_names}")
            print(f"Checking out branch {branch_name}...")
            if branch_name not in branch_names:
                print(f"Creating branch {branch_name}...")
                local_repo.checkout(branch_name, create=True)
            
            if commit:
                print(f"\nCalling load_dataset('{dataset_name}', '{subset}', revision='{commit.commit_id}')...\n")
                hf_dataset = load_dataset(dataset_name, subset, revision=commit.commit_id)
            else:
                print(f"\nCalling load_dataset('{dataset_name}', '{subset}')...\n")
                hf_dataset = load_dataset(dataset_name, subset)

        for key, dataset in hf_dataset.items():
            filename = os.path.join(data_dir, f"{key}.parquet")
            dataset.to_parquet(filename)
            print(f"Adding {filename} to local repo")
            local_repo.add(filename)
            
        status = local_repo.status()
        print(status)
        if status.is_dirty():
            print(f"✅ Committing {dataset_name} to {branch_name}...")

            if commit:
                commit_message = f"{commit.title}\n\n{commit.message}"
                if commit.title == "" and commit.message == "":
                    commit_message = f"Update dataset from git commit {commit.commit_id}"
                local_repo.commit(commit_message)
            else:
                local_repo.commit("Adding dataset")

            print(f"Pushing {dataset_name} to {host}...")
            local_repo.push(branch=branch_name)

def download_and_add_readme_if_exists(dataset_name, local_repo): 
    # Download the readme
    try:
        readme_url = f"https://huggingface.co/datasets/{dataset_name}/resolve/main/README.md"
        readme_file = os.path.join(output_dir, "README.md")
        print(f"Downloading {readme_url} to {readme_file}")
        urllib.request.urlretrieve(readme_url, readme_file)
        
        local_repo.add(readme_file)
        local_repo.commit("Adding README.md")
        local_repo.push()
    except Exception as e:
        print(f"Failed to download README.md from dataset {dataset_name}")
        print(f"Got Exception: {e}")

# if main
if __name__ == "__main__":

    # argparse the name of the dataset
    parser = argparse.ArgumentParser(description='Download a dataset from hugging face and upload to Oxen.')
    # parse dataset as -d or --dataset
    parser.add_argument('-d','--dataset', dest="dataset", required=True, help="Name of the dataset to download from hugging face")
    parser.add_argument('-o','--output', dest="output", required=True, help="The output directory to save the dataset to")
    parser.add_argument('-n', '--namespace', dest="namespace", default="ox", help="The oxen namespace to upload to")
    parser.add_argument('--host', dest="host", default="hub.oxen.ai", help="The host to upload to")
    args = parser.parse_args()

    dataset_name = args.dataset
    output_dir = args.output
    namespace = args.namespace
    host = args.host

    # if dir exists, do not continue
    output_dir = os.path.join(output_dir, dataset_name)
    if os.path.exists(output_dir):
        print(f"Directory {output_dir} exists, do not continue")
        exit(1)

    clean_name = dataset_name
    if "/" in clean_name:
        clean_name = dataset_name.replace("/", "_")

    name = f"{namespace}/{clean_name}"
    # Create Remote Repo
    if get_repo(name, host=host):
        print(f"Repo {name} exists, do not continue")
        exit(1)

    # create dirs
    data_dir = os.path.join(output_dir, "data")
    os.makedirs(data_dir)
    
    # {"size": sum_sizes, "description": description, "subsets": subsets}
    info = get_repo_info(dataset_name)
    sum_sizes = info['size']
    description = info['description']
    subsets = info['subsets']

    if sum_sizes > 5_000_000_000:
        print(f"Dataset size is {human_size(sum_sizes)}, this is greater than 5GB, do not continue")
        exit(1)

    # Create Oxen Remote Repo
    remote_repo = create_repo(name, description=description, host=host)
    local_repo = Repo(output_dir)
    local_repo.init()
    local_repo.set_remote("origin", remote_repo.url())

    # Try to create README.md, some don't have it
    download_and_add_readme_if_exists(dataset_name, local_repo)

    # Try to process the commit history
    api = HfApi()
    commits = api.list_repo_commits(dataset_name, repo_type="dataset")
    commits.reverse()
    print(f"\nProcessing {len(commits)} commits\n")
    for commit in commits:
        print(f"Loading commit: {commit}...")

        # download a specific from hugging face
        try:
            download_dataset_subsets(dataset_name, subsets, local_repo, data_dir, commit=commit)

        except Exception as e:
            print(f"Failed to download commit {commit} from dataset {dataset_name}")
            print(f"Got Exception: {e}")

    if len(subsets) == 0:
        # Download the dataset with the base load_dataset function to get the latest version in case all the commit history fails, because sometimes the commit history is broken
        local_repo.checkout("main")
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        download_dataset_subsets(dataset_name, subsets, local_repo, data_dir)

    # TODO: what to do it main does not exist in the dataset? like lighteval/legal_summarization


