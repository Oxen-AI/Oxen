from huggingface_hub import HfApi, DatasetSearchArguments
import json
from datasets import list_datasets

api = HfApi()

args = DatasetSearchArguments()
tags = api.get_dataset_tags()

# for tag in tags:
#     print(tag)

# for task in tags.task_categories:
#     print(task)

def process_datasets(datasets, datasets_outfile):
    total = 0
    for dataset in datasets:
        # Dataset Name: polinaeterna/OpenOrca, Tags: ['task_categories:conversational', 'task_categories:text-classification', 'task_categories:token-classification', 'task_categories:table-question-answering', 'task_categories:question-answering', 'task_categories:zero-shot-classification', 'task_categories:summarization', 'task_categories:feature-extraction', 'task_categories:text-generation', 'task_categories:text2text-generation', 'size_categories:10M<n<100M', 'language:en', 'license:mit', 'arxiv:2306.02707', 'arxiv:2301.13688', 'region:us']

        task_categories = [tag.split(':')[1] for tag in dataset.tags if tag.startswith('task_categories:')]
        arxiv_ids = [tag.split(':')[1] for tag in dataset.tags if tag.startswith('arxiv:')]
        licence = [tag.split(':')[1] for tag in dataset.tags if tag.startswith('license:')]
        languages = [tag.split(':')[1] for tag in dataset.tags if tag.startswith('language:')]
        if len(licence) > 0:
            licence = licence[0]
        else:
            licence = 'unknown'

        print(dataset)
        dataset_json = {
            "id": dataset.id,
            "downloads": dataset.downloads,
            "categories": task_categories,
            "license": licence,
            "arxiv_ids": arxiv_ids,
            "languages": languages,
        }
        print(dataset_json)

        json.dump(dataset_json, datasets_outfile)
        datasets_outfile.write('\n')
        total += 1
    print(f"Got {total} datasets for tag")
    # tag_json = {
    #     "id": tag_id,
    #     "num_datasets": total,
    # }
    # json.dump(tag_json, tags_outfile)
    # tags_outfile.write('\n')


# with open('tags.jsonl', 'w') as tags_outfile:
with open('hf_datasets.jsonl', 'w') as datasets_outfile:
    # for tag_id in tags.task_ids:
        # tag_id = tag_id.replace("_", "-")
        # tag_id = f"task_ids:{tag_id}"
        # print(tag_id)
        # datasets = api.list_datasets(filter=tag_id, sort="downloads", direction=-1, limit=100000)
    datasets = list_datasets(with_details=True)
    process_datasets(datasets, datasets_outfile)
    
        # for tag_id in tags.task_categories:
        #     tag_id = tag_id.replace("_", "-")
        #     tag_id = f"task_categories:{tag_id}"
        #     print(tag_id)
        #     datasets = api.list_datasets(filter=tag_id, sort="downloads", direction=-1, limit=100000)
        #     process_datasets(tag_id, datasets, tags_outfile, datasets_outfile)
            

# for dataset in datasets:
#     print(dataset)
