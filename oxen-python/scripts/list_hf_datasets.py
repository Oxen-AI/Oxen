
from datasets import list_datasets
import json

datasets_list = list_datasets(with_details=True)
# print(len(datasets_list))

with open('datasets_info.jsonl', 'w') as f:
    for dataset in datasets_list:
        print(dataset)
        dataset_json = {
            'description': dataset.description,
            'homepage': dataset.homepage,
            'builder_name': dataset.builder_name,
            'config_name': dataset.config_name,
            'license': dataset.license,
            'citation': dataset.citation,
            'download_size': dataset.download_size,
            'size_in_bytes': dataset.size_in_bytes,
            'splits': dataset.splits,
        }
        
        json.dump(dataset_json, f)
        f.write('\n')