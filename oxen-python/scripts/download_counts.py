import requests
import csv
import os
import argparse

# argparse the name of the dataset
parser = argparse.ArgumentParser(description='Save download stats to csv')
# parse dataset as -d or --dataset
parser.add_argument('-r','--repo_name', dest="repo_name", required=True, help="Name of the oxen repo to get stats for")
parser.add_argument('-o','--output_dir', dest="output_dir", required=True, help="Name of the output dir name")
args = parser.parse_args()

repo_owner = "Oxen-AI"
repo_name = "Oxen"
base_url = f"https://api.github.com/repos/{repo_owner}/{args.repo_name}/releases"

total_downloads = 0
downloads_per_tag = {}
release_date_per_tag = {}

page = 1
per_page = 30  # You can set another number (max 100)

while True:
    print(f"Fetching page {page}...")
    params = {"page": page, "per_page": per_page}
    response = requests.get(base_url, params=params)
    data = response.json()

    if not data:
        break  # No more data to paginate through

    for release in data:
        tag_name = release["tag_name"]
        release_date = release["published_at"]
        download_count = 0
        for asset in release["assets"]:
            download_count += asset["download_count"]
            total_downloads += asset["download_count"]
        downloads_per_tag[tag_name] = download_count
        release_date_per_tag[tag_name] = release_date

    page += 1  # Move to the next page

# mkdir if not exists args.output_dir
output_dir = os.path.join(args.output_dir, args.repo_name)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# write counts and dates to csv
filename = f"{output_dir}/download_counts.csv"
print(f"Writing download counts to {filename}...")
with open(filename, "w") as f:
    writer = csv.writer(f)
    writer.writerow(["tag_name", "download_count", "release_date"])
    for tag_name, download_count in downloads_per_tag.items():
        print(f"{tag_name}: {download_count}")
        writer.writerow([tag_name, download_count, release_date_per_tag[tag_name]])

# write total to txt file
filename = f"{output_dir}/total_downloads.txt"
print(f"Writing total downloads to {filename}...")
with open(filename, "w") as f:
    f.write(str(total_downloads))
