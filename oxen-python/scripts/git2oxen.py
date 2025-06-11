from git import Repo as GitRepo
from oxen import Repo as OxenRepo

import time
import os
import shutil

import argparse

def main(input_dir):
    git_repo = GitRepo(input_dir)
    git = git_repo.git

    # if a .oxen directory exists in input_dir, delete it
    if os.path.exists(input_dir + "/.oxen"):
        shutil.rmtree(input_dir + "/.oxen")

    oxen_repo = OxenRepo(input_dir)
    oxen_repo.init()

    # iterate over branches in git repo
    for branch in git_repo.branches:
        print(f"Processing branch {branch.name}")
        # iterate over commits on branch
        commits = []
        for commit in branch.commit.iter_parents():
            commits.append(commit)

        commits.reverse()
        total_commits = len(commits)
        for (i, commit) in enumerate(commits):
            commit_time = time.gmtime(commit.committed_date)
            message = commit.message[0:50].replace("\n", " ")
            print(f"\tProcessing commit {i}/{total_commits} {type(commit)} {commit.hexsha} {commit_time.tm_year}-{commit_time.tm_mon}-{commit_time.tm_mday}")
            print(f"\t'{message}'")
            try:
                git.checkout(commit.hexsha)

                # list of files in commit
                for entry in commit.tree:
                    print(f"\t\tProcessing tree entry {type(entry)} {entry.path}")
                    oxen_repo.add(entry.path)

                # TODO: Make sure we have the python API docs on docs.oxen.ai
                status = oxen_repo.status()
                for file in status.removed_files():
                    print(f"\t\tProcessing removed file {file}")
                    oxen_repo.rm(file)

                oxen_repo.commit(message=message)
            except Exception as e:
                print(f"\t\tError processing commit {commit.hexsha}")
                print(e)

            print("---------END COMMIT-----------")

        print("---------END BRANCH-----------")

    git.checkout("main")


if __name__ == '__main__':
    # Parse the arguments with argparse
    parser = argparse.ArgumentParser(description='Convert a git repository to oxen')
    parser.add_argument('-i', '--input', dest="input_dir", required=True, help="Path to input directory")
    args = parser.parse_args()

    main(args.input_dir)