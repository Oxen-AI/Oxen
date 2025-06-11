import os


def test_checkout(chat_bot_local_repo_no_commits):
    repo = chat_bot_local_repo_no_commits

    # oxen add prompt.txt
    prompt_file = "prompt.txt"
    full_path = os.path.join(repo.path, prompt_file)
    repo.add(full_path)

    # oxen commit
    repo.commit("Add initial prompt")

    # read prompt contents
    with open(full_path, "r") as f:
        old_contents = f.read()

    initial_branch = repo.current_branch.name

    # oxen checkout -b new_branch
    new_branch = "new_branch"
    repo.checkout(new_branch, create=True)
    assert repo.current_branch.name == "new_branch"

    # change prompt contents
    new_contents = "Summarize the following text:\n\n{}"
    with open(full_path, "w") as f:
        f.write(new_contents)

    # oxen add prompt.txt
    repo.add(full_path)

    # oxen commit
    repo.commit("Change prompt contents")

    # oxen checkout main
    repo.checkout(initial_branch)
    assert repo.current_branch.name == initial_branch
    with open(full_path, "r") as f:
        contents = f.read()
        assert contents == old_contents

    # oxen checkout new_branch
    repo.checkout(new_branch)
    assert repo.current_branch.name == new_branch
    with open(full_path, "r") as f:
        contents = f.read()
        assert contents == new_contents
