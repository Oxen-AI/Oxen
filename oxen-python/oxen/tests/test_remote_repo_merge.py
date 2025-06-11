import os
import pytest
from oxen import RemoteRepo


def test_merge_branches(chat_bot_remote_repo_fully_pushed: RemoteRepo, shared_datadir):
    local_repo, remote_repo = chat_bot_remote_repo_fully_pushed
    local_repo.checkout("update_prompt", create=True)
    with open(os.path.join(local_repo.path, "prompt.txt"), "w") as f:
        f.write("This is an updated prompt!")
    local_repo.add("prompt.txt")
    local_repo.commit("Updated prompt")
    local_repo.push("origin", "update_prompt")

    remote_repo.merge("main", "update_prompt")

    local_repo.checkout("main")
    local_repo.pull()

    with open(os.path.join(local_repo.path, "prompt.txt"), "r") as f:
        assert f.read() == "This is an updated prompt!"


def test_merge_branches_with_conflicts(
    chat_bot_remote_repo_fully_pushed: RemoteRepo, shared_datadir
):
    local_repo, remote_repo = chat_bot_remote_repo_fully_pushed

    local_repo.checkout("update_prompt", create=True)
    with open(os.path.join(local_repo.path, "prompt.txt"), "w") as f:
        f.write("This is an updated prompt!")
    local_repo.add("prompt.txt")
    local_repo.commit("Updated prompt")
    local_repo.push("origin", "update_prompt")

    local_repo.checkout("main")
    with open(os.path.join(local_repo.path, "prompt.txt"), "w") as f:
        f.write("This is the original prompt")
    local_repo.add("prompt.txt")
    local_repo.commit("Initial prompt")
    local_repo.push("origin", "main")

    with pytest.raises(ValueError, match=r"Merge conflict"):
        remote_repo.merge("main", "update_prompt")
