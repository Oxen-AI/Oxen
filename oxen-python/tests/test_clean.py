import os

from oxen import Repo


def _init_repo_with_one_tracked_file(repo_dir, tracked_name="tracked.txt"):
    """Init an oxen repo at `repo_dir` with a single committed file, returning
    the `Repo` plus the absolute path of the tracked file.
    """
    os.makedirs(repo_dir, exist_ok=True)
    repo = Repo(repo_dir)
    repo.init()
    tracked = os.path.join(repo_dir, tracked_name)
    with open(tracked, "w") as f:
        f.write("tracked contents")
    repo.add(tracked)
    repo.commit("add tracked file")
    return repo, tracked


def test_clean_default_is_dry_run(tmp_path):
    repo, _ = _init_repo_with_one_tracked_file(str(tmp_path / "r"))
    untracked = os.path.join(repo.path, "scratch.txt")
    with open(untracked, "w") as f:
        f.write("stale")

    result = repo.clean()

    assert result.applied is False
    assert os.path.exists(untracked), "dry-run must not delete anything"
    assert any(filter(lambda p: os.path.basename(p) == "scratch.txt", result.files))


def test_clean_force_removes_untracked_file(tmp_path):
    repo, _ = _init_repo_with_one_tracked_file(str(tmp_path / "r"))
    untracked = os.path.join(repo.path, "scratch.txt")
    with open(untracked, "w") as f:
        f.write("stale")

    result = repo.clean(force=True)

    assert result.applied is True
    assert not os.path.exists(untracked)
    assert "scratch.txt" in {os.path.basename(p) for p in result.files}


def test_clean_force_removes_untracked_dir(tmp_path):
    repo, _ = _init_repo_with_one_tracked_file(str(tmp_path / "r"))
    junk_dir = os.path.join(repo.path, "junk")
    os.makedirs(junk_dir)
    with open(os.path.join(junk_dir, "a.txt"), "w") as f:
        f.write("a")
    with open(os.path.join(junk_dir, "b.txt"), "w") as f:
        f.write("b")

    result = repo.clean(force=True)

    assert result.applied is True
    assert not os.path.exists(junk_dir)
    assert "junk" in {os.path.basename(p) for p in result.dirs}


def test_clean_preserves_tracked_files_and_oxen(tmp_path):
    repo, tracked = _init_repo_with_one_tracked_file(str(tmp_path / "r"))
    untracked = os.path.join(repo.path, "scratch.txt")
    with open(untracked, "w") as f:
        f.write("stale")

    repo.clean(force=True)

    assert os.path.exists(tracked), "tracked files must survive clean"
    assert os.path.isdir(os.path.join(repo.path, ".oxen")), ".oxen/ must survive clean"
    assert not os.path.exists(untracked)


def test_clean_path_scoping(tmp_path):
    repo, _ = _init_repo_with_one_tracked_file(str(tmp_path / "r"))
    sub_a = os.path.join(repo.path, "subtree_a")
    sub_b = os.path.join(repo.path, "subtree_b")
    os.makedirs(sub_a)
    os.makedirs(sub_b)
    with open(os.path.join(sub_a, "x.txt"), "w") as f:
        f.write("x")
    with open(os.path.join(sub_b, "y.txt"), "w") as f:
        f.write("y")

    repo.clean(paths=[sub_a], force=True)

    assert not os.path.exists(sub_a), "subtree_a should be removed (in scope)"
    assert os.path.exists(os.path.join(sub_b, "y.txt")), (
        "subtree_b should survive (out of scope)"
    )


def test_clean_result_counts(tmp_path):
    repo, _ = _init_repo_with_one_tracked_file(str(tmp_path / "r"))
    with open(os.path.join(repo.path, "a.txt"), "w") as f:
        f.write("12345")  # 5 bytes
    with open(os.path.join(repo.path, "b.txt"), "w") as f:
        f.write("hi")  # 2 bytes
    junk_dir = os.path.join(repo.path, "junk")
    os.makedirs(junk_dir)
    with open(os.path.join(junk_dir, "c.txt"), "w") as f:
        f.write("xyz")  # 3 bytes

    result = repo.clean()

    assert len(result.files) == 2
    assert len(result.dirs) == 1
    assert result.total_bytes == 10
