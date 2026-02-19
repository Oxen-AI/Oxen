import os
from pathlib import Path

from oxen import RemoteRepo, Workspace
from oxen.workspace import _filepaths_from
from pytest import raises


def test_workspace_add_single_file(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main", "test-workspace")

    full_path: Path = shared_datadir / "CelebA" / "images" / "1.jpg"

    # test that this accepts a Path
    workspace.add(full_path, "a-folder")
    status = workspace.status()
    added_files = status.added_files()

    assert len(added_files) == 1
    assert added_files[0] == str(Path("a-folder") / "1.jpg")


def test_workspace_add_dir(celeba_remote_repo_one_image_pushed, shared_datadir):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main", "test-workspace")

    full_path: Path = shared_datadir / "CelebA" / "images"

    # test that this accepts a str
    workspace.add(str(full_path), "")
    status = workspace.status()
    added_files = status.added_files()

    assert len(added_files) == 9
    assert sorted(added_files) == sorted([f"{i}.jpg" for i in range(1, 10)])


def test_workspace_add_many(celeba_remote_repo_one_image_pushed, shared_datadir):

    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main", "test-workspace")

    shared_datadir = Path(shared_datadir)
    image_paths: list[Path] = [
        shared_datadir / "CelebA" / "images" / f"{i}.jpg" for i in [1, 2, 3]
    ]
    image_paths.append(shared_datadir / "Diffs")

    # force it to actually work on an Iterable[str]
    workspace.add(map(str, image_paths), "")
    status = workspace.status()
    added_files = status.added_files()

    assert len(added_files) == 7

    assert sorted(added_files) == sorted(
        [
            "1.jpg",
            "2.jpg",
            "3.jpg",
            "prompts_added_row.csv",
            "prompts_added_row.txt",
            "prompts.csv",
            "prompts.txt",
        ]
    )


def test_workspace_add_files_preserve_paths(celeba_remote_repo_one_image_pushed, shared_datadir):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main", "test-workspace")

    shared_datadir = Path(shared_datadir)
    image_paths: list[Path] = [
        shared_datadir / "CelebA" / "images" / f"{i}.jpg" for i in [1, 2, 3]
    ]

    # force it to actually work on an Iterable[str]
    workspace.add_files(shared_datadir, image_paths)

    status = workspace.status()
    added_files = status.added_files()

    assert len(added_files) == 3
    assert sorted(added_files) == sorted(
        [
            'CelebA/images/1.jpg', 'CelebA/images/2.jpg', 'CelebA/images/3.jpg'
        ]
    )

def test_workspace_add_invalid_path(tmp_path, celeba_remote_repo_one_image_pushed):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main", "test-workspace")

    invalid_paths = [
        tmp_path / invalid
        for invalid in (
            "not_real",
            "not_here",
            "oh_this_will_be_fun",
        )
    ]

    # force it to actually work on an Iterable[Path]
    with raises(ValueError):
        workspace.add(invalid_paths, "")


def test_filepaths_from(tmp_path):
    dir_root = tmp_path / "dir_root"
    dir_root.mkdir()

    # test a few files in the directory root
    (dir_root / "X").touch()
    (dir_root / "Y").touch()

    # test a few sub-directories, each with a file
    for name, file in [("dir_a", "A"), ("dir_b", "B"), ("dir_c", "C")]:
        d = dir_root / name
        d.mkdir()
        (d / file).touch()

    # Test a very nested case
    depth_2 = dir_root / "depth_0" / "depth_1" / "depth_2"
    depth_2.mkdir(parents=True)
    for file in ["D", "E", "F", "G", "H"]:
        (depth_2 / file).touch()

    result = sorted(str(p.relative_to(dir_root)) for p in _filepaths_from(dir_root))

    expected = sorted(
        [
            "X",
            "Y",
            # NOTE: For windows, we can't use `/`,
            #       so we use `os.path.join` to re-create the right string.
            os.path.join("dir_a", "A"),
            os.path.join("dir_b", "B"),
            os.path.join("dir_c", "C"),
            os.path.join("depth_0", "depth_1", "depth_2", "D"),
            os.path.join("depth_0", "depth_1", "depth_2", "E"),
            os.path.join("depth_0", "depth_1", "depth_2", "F"),
            os.path.join("depth_0", "depth_1", "depth_2", "G"),
            os.path.join("depth_0", "depth_1", "depth_2", "H"),
        ]
    )

    assert result == expected
