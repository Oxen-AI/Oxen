import os

import pandas as pd
from pathlib import PurePath

from oxen import DataFrame, RemoteRepo, Workspace


def test_data_frame_crud(celeba_remote_repo_fully_pushed):
    _, remote_repo = celeba_remote_repo_fully_pushed

    image = str(PurePath("images", "123456.png"))
    new_row = {"file": str(image), "hair_color": "purple"}

    train_path = str(PurePath("annotations", "train.csv"))
    df = DataFrame(
        remote_repo.identifier,
        train_path,
        host="localhost:3000",
        scheme="http",
    )
    _width, og_height = df.size()

    # List the rows
    rows = df.list_page(1)
    assert len(rows) > 0, "Error listing rows"

    # Add a row
    row_id = df.insert_row(new_row)
    _width, height = df.size()
    assert height == og_height + 1, "Error adding to test remove"

    # Update a row
    df.update_row(row_id, {"hair_color": "blue"})
    row = df.get_row_by_id(row_id)
    assert row["hair_color"] == "blue", "Error updating row"

    # Remove a row
    df.delete_row(row_id)
    _width, height = df.size()
    assert height == og_height, "Error removing row"


def test_data_frame_create_on_insert(celeba_remote_repo_fully_pushed):
    _, remote_repo = celeba_remote_repo_fully_pushed

    new_file = "logs/prompts.jsonl"
    new_row = {
        "prompt": "what is the best data version control tool?",
        "response": "You should check out Oxen.ai",
    }

    df = DataFrame(
        remote_repo.identifier,
        new_file,
        host="localhost:3000",
        scheme="http",
    )

    # Add a row and commit
    row_id = df.insert_row(new_row)
    _width, height = df.size()
    assert height == 1, "DataFrame should have 1 row"

    rows = df.list_page(1)
    assert len(rows) == 1, "DataFrame should have 1 row"
    row = df.get_row_by_id(row_id)

    assert row["prompt"] == new_row["prompt"], "Prompt should match"
    assert row["response"] == new_row["response"], "Response should match"

    # insert another row
    second_row = {
        "prompt": "what is the fastest data version control tool?",
        "response": "Oxen.ai scales and is fast",
    }
    df.insert_row(second_row)
    rows = df.list_page(1)
    assert len(rows) == 2, "DataFrame should have 2 rows"

    assert rows[0]["prompt"] == new_row["prompt"], "Prompt should match"
    assert rows[0]["response"] == new_row["response"], "Response should match"
    assert rows[1]["prompt"] == second_row["prompt"], "Prompt should match"
    assert rows[1]["response"] == second_row["response"], "Response should match"


def test_data_frame_create_on_insert_on_branch(celeba_remote_repo_fully_pushed):
    _, remote_repo = celeba_remote_repo_fully_pushed

    new_file = "logs/prompts.jsonl"
    new_row = {
        "prompt": "what is the best data version control tool?",
        "response": "You should check out Oxen.ai",
    }

    df = DataFrame(
        remote_repo.identifier,
        new_file,
        host="localhost:3000",
        scheme="http",
        branch="test-branch",
    )

    # Add a row and commit
    row_id = df.insert_row(new_row)
    _width, height = df.size()
    assert height == 1, "DataFrame should have 1 row"

    rows = df.list_page(1)
    assert len(rows) == 1, "DataFrame should have 1 row"
    row = df.get_row_by_id(row_id)

    assert row["prompt"] == new_row["prompt"], "Prompt should match"
    assert row["response"] == new_row["response"], "Response should match"

    # insert another row
    second_row = {
        "prompt": "what is the fastest data version control tool?",
        "response": "Oxen.ai scales and is fast",
    }
    df.insert_row(second_row)
    rows = df.list_page(1)
    assert len(rows) == 2, "DataFrame should have 2 rows"

    assert rows[0]["prompt"] == new_row["prompt"], "Prompt should match"
    assert rows[0]["response"] == new_row["response"], "Response should match"
    assert rows[1]["prompt"] == second_row["prompt"], "Prompt should match"
    assert rows[1]["response"] == second_row["response"], "Response should match"


def test_data_frame_create_on_insert_on_branch_instantiated_from_remote_repo(
    celeba_remote_repo_fully_pushed,
):
    _, remote_repo = celeba_remote_repo_fully_pushed

    new_file = "logs/prompts.jsonl"
    new_row = {
        "prompt": "what is the best data version control tool?",
        "response": "You should check out Oxen.ai",
    }

    remote_repo.create_checkout_branch("test-branch")
    df = DataFrame(remote_repo, new_file)

    # Add a row and commit
    row_id = df.insert_row(new_row)
    _width, height = df.size()
    assert height == 1, "DataFrame should have 1 row"

    rows = df.list_page(1)
    assert len(rows) == 1, "DataFrame should have 1 row"
    row = df.get_row_by_id(row_id)

    assert row["prompt"] == new_row["prompt"], "Prompt should match"
    assert row["response"] == new_row["response"], "Response should match"

    # insert another row
    second_row = {
        "prompt": "what is the fastest data version control tool?",
        "response": "Oxen.ai scales and is fast",
    }
    df.insert_row(second_row)
    rows = df.list_page(1)
    assert len(rows) == 2, "DataFrame should have 2 rows"

    assert rows[0]["prompt"] == new_row["prompt"], "Prompt should match"
    assert rows[0]["response"] == new_row["response"], "Response should match"
    assert rows[1]["prompt"] == second_row["prompt"], "Prompt should match"
    assert rows[1]["response"] == second_row["response"], "Response should match"


def test_data_frame_commit(celeba_remote_repo_fully_pushed):
    _, remote_repo = celeba_remote_repo_fully_pushed

    image = str(PurePath("images", "123456.png"))
    new_row = {"file": image, "hair_color": "purple"}

    train_path = str(PurePath("annotations", "train.csv"))
    df = DataFrame(
        remote_repo.identifier,
        train_path,
        host="localhost:3000",
        scheme="http",
    )

    # List commits before
    og_commits = remote_repo.log()

    # Add a row and commit
    df.insert_row(new_row)
    df.commit("Add row")

    # List commits after
    new_commits = remote_repo.log()
    assert len(new_commits) == len(og_commits) + 1, "Error committing row"


def test_remove_data_frame_row(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed

    file_path = os.path.join(shared_datadir, "CelebA", "annotations", "train.csv")
    _df = pd.read_csv(file_path)
    workspace = Workspace(remote_repo, "main")
    print("Created workspace ", workspace)

    image = str(PurePath("images", "123456.png"))
    new_row = {"file": image, "hair_color": "purple"}

    assert len(workspace.status().added_files()) == 0
    workspace.add(file_path, "csvs")
    workspace.commit("add train.csv")

    train_path = str(PurePath("csvs", "train.csv"))
    workspace = Workspace(remote_repo, "main")
    df = DataFrame(workspace, train_path)
    _width, og_height = df.size()

    row_id = df.insert_row(new_row)
    _width, height = df.size()
    assert height == og_height + 1, "Error adding to test remove"
    df.delete_row(row_id)
    _width, height = df.size()
    assert height == og_height, "Error removing row"


def test_data_frame_add_column(celeba_remote_repo_fully_pushed):
    _, remote_repo = celeba_remote_repo_fully_pushed

    new_file = "logs/prompts.jsonl"
    new_row = {
        "prompt": "what is the best data version control tool?",
        "response": "You should check out Oxen.ai",
    }

    remote_repo.create_checkout_branch("test-branch")
    df = DataFrame(remote_repo, new_file)

    # Add a row
    df.insert_row(new_row)
    # Get the columns
    columns = df.get_columns()
    assert (
        len(columns) == 3
    ), "DataFrame should have 3 columns (plus the _oxen_id column)"

    # Add a column
    df.add_column("new_column", "str")

    # Get the columns again
    columns = df.get_columns()
    assert len(columns) == 4, "DataFrame should have 4 columns"
    # make sure we have the new column
    assert "new_column" in [c.name for c in columns], "New column should be added"
