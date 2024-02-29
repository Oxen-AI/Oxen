import os

from oxen import diff


def test_tabular_diff_added_row(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "Diffs")

    result = diff(
        os.path.join(repo_dir, "prompts.csv"),
        os.path.join(repo_dir, "prompts_added_row.csv"),
    )

    print(result.get())

    df = result.tabular.data
    assert df.shape[0] == 1
    assert df.shape[1] == 3


def test_text_diff_added_row(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "Diffs")

    result = diff(
        os.path.join(repo_dir, "prompts.txt"),
        os.path.join(repo_dir, "prompts_added_row.txt"),
    )

    print(result.get())

    assert result.text.num_added == 3
    assert result.text.num_removed == 1
