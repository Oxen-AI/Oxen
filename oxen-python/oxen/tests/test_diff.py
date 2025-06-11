import os

from oxen import diff


def test_tabular_diff_added_row(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "Diffs")

    result = diff(
        os.path.join(repo_dir, "prompts.csv"),
        os.path.join(repo_dir, "prompts_added_row.csv"),
    )

    df = result.tabular.data
    assert df.shape[0] == 1
    assert df.shape[1] == 3


def test_text_diff_markdown_file_no_changes(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "Diffs")

    # Add markdown file
    filename = os.path.join(repo_dir, "README.md")
    with open(filename, "w") as f:
        f.write(
            "# Cats vs. Dogs\n\nWhich is it? We will be using machine learning to find out!"
        )

    result = diff(filename, filename)

    print(result.get())

    assert result.text.num_added == 0
    assert result.text.num_removed == 0
