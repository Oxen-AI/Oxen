import os

from oxen import Repo


def test_init(empty_local_dir):
    # Test that we make the oxen hidden dir when we init()
    oxen_hidden_dir = os.path.join(empty_local_dir, ".oxen")
    assert not os.path.exists(oxen_hidden_dir)

    repo = Repo(empty_local_dir)
    repo.init()

    assert os.path.exists(oxen_hidden_dir)
