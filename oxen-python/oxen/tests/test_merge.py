import os


def test_merge(house_prices_local_repo_fully_committed):
    repo = house_prices_local_repo_fully_committed

    prices_file = "prices.csv"
    full_path = os.path.join(repo.path, prices_file)
    initial_branch = repo.current_branch.name

    # read initial prices.csv contents
    with open(full_path, "r") as f:
        initial_contents = f.read()

    # oxen checkout -b new_branch
    new_branch = "new_branch"
    repo.checkout(new_branch, create=True)
    assert repo.current_branch.name == "new_branch"

    # update prices.csv
    new_line = "6000000,6000,7,7,2015"
    with open(full_path, "a") as f:
        f.write(new_line)

    # oxen add prices.csv
    repo.add(full_path)

    # oxen commit
    repo.commit("Add new price")
    with open(full_path, "r") as f:
        updated_contents = f.read()

    # oxen checkout main
    repo.checkout(initial_branch)
    assert repo.current_branch.name == initial_branch
    with open(full_path, "r") as f:
        contents = f.read()
        assert contents == initial_contents

    # oxen merge new_branch
    repo.merge(new_branch)
    assert repo.current_branch.name == initial_branch
    with open(full_path, "r") as f:
        contents = f.read()
        assert contents == updated_contents
