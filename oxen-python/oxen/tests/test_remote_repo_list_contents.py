def test_remote_repo_list_contents(celeba_remote_repo_fully_pushed):
    """
    Tests both `ls()` (paginated) and `scan()` (generator)
    by pushing actual data from a local repo to a remote repo.
    """

    local_repo, remote_repo = celeba_remote_repo_fully_pushed

    # paginated ls()
    page_1_after = remote_repo.ls(directory="images", page_num=1, page_size=10)
    assert len(page_1_after.entries) > 0

    # generator scan()
    scanned_files = list(remote_repo.scan("images"))
    assert len(scanned_files) > 0

    all_files_paginated = []
    page_num = 1

    while True:
        page = remote_repo.ls(directory="images", page_num=page_num, page_size=10)

        if not page.entries:
            break

        all_files_paginated.extend(page.entries)
        page_num += 1

    assert len(scanned_files) == len(all_files_paginated)

    scanned_names = {entry.filename for entry in scanned_files}
    paginated_names = {entry.filename for entry in all_files_paginated}
    assert scanned_names == paginated_names
