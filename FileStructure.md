
## Local File Structure

To inspect any of the key value dbs below

```
oxen kvdb-inspect <PATH_TO_DB>
```

```
.oxen/
  HEAD (file that contains name of current "ref")

    ex) heads/main

  refs/ (keeps track of branch heads, remote names and their current commits)
    key,value db of:

    # Local heads
    heads/main -> COMMIT_ID
    heads/feature/add_cats -> COMMIT_ID
    heads/experiment/add_dogs -> COMMIT_ID

    # What has been pushed in these branches
    remotes/experiment/add_dogs -> COMMIT_ID

  staged/ (created from `oxen add <file>` command)
    dirs/ (rocksdb of directory names)
      key: path/to/dir
      value: {  }
    files/ (going to mimic dir structure for fast access to subset)
      path/
        to/
          dir/ (rocks db of files specific to that dir, with relative paths)
            key: filename.jpg
            value: {"hash": "FILE_HASH", "tracking_type": "tabular|regular"} (we generate a file ID and hash for each file that is added)

  history/ (list of commits)
    COMMIT_HASH_1/
      dirs/ (rocks db of dirnames in commit, similar to staged above, but could include computed metadata)
        key: path/to/dir
        value: { "count": 1000, "other_metadata": ? }
      files/
        path/
          to/
            dir/
              key: filename
              value: {
                "hash" => "FILE_HASH", (use this to know if a file was different)
                ... other metadata
              }

    COMMIT_HASH_2/
    COMMIT_HASH_3/

  commits/ (created from `oxen commit -m "my message"` command. Also generates history/commit_hash)
    key,value of:

    COMMIT_HASH -> Commit

    A Commit is an object that contains, can use parent for ordering the commit logs
      - Message
      - Parent Commit ID
      - Author
      - Timestamp

  versions/ (copies of original files, versioned with commit ids)
    //
    //       ex) 59E029D4812AEBF0 -> 59/E029D4812AEBF0
    //           72617025710EBB55 -> 72/617025710EBB55
    //
    FILE_HASH_DIRS_1/
      COMMIT_ID_1 (dog_1.jpg)
    FILE_HASH_DIRS_2/
      COMMIT_ID_1 (dog_2.jpg)
```
