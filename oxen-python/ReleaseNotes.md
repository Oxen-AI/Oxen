
# v0.6.0+2

## Features/Fixes
* `oxen remote download` now recursively works with directories
* Fix create branch logic on server side
* Fix csv parsing with carriage returns
* Improve APIs
* Improve internal error handling
* Improve liboxen crate.io docs
* Bump rust edition to 2021
* Bump all dependencies to latest
* Code organization


# v0.5.3

## Features/Improvements
* Creating liboxen crates.io package
* API to fetch commit history given a committish base..head
* API to check if commits are mergeable
* `oxen diff` can take two different files or a committish to compare
* Add pagination on output of log command
* Add --head and --tail to `oxen df` command
* Better error handling and messages
  * Cloning non-existent remote
  * Pushing to non-existent remote
  * remote df on an empty DataFrame


# v0.5.2

## Features
* Can rename local branch with `oxen branch -m <name>`
* Auto detect delimiter for csv's and try to infer
  * Can specify delimiter csv files for `oxen df` command with `-d`
  * Can specify delimiter via API for remote df
* Add progress bar to `oxen rm`

## Bug Fixes
* (#93) Set rocksdb MAX_OPEN_FILES to 128, and allow it to be set via environment variable

# v0.5.1

## Features
* Add `oxen remote ls` command to view what files are on your remote branch

## Bug Fixes
* Fully resolve path for `oxen remote add` command and check if it is in the repo directory before requiring `-p` flag
* Fix pagination on directories
* Fix `oxen rm -r` with a trailing slash on the directory
* Fix error message when trying to push and a remote is not set

# v0.5.0

## Features
* Remote staging area
  * `oxen remote status`
  * `oxen remote add file.png` 
  * `oxen remote commit -m "committing without cloning data"`
  * `oxen remote diff path/to/file.png` 
* Remote DataFrame CLI interface
  * `oxen remote df annotations/train.csv`
  * `oxen remote df annotations/train.csv --add-row 'file.png,label'`

## Bug Fixes
* DF api on DataFrames where we cannot detect the schema

# v0.4.11

* Fix cloning an empty remote repo
* Server confirms on push that you are synced to the correct tree
* Speed up restoring commit on checkout
* Fix bug with checking out version files that have not been synced
* Fix bug with adding new api key for host (replaces now instead of append)
* Add instructions / build for x86_64
* Bump dependencies flagged in dependabot
    * rocksdb, dotenv, reqwest, time

# v0.4.10+2

* Add health endpoint with disk usage info

# v0.4.10

* Update `oxen rm` to be more consistent with `git rm` functionality
  * Add `-r` flag to recursively remove directories that have been committed
  * Add `--staged` flag to remove staged changes that you do not want to commit.

# v0.4.9

* Added Windows Support and builds to CI
* Added Mac builds for OSX 11 and OSX 12
* Add support for .oxenignore file
* Fix start server usage message
* Get rid of unnecessary warnings on adding empty csvs

# v0.4.8

* Features
  * Default to pulling `main` on clone and add `--shallow` flag
* Bug Fixes
  * Prevent accidental double push
  * Support can unstaging a staged directory


# v0.4.7

* Features
  * Stats endpoint on repo
  * Consistent pagination params, page_num -> page
  * Better logging
  * Upgrade cargo 1.67.0

# v0.4.6

* Features
  * GET requests for downloading data

# v0.4.5

* Features
  * Checks version with oxen-server to verify we have an up to date client

# v0.4.4

* Bug Fixes
  * Fix reading CSVs with quotes and newlines
  * Fix page_size and page_num params for DataFrames
  * Fix partial pull then full
  * Fix pushing to a remote branch you are not currently on

# v0.4.3

* Features
    * Push commit size before syncing data to make sure we have enough remote storage
    * Add compute cache post commit to perform operations that we may want to cache per commit
    * Update is_synced api to give more detailed information on sync status
    * Add disk usage API for a namespace
    * Add pagination for data frame api
    * Add default for latest commit for an empty directory
    * Add retry and sleep for pulling data

# v0.4.2+1

* Features
    * Schema computation and naming now done in `oxen add` staging area before commit
    * Oxen Server API Changes
        * entry and meta endpoints return resource, version, path and datatype
    * Bump polars version
    * Support for json and jsonl file schemas
    * Add server driven warnings to CLI
* Bug Fixes
    * Infer schema from more lines (100 -> 10,000)

# v0.4.1

* Features
    * Faster data download 🔥
    * Chunked download data APIs

# v0.4.0

* Features
    * Faster data upload 🔥
    * Chunked upload data APIs

* Breaking Changes
    * Removed CADF
    * Remove `oxen index` commands

# v0.3.0

* Features
    * `oxen df --unique`
    * `oxen rm`
* Schema changes
    * add "email" to Commit
    * changed "timestamp" format on Commit to be RFC3339

# v0.2.7

* Features
    * oxen-server
        * HTTP APIs for fetching schemas and data frames
        * return more robust metadata on listing directories
    * oxen
        * support for multi argument filters ie "label == person && is_correct == true"
        * support for single or double quotes on aggregations
* Bug Fixes
    * fix restore tabular modified file

# v0.2.6

* Features
    * `oxen add` accepts multiple values
* Bug fixes
    * `oxen checkout` arg parsing
    * `oxen commit` files without extensions

# v0.2.5

* Bug fixes
    * Better logging
    * Don't fail silently on remote push

# v0.2.4

* Bug fixes
    * Handle duplicate lines in tabular data files
    * Fix `/dir` endpoint when no path is specified
    * Re-enable `/file` endpoint
* Add more tests to catch tabular data edge cases

# v0.2.3

*  `oxen df`
    * `-a` for aggregations
    * `--sort` to sort by a field
    * `--reverse` to reverse the order
* `oxen schemas`
    * `create_index` subcommand to create an index on a field
    * `query` subcommand to query an index
* Bug fixes on indexing DataFrames

# v0.2.2

Update `oxen restore`
* `oxen restore --staged <pathspec>` will remove a file or directory from staging
* `oxen restore <pathspec>` now works recursively

# v0.2.1

Performance Improvements:
* `oxen add` opening less db connections
* `oxen push` faster local delta computation

Better Formatting
* Printing Schemas

Bug Fixes:
* `oxen pull` progress bar fixes, unpacking fixes
* Extra print statements

# v0.2.0

* Add row level annotation tracking so that you can efficiently compare tabular data
* Add `oxen df` commands to help work with tabular data
* Update underlying repository structure to be more efficient per directory and more efficient with tabular data
* Solidify underlying API routes for HTTP server
* Add optional authorization on Oxen Server
* Can delete remote branches

