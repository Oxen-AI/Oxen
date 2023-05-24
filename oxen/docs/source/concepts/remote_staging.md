# Remote Staging Workflow

Oxen has the concept of a "remote staging area" to enable easy data collection and labeling workflows. There are two main types of data you might want to stage.

1) Unstructured data files (images, videos, audio, text)
2) Structured annotations (rows for tabular data frames)

Instead of cloning the entire dataset locally (which can take a lot of time, bandwidth, and storage) you can stage data directly on the remote server.

The commands you are used to working with in your local workspace (`status`, `add`, `commit`, etc...) now work with the remote staging area. Each user's changes are sand-boxed to their own identity, so when you add to a remote staging workspace, it will not overlap with other users.

# Staging Files

One problem with extending a dataset today is that you have to download the whole data repository locally to add a single data point. This is not ideal for large datasets. To avoid this extra workflow, oxen has the `remote` subcommand.

## Shallow Clone

To start, you can clone a repository with the `--shallow` flag. This flag downloads the metadata about the remote files, but not the files themselves. To make sure you are on the correct branch you should also pass the `-b` flag.

```bash
$ oxen clone https://hub.oxen.ai/ox/CatDogBoundingBox --shallow -b branch-name
$ cd CatDogBoundingBox
$ ls # note that no files have been pulled, leaving your repo in a shallow state
```

Note: When you do a shallow clone, your local commands will not work until you `oxen pull` the data. Pulling a branch will get you back to a fully synced state.

## Create Remote Branch

After you have a shallow clone, then you can create a local branch, and push it to the remote. Every remote branch has a remote staging area that is tied to the branch.

```bash 
$ oxen checkout -b add-images
$ oxen push origin add-images
```

## Check Remote Status

Now that you have created a remote branch, you can interact with the remote staging area with the `oxen remote` subcommand. The oxen remote subcommand defaults to checking the current branch you are on but on the remote server.

```bash
$ oxen remote status
```

## Remote Add File

To add a file to the remote staging area simply use `oxen remote add`.

```bash
$ oxen remote add image.jpg
```

For relative paths, oxen will mirror the directory structure you have locally.

```bash
$ mkdir my-images/ # create local dir
$ cp /path/to/image.jpg my-images/ # add image to local dir
$ oxen remote add my-images/image.jpg # upload image to remote staging area in the my-images/ directory
```

For absolute paths to a file, you will also need to specify the path you would like to put it in with the `-p` flag.

```bash
$ oxen remote add /path/to/image.jpg -p my-images # upload image to remote staging area
```

You can now use the `oxen remote status` command to see the files that are staged on the remote branch.

```bash
$ oxen remote status

Checking remote branch add-images -> 6f98e855fbc0fd1

Directories to be committed
  added: my-images with 1 file

Files to be committed:
  new file: my-images/image.jpg

```

## Delete Remotely Added File

If you accidentally add file from the remote staging area and want to remove it, no worries, you can unstage it with `oxen remote rm`. 

(TODO: right now the functionality only operates on staging area regardless of the --staged flag, we might want to allow remote removing of files and directories).

```bash
$ oxen remote rm --staged my-images/image.jpg
```

## Commit Staged Files

When you are ready to commit the staged data you can call the `oxen remote commit` command.

```bash
$ oxen remote commit -m "adding my file without pulling the whole repo"
```

You have now committed data to the remote branch without cloning the full repo 🎉.

Note: If the remote branch cannot be merged cleanly, the remote commit will fail, and you will have to resolve the merge conflicts with some more advanced commands which we will cover later.

## Remote Log

To see a list of remote commits on the branch you can use `remote log`. Your latest commit will be at the top of this list.

```bash
$ oxen remote log
```

# Staging Tabular Data

Commonly, you will want to tie some sort of annotation to your unstructured data. For example, you might want to label an image with a bounding box, or a video with a bounding box and a class label.

Oxen has native support for extending and managing structured DataFrames in the form of csv, jsonl, or parquet files. To interact with these files remotely you can use the `oxen remote df` command.

We will be focusing on adding data to these files, but you can also use the `oxen remote df` command to view the contents of a DataFrame with all the same parameters locally *TODO add link to df docs*.

```bash
$ oxen remote df annotations/train.csv # get a summary of the DataFrame

Full shape: (9000, 6)

Slice shape: (10, 6)
┌─────────────────────────┬────────┬───────┬────────┬────────┬────────┐
│ file                    ┆ height ┆ label ┆ min_x  ┆ min_y  ┆ width  │
│ ---                     ┆ ---    ┆ ---   ┆ ---    ┆ ---    ┆ ---    │
│ str                     ┆ f64    ┆ str   ┆ f64    ┆ f64    ┆ f64    │
╞═════════════════════════╪════════╪═══════╪════════╪════════╪════════╡
│ images/000000128154.jpg ┆ 129.58 ┆ cat   ┆ 0.0    ┆ 19.27  ┆ 130.79 │
│ images/000000544590.jpg ┆ 188.35 ┆ cat   ┆ 9.75   ┆ 13.49  ┆ 214.25 │
│ images/000000000581.jpg ┆ 116.08 ┆ dog   ┆ 49.37  ┆ 67.79  ┆ 74.29  │
│ images/000000236841.jpg ┆ 42.29  ┆ cat   ┆ 115.21 ┆ 96.65  ┆ 93.87  │
│ ...                     ┆ ...    ┆ ...   ┆ ...    ┆ ...    ┆ ...    │
│ images/000000201969.jpg ┆ 64.94  ┆ dog   ┆ 167.24 ┆ 73.99  ┆ 37.0   │
│ images/000000201969.jpg ┆ 38.95  ┆ dog   ┆ 110.81 ┆ 83.87  ┆ 18.02  │
│ images/000000201969.jpg ┆ 18.55  ┆ dog   ┆ 157.04 ┆ 133.63 ┆ 38.63  │
│ images/000000201969.jpg ┆ 71.11  ┆ dog   ┆ 97.72  ┆ 110.2  ┆ 35.9   │
└─────────────────────────┴────────┴───────┴────────┴────────┴────────┘
```

Say you want to add a bounding box annotation to this dataframe without cloning it locally. You can use the `--add-row` flag on the `oxen remote df` command to remotely stage a row on the DataFrame.

TODO: change the remote status to not be modified but be added


```bash
$ oxen remote df annotations/train.csv --add-row "my-images/image.jpg,dog,100,100,200,200"

shape: (1, 7)
┌──────────────────────────────────┬──────────────────────┬───────┬───────┬───────┬───────┬────────┐
│ _id                              ┆ file                 ┆ label ┆ min_x ┆ min_y ┆ width ┆ height │
│ ---                              ┆ ---                  ┆ ---   ┆ ---   ┆ ---   ┆ ---   ┆ ---    │
│ str                              ┆ str                  ┆ str   ┆ f64   ┆ f64   ┆ f64   ┆ f64    │
╞══════════════════════════════════╪══════════════════════╪═══════╪═══════╪═══════╪═══════╪════════╡
│ 744bc2f5736472a0b8fec3339bf14615 ┆ my-images/image3.jpg ┆ dog   ┆ 100.0 ┆ 100.0 ┆ 200.0 ┆ 200.0  │
└──────────────────────────────────┴──────────────────────┴───────┴───────┴───────┴───────┴────────┘
```

This returns a unique ID for the row that we can use as a handle to interact with the specific row in the remote staging area. To list the added rows on the dataframe you can use the `oxen remote diff` command.

```bash
$ oxen remote diff annotations/train.csv

Added Rows

shape: (2, 7)
┌──────────────────────────────────┬──────────────────────┬───────┬───────┬───────┬───────┬────────┐
│ _id                              ┆ file                 ┆ label ┆ min_x ┆ min_y ┆ width ┆ height │
│ ---                              ┆ ---                  ┆ ---   ┆ ---   ┆ ---   ┆ ---   ┆ ---    │
│ str                              ┆ str                  ┆ str   ┆ f64   ┆ f64   ┆ f64   ┆ f64    │
╞══════════════════════════════════╪══════════════════════╪═══════╪═══════╪═══════╪═══════╪════════╡
│ 822ac1facbd79444f1f33a2a0b2f909d ┆ my-images/image2.jpg ┆ dog   ┆ 100.0 ┆ 100.0 ┆ 200.0 ┆ 200.0  │
│ ab8e28d66d21934f35efcb9af7ce866f ┆ my-images/image3.jpg ┆ dog   ┆ 100.0 ┆ 100.0 ┆ 200.0 ┆ 200.0  │
└──────────────────────────────────┴──────────────────────┴───────┴───────┴───────┴───────┴────────┘
```

If you want to delete a staged row, you can delete it with the `--delete-row` flag and the value in the `_id` column.

```bash
$ oxen remote df annotations/train.csv --delete-row 822ac1facbd79444f1f33a2a0b2f909d
```

To clear all staged rows, you can use the `restore` subcommand to restore the file.

```bash
$ oxen remote restore --staged annotations/train.csv
```
