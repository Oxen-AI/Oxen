# Initializing and Committing

Oxen mirrors the commands from `git` that you know and love. This tutorial can serve as a refresher or a clean start if you have never used version control before.

We will be working with an example dataset that you can download from the [SmallCatDog.zip dataset](https://github.com/Oxen-AI/Oxen/raw/main/data/datasets/SmallCatDog.zip). It is a smaller dataset created from the Kaggle [Microsoft Cats vs Dogs dataset](https://www.kaggle.com/datasets/shaunthesheep/microsoft-catsvsdogs-dataset). It consists of 20 images of cats and dogs, along with some annotation files.

The goal of this tutorial is to show you how to stage and commit data so that you can have an auditable trail of where the data came from, as well as revert back to the data in different points of time.

We have the sample dataset hosted HERE TODO. Start by downloading the dataset and entering the directory.

```shell
$ wget https://github.com/Oxen-AI/Oxen/raw/main/data/datasets/SmallCatDog.zip .
$ unzip SmallCatDog.zip
$ cd SmallCatDog/
```

Before working with the data, it is always good to see if there is an existing oxen repository.

```shell
$ oxen status

fatal: no oxen repository exists, looking for directory: .oxen
```

If there is not one, you can create one with the `oxen init` command.

```shell
$ oxen init .

Initial commit 061d9dce-fb1f-4f1d-b5e7-d8d4145f82a9
🐂 repository initialized at: "~/Datasets/SmallCatDog"
```

Check the status again and you will see the untracked files.

```shell
$ oxen status

On branch main -> 061d9dce-fb1f-4f1d-b5e7-d8d4145f82a9

Untracked files:
  (use "oxen add <file>..." to update what will be committed)
  test/ with untracked 10 files
  annotations/ with untracked 2 files
  train/ with untracked 20 files
  labels.txt

```

You can see that there are 3 top level directories here, and one top level file. The file is a labels.txt that represents the class labels. There is one directory for the training images, one for the test images, and one for the annotation files. Let's use the `oxen add` command to add the top level `labels.txt` file to start.

```shell
oxen add labels.txt
```

Then we can check the status to see that the file is staged.

```shell
$ oxen status

On branch main -> 061d9dce-fb1f-4f1d-b5e7-d8d4145f82a9

Changes to be committed:
  added:  labels.txt

Untracked files:
  (use "oxen add <file>..." to update what will be committed)
  test/ with untracked 10 files
  annotations/ with untracked 2 files
  train/ with untracked 20 files

```

You can also add entire directories of files with the add command. Let's add the 20 files in the training data folder.

```shell
oxen add train
```

Then check the status again.

```shell
$ oxen status

On branch main -> 061d9dce-fb1f-4f1d-b5e7-d8d4145f82a9

Changes to be committed:
  added:  train/ with added 20 files
  added:  labels.txt

Untracked files:
  (use "oxen add <file>..." to update what will be committed)
  test/ with untracked 10 files
  annotations/ with untracked 2 files

```

Now we have some of the directory staged, but nothing is committed yet. Committing saves the files at this point in time so that we can always revert back to this version.

```shell
$ oxen commit -m "adding initial training images and labels.txt file"

Committing with message: adding initial training images and labels.txt file
```

Check the status again and we will see there are only two untracked directories remaining. You will also see a new commit id that the `main` branch is pointing to.

```shell
$ oxen status

On branch main -> fe9f31b4-dc50-4532-ba45-40cfb3d324cc

Untracked files:
  (use "oxen add <file>..." to update what will be committed)
  test/ with untracked 10 files
  annotations/ with untracked 2 files

```

Let's go ahead and add and commit the rest of the data.

```shell
oxen add .
```

```shell
$ oxen status

On branch main -> fe9f31b4-dc50-4532-ba45-40cfb3d324cc

Changes to be committed:
  added:  annotations/ with added 2 files
  added:  test/ with added 10 files

```

Then commit the data.

```shell
$ oxen commit -m "adding the test and annotations directory"

Committing with message: adding the test and annotations directory
```

Now we have a couple commits in our local history, which is nice if we want to go back to a previous version of the data. We can view the commit history with the `oxen log` command.

```shell
$ oxen log
commit eb1bdd88-949b-4570-87c0-f176b879785a

Author: greg
Date:   2022-06-12 08:03:50 UTC

    adding the test and annotations directory

commit fe9f31b4-dc50-4532-ba45-40cfb3d324cc

Author: greg
Date:   2022-06-12 07:55:09 UTC

    adding initial training images and labels.txt file

commit 061d9dce-fb1f-4f1d-b5e7-d8d4145f82a9

Author: greg
Date:   2022-06-12 07:50:30 UTC

    Initialized Repo 🐂

```

Another reason we use version control is for collaboration as well. For this we will have to sync the data to a remote server, which we will go over in the [next tutorial](2_CollabAdd.md).
