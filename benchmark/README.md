# 1 Billion File Version Control Benchmark

Modern machine learning datasets consist of web scale datasets to pre-train on. We are no longer in the days of training models on a few thousand images. We are now in the days of training models on billions or trillions of data points. These data points may be images, text, audio, video, or any other type of data, often sharded into many different files.

Knowing exactly what version of a dataset was used to train a model is critical to ensure reproducibility, auditability, and safety. In turn, scalable version control tools are a critical step in the development of trustworthy AI.

This is a benchmark to test how well version control systems scale to arbitrary large datasets. The main goal is to see how well Oxen and other VCS's handle a few different configurations.

# Data Generation

We have provided a script to generate a dataset with an arbitrary number of files, split within subdirectories.

```bash
python benchmark/generate_image_repo.py --output_dir ~/Data/10k_images --num_images 10000 --num_dirs 1000 --image_size 128 128
```

# Configurations

Within each configuration there are multiple different configurations.

1) A single sub directory containing all the files

```
README.md
labels.csv
images/
  image_1.jpg
  image_2.jpg
  image_3.jpg
  ...
  image_1_000_000_0000.jpg
```

2) Two sub directories of files with a train/test split

```
README.md
labels.csv
images/
  train/
    image_1.jpg
    image_2.jpg
    image_3.jpg
    ...
    image_900_000_0000.jpg
  test/
    image_1.jpg
    image_2.jpg
    image_3.jpg
    ...
    image_100_000_0000.jpg
```

3) Many sub directories of files, one per category of data

```
README.md
labels.csv
images/
  cat/
    cat1.jpg
    cat2.jpg
    cat3.jpg
    ...
    cat_1_000_0000.jpg
  dog/
    dog1.jpg
    dog2.jpg
    dog3.jpg
    ...
    dog_1_000_0000.jpg
  ...
  frog/
    frog_1.jpg
    frog_2.jpg
    frog_3.jpg
    ...
    frog_1_000_0000.jpg
```

These configurations are commonly used in machine learning datasets, and are a good starting point for testing how well a version control system scales.

# Benchmarks

For each configuration we will run a few different benchmarks.

* add time
* commit time
* push time
* pull time
* local repository size on disk
* remote repository size on disk
* validate merkle tree time
* list remote directory time
* get remote file time

## 10k Files

### Single Directory

### Two Sub Directories

### Many Sub Directories

Baseline
--------

10_000 files in 1_000 subdirectories

oxen add - 25.36 secs
oxen commit - 64.58 secs
oxen push - ?
oxen clone - ?

Disk Usage
```
36K	.oxen/commits
4.0K	.oxen/config.toml
4.0K	.oxen/HEAD
264K	.oxen/history
 20K	.oxen/merge
1.5M	.oxen/objects
 40K	.oxen/refs
 35M	.oxen/staged
8.0K	.oxen/sync_status
1.9G	.oxen/versions
```


Merkle Tree
-----------

TODO

## 100k Files

100k files 10k folders

oxen add - 290.11 secs
oxen commit - 547.43 secs



## 200k Files

oxen add - 14.21 sec
oxen commit - 119.97 sec
oxen push - ?
oxen clone - ?

Disk Usage
```
 36K	.oxen/commits
4.0K	.oxen/config.toml
4.0K	.oxen/HEAD
240K	.oxen/history
 20K	.oxen/merge
 13M	.oxen/objects
 40K	.oxen/refs
 15M	.oxen/staged
8.0K	.oxen/sync_status
9.9G	.oxen/versions
```

## 1 Million Files

### Single Directory

### Two Sub Directories

1m_images> oxen add .
🐂 oxen added 1000006 files (49.4 GB) in 1820.286081054s
gregschoeninger@Gregs-iMac-Pro ~/D/1m_images> oxen commit -m "adding 1 million files"
Committing with message: adding 1 million files
🐂 commit dca9c304d2884dcc21c8cd213f68228d in 67.50107683s

### Many Sub Directories

1m files 1000 folders

oxen add - 140.30 secs
oxen commit - 63.55 mins

Load Tree Legacy
----
Time to load: 223.039642208s
Time to print: 3.051239563s

Load Tree New
----
Time to load tree: 5.17906844s
Time to print tree: 6.319524273s

## 10 Million Files

## 100 Million Files

## 1 Billion Files








