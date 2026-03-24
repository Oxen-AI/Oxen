# Merge Conflict

Here are some commands to generate a merge conflict:

```bash
# create empty directory
$ mkdir merge-conflict-demo
$ cd merge-conflict-demo

# create a new oxen repo and add a test csv file on main
$ oxen init
$ echo "file,label" > test.csv
$ echo "images/dog.png,dog" >> test.csv
$ oxen add test.csv
$ oxen commit -m "adding a dog"

# checkout a new branch where you are going to append a cat
$ oxen checkout -b adding-cat
$ echo "images/cat.png,cat" >> test.csv
$ oxen add test.csv
$ oxen commit -m "adding a cat"

# checkout the main branch again, and append a fish
$ oxen checkout main
$ echo "images/fish.png,fish" >> test.csv
$ oxen add test.csv
$ oxen commit -m "adding a fish"

# try to merge the branch with the cat (should fail)
$ oxen merge adding-cat
```