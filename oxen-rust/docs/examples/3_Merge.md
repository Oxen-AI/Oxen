# Merge Branches

Once you have added data that you are convinced improve the overall model and you want to merge the changes into the mainline there is the `oxen merge` command. We will be starting from a repo that was initialized [here](2_CollabAdd.md).

If you weren't following the example before, you can clone and pull from this remote:

(TODO: have server running we can pull from)

```shell
oxen clone http://0.0.0.0:3000/repositories/SmallCatDog
cd SmallCatDog
oxen pull origin main
```

Make sure that we have pulled all the entries on the branch. First let's make sure the branches are synced.

TODO: Implement the `fetch` command.

```shell
$ oxen fetch

Fetching remote branches...
Checking branch: main
Checking branch: add-training-data
Downloading meta-data for branch: add-training-data
```

To verify which branch you are on, as well as see the other branches that exist locally there is the `branch` command. To list them all use `-a`

```shell
$ oxen branch -a

* main
add-training-data
```

The asterisk next to the main branch indicates that we are on the main branch.

We need to checkout and pull the entries on the training data branch. TODO: Do we want to checkout the branch when you pull? Or make it separate commands?

```shell
$ oxen pull origin add-training-data
```

You should see the new images dog_200.jpg...dog_209.jpg in the train/ dir.

```shell
$ ls train/

cat_0.jpg	cat_3.jpg	cat_6.jpg	cat_9.jpg	dog_2.jpg	dog_202.jpg	dog_205.jpg	dog_208.jpg	dog_4.jpg	dog_7.jpg
cat_1.jpg	cat_4.jpg	cat_7.jpg	dog_0.jpg	dog_200.jpg	dog_203.jpg	dog_206.jpg	dog_209.jpg	dog_5.jpg	dog_8.jpg
cat_2.jpg	cat_5.jpg	cat_8.jpg	dog_1.jpg	dog_201.jpg	dog_204.jpg	dog_207.jpg	dog_3.jpg	dog_6.jpg	dog_9.jpg
```

Checkout the main branch again because it is the target we want to merge into.

```shell
$ oxen checkout main

Checkout branch: main
Setting working directory to 11f6c5d5-f683-42b2-9d6e-a82172509eed
```

The images will temporarily disappear from the training data directory, until we merge in the branch.

```shell
$ oxen merge add-training-data

Successfully merged `add-training-data` into `main`
HEAD -> 7d6258e0-5956-4695-aa13-6844b3c73e6d
```

Since this branch was simply a set of additions that were added after the last change to the main branch, and no one made any changes inbetween our changes, it simply will fast-forward the changes to our commit. Now you should have all 30 images in the training directory again. We can see this by piping the output of the `ls` command into a line count command `wc -l`.

```shell
ls train | wc -l

30
```

Assuming we don't want to use this branch anymore you can delete it with the `-d` flag. TODO: Implement this.

```shell
oxen branch -d add-training-data
```

Let's consider a more complicated scenario. Say we have multiple people working on the same dataset. One of them is tasked with adding a `fish` category, but the other is in the process of adding a `human` category. If we are following best practices, we should have two separate branches for these to features so that we can work in parallel.

The person working on the `fish` label might create a branch called `add-fish-label` and the other person might make a branch called `add-human-label`

From one workstation lets start by adding the `fish` label.

```shell
$ oxen checkout -b add-fish-label # create & checkout branch
$ echo "fish" >> labels.txt # append the "fish" label to our labels file
$ oxen add labels.txt # stage the labels file
$ oxen commit -m "added fish label to labels file" # commit the change
```

Then add 10 images of fish from another directory. In this example the user has [tiny-imagenet](https://www.kaggle.com/datasets/akash2sharma/tiny-imagenet) downloaded to their ~/Datasets/tiny-imagenet-200/ directory.

```shell
# copy over 10 images of fish
# bash
$ for i in $(seq 200 209) ; do cp ~/Datasets/tiny-imagenet-200/train/n01443537/images/n01443537_$i.JPEG train/fish_$i.jpg ; done
# fish
$ for i in (seq 0 9) ; cp ~/Datasets/tiny-imagenet-200/train/n01443537/images/n01443537_$i.JPEG train/fish_$i.jpg ; end
$ oxen add train # stage the train directory
$ oxen commit -m "added ten images of fish to training data" # commit the changes
$ oxen push origin add-fish-label # push to remote
```

Then from another branch (or another workstation) let's try to add the humans label to the same labels file (this will cause a conflict).

TODO: if we have file name semantics... we can do custom merges where we could catch this scenario...?

```shell
$ oxen checkout main # Make sure we are branching from the default main branch
$ oxen checkout -b add-human-label # create & checkout branch
$ echo "human" >> labels.txt # append the "human" label to our labels file
$ oxen add labels.txt # stage the labels file
$ oxen commit -m "added human label to labels file" # commit the change
```

Then we add ten images of humans from this [human action recognition dataset](https://www.kaggle.com/datasets/meetnagadia/human-action-recognition-har-dataset).

```shell
# copy over 10 images of humans
# bash
$ for i in $(seq 200 209) ; do cp ~/Datasets/tiny-imagenet-200/HumanActionRecognition/train/Image_$i.jpg train/human_$i.jpg ; done
# fish
$ for i in (seq 1 10) ; cp ~/Datasets/tiny-imagenet-200/HumanActionRecognition/train/Image_$i.jpg train/human_$i.jpg ; end
$ oxen add train # stage the changes
$ oxen commit -m "add a new label of humans, with two new training images" # commit the changes
$ oxen push origin add-human-label # push the changes to the remote
```

Now we have two branches that have added different labels and new images for each category. Let's say that the fish branch merged in first.

```shell
$ oxen checkout main
$ oxen merge add-fish-label

Updating 900b70e2-1724-45ec-9056-dc0aabb729c8 -> c245849a-92c4-4f4c-850a-4ca12f552de6
Fast-forward
Successfully merged `add-fish-category` into `main`
HEAD -> c245849a-92c4-4f4c-850a-4ca12f552de6
```

First merge goes smoothly again since it is simply an addition without any conflicts. Now let's try to merge in the `add-human-label` branch.

```shell
$ oxen merge add-human-label

Updating c245849a-92c4-4f4c-850a-4ca12f552de6 -> 894ccc6b-58bf-4593-a458-dd34f88b012f
Automatic merge failed; fix conflicts and then commit the result.
```

We can now see that there were merge conflicts and the merge failed. If we look at the status we can see which files could not automatically be merged.

TODO: How do we want to display/diff these changes? There is [this library](https://docs.rs/diffy/latest/diffy/index.html) for text files.

Thoughts:
* Make it easy to choose from the three versions
* Less likely they want a line by line diff...even for labels or csv file...?
* Tool to show images, audio, video next to eachother/


```shell
$ oxen status

On branch main -> c245849a-92c4-4f4c-850a-4ca12f552de6

Unmerged paths:
  (use "oxen add <file>..." to mark resolution)
  both modified:  labels.txt

```

If we look at the file, it defaults to the version that was on the main branch in the HEAD commit.

```shell
$ cat labels.txt
cat
dog
fish
```

Let's add back in the human category add the end of the file, add and commit it. This will create the merge commit linking both parents where the conflict occurred.

TODO: A command similar to `git log --oneline --graph --decorate --all` to display the tree.

```shell
$ echo "human" >> labels.txt
$ oxen add labels.txt
$ oxen commit -m "merge human label into labels.txt file"
```
