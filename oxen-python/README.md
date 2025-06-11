

<div align="center">
  <a href="https://docs.oxen.ai/" style="padding: 2px;">
    <img src="https://img.shields.io/badge/%F0%9F%93%9A-Documentation-245AF0" alt="Oxen.ai Documentation">
  </a>
  <a href="https://oxen.ai/" style="padding: 2px;">
    <img src="https://img.shields.io/badge/%F0%9F%90%82-Oxen%20Hub-245AF0" alt="Oxen.ai">
  </a>
  <a href="https://crates.io/crates/liboxen" style="padding: 2px;">
    <img src="https://img.shields.io/crates/v/liboxen.svg?color=245AF0" alt="Oxen.ai Crate"/>
  </a>
  <a href="https://pypi.org/project/oxenai/" style="padding: 2px;">
    <img src="https://img.shields.io/pypi/v/oxenai.svg?color=245AF0" alt="PyPi Latest Release"/>
  </a>
  <a href="https://discord.com/invite/s3tBEn7Ptg" style="padding: 2px;">
    <img src="https://dcbadge.vercel.app/api/server/s3tBEn7Ptg?compact=true&style=flat" alt ="Oxen.ai Discord">
  </a>
  <a href="https://twitter.com/oxen_ai" style="padding: 2px;">
    <img src="https://img.shields.io/twitter/url/https/twitter.com/oxenai.svg?style=social&label=Follow%20%40Oxen.ai" alt ="Oxen.ai Twitter">
  </a>
  <br/>
</div>

#

![Oxen.ai Logo](/images/oxen-no-margin-white.svg#gh-dark-mode-only)
![Oxen.ai Logo](/images/oxen-no-margin-black.svg#gh-light-mode-only)

## 🐂 What is Oxen?

Oxen is a lightning fast data version control system for structured and unstructured machine learning datasets. We aim to make versioning datasets as easy as versioning code.

The interface mirrors git, but shines in many areas that git or git-lfs fall short. Oxen is built from the ground up for data, and is optimized to handle large datasets, and large files.

```bash
oxen init
oxen add images/
oxen add annotations/*.parquet
oxen commit "Adding 200k images and their corresponding annotations"
oxen push origin main
```

Oxen is comprised of a [command line interface](https://docs.oxen.ai/getting-started/cli), as well as bindings for [Rust](https://github.com/Oxen-AI/Oxen) 🦀, [Python](https://docs.oxen.ai/getting-started/python) 🐍, and [HTTP interfaces](https://docs.oxen.ai/http-api) 🌎 to make it easy to integrate into your workflow.

## 🌾 What kind of data?

Oxen is designed to efficiently manage large datasets, including those with large individual files, for example CSV files with millions of rows. It also handles datasets comprising millions of individual files and directories such as the complete collection of ImageNet images.

## 🚀 Built for speed

One of the main reasons datasets are hard to maintain is the pure performance of indexing the data and transferring the data over the network. We wanted to be able to index hundreds of thousands of images, videos, audio files, and text files in seconds.

Watch below as we version **hundreds of thousands of images** in seconds 🔥

<p align="center">
    <img src="https://github.com/Oxen-AI/oxen-release/raw/main/images/cli-celeba.gif?raw=true" alt="oxen cli demo" />
</p>

But speed is only the beginning.

## ✅ Features

Oxen is built around ergonomics, ease of use, and it is easy to learn. If you know how to use git, you know how to use Oxen.

* 🔥 Fast (efficient indexing and syncing of data)
* 🧠 Easy to learn (same commands as git)
* 💪 Handles large files (images, videos, audio, text, parquet, arrow, json, models, etc)
* 🗄️ Index lots of files (millions of images? no problem)
* 📊 Native DataFrame processing (index, compare and serve up DataFrames)
* 📈 Tracks changes over time (never worry about losing the state of your data)
* 🤝 Collaborate with your team (sync to an oxen-server)
* 🌎 [Workspaces](https://docs.oxen.ai/concepts/workspace) to interact with the data without downloading it
* 👀 Better data visualization on [OxenHub](https://oxen.ai)

## 🔨 Next Up

- [ ] Hugging face compatible APIs
  - [ ] Upload model to hub
  - [ ] Download model with `transformers` library
  - [ ] Upload dataset to hub
  - [ ] Download dataset with `datasets` library
- [ ] Configurable storage backends
  - [ ] S3
  - [ ] Backblaze
  - [ ] GCS
  - [ ] Azure
- [ ] Block level deduplication

## 🐮 Learn The Basics

To learn what everything Oxen can do, the full documentation can be found at [https://docs.oxen.ai](https://docs.oxen.ai).

## 🧑‍💻 Getting Started

You can install through homebrew or pip or from our [releases page](https://github.com/Oxen-AI/Oxen/releases).

### 🐂 Install Command Line Tool

```bash CLI
brew tap Oxen-AI/oxen
brew install oxen
```

### 🐍 Install Python Library

```bash Python
pip install oxenai
```

### ⬇️ Clone Dataset

Clone your first Oxen repository from the [OxenHub](https://oxen.ai/explore).

<CodeGroup>

```bash CLI
oxen clone https://hub.oxen.ai/ox/CatDogBBox
```

## ⭐️ Every GitHub star gives an ox its wings

No really.

We hooked up the GitHub webhook for stars to an [OxenHub Repository](https://www.oxen.ai/ox/FlyingOxen). Learn how we did it and go find your own in our [ox/FlyingOxen](https://www.oxen.ai/ox/FlyingOxen) repository.

<p align="center">
    <img src="https://github.com/Oxen-AI/oxen-release/blob/main/images/ox-with-wings.png?raw=true" alt="oxen repo with wings" />
</p>

## 🤝 Support

If you have any questions, comments, suggestions, or just want to get in contact with the team, feel free to email us at `hello@oxen.ai`

## 👥 Contributing

This repository contains the Python library that wraps the core Rust codebase. We would love help extending out the python interfaces, the documentation, or the core rust library.

Code bases to contribute to:

* 🦀 [Core Rust Library](https://github.com/Oxen-AI/Oxen)
* 🐍 [Python Interface](https://github.com/Oxen-AI/oxen-release/tree/main/oxen)
* 📚 [Documentation](https://github.com/Oxen-AI/docs)

If you are building anything with Oxen.ai or have any questions we would love to hear from you in our [discord](https://discord.gg/s3tBEn7Ptg).

## Build 🔨

Set up virtual environment:

```Bash
# Set up your python virtual environment
$ python -m venv ~/.venv_oxen # could be python3
$ source ~/.venv_oxen/bin/activate
$ pip install -r requirements.txt
```

```Bash
# Install rust
$ curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Run maturin
$ cd oxen
$ maturin develop
```

## Test

```Bash
$ pytest -s tests/
```

## Why build Oxen?

Oxen was build by a team of machine learning engineers, who have spent countless hours in their careers managing datasets. We have used many different tools, but none of them were as easy to use and as ergonomic as we would like.

If you have ever tried [git lfs](https://git-lfs.com/) to version large datasets and became frustrated, we feel your pain. Solutions like git-lfs are too slow when it comes to the scale of data we need for machine learning.

If you have ever uploaded a large dataset of images, audio, video, or text to a cloud storage bucket with the name:

`s3://data/images_july_2022_final_2_no_really_final.tar.gz`

We built Oxen to be the tool we wish we had.

## Why the name Oxen?

"Oxen" 🐂 comes from the fact that the tooling will plow, maintain, and version your data like a good farmer tends to their fields 🌾. Let Oxen take care of the grunt work of your infrastructure so you can focus on the higher-level ML problems that matter to your product.

<!---------------------------------------------------------------------------->

[Learn The Basics]: https://img.shields.io/badge/Learn_The_Basics-37a779?style=for-the-badge

