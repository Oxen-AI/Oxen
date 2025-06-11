import pytest
import logging

import uuid
import os
from pathlib import PurePath

from oxen import Repo, RemoteRepo

FORMAT = "%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s"
logging.basicConfig(format=FORMAT)

if "OXEN_TEST_LOG" in os.environ:
    level = os.environ["OXEN_TEST_LOG"].lower()
    if "debug" == level:
        logging.getLogger().setLevel(logging.DEBUG)
    elif "info" == level:
        logging.getLogger().setLevel(logging.INFO)

TEST_SCHEME = "http"
TEST_HOST = "localhost:3000"
if "OXEN_TEST_HOST" in os.environ:
    TEST_HOST = os.environ["OXEN_TEST_HOST"]

# These fixtures build on each other to represent different states
# of oxen repos. For example we have:
# * empty local dir
# * empty local repo
# * empty remote repo
# * local repos with data committed
# * remote repos with data pushed
# * local repos with data committed
# * remote repos with data pushed

# shared_data_dir is a pytest fixture that points to the shared data
# that we define in tests/data and takes setup and cleaning up after


@pytest.fixture
def empty_local_dir(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "empty_repo")
    repo_name = f"test_repo_{str(uuid.uuid4())}"
    yield os.path.join(repo_dir, repo_name)


@pytest.fixture
def empty_local_repo(empty_local_dir):
    repo = Repo(empty_local_dir)
    repo.init()

    yield repo


@pytest.fixture
def celeba_local_repo_no_commits(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "CelebA")
    repo = Repo(repo_dir)
    repo.init()

    yield repo


@pytest.fixture
def question_embeddings_local_repo_no_commits(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "QuestionEmbeddings")
    repo = Repo(repo_dir)
    repo.init()

    yield repo


@pytest.fixture
def question_embeddings_local_repo_fully_committed(
    question_embeddings_local_repo_no_commits,
):
    repo = question_embeddings_local_repo_no_commits

    repo.add(os.path.join(repo.path, "chunk_embeddings.parquet"))
    repo.add(os.path.join(repo.path, "question_embeddings.parquet"))
    repo.add(os.path.join(repo.path, "smol.jsonl"))
    repo.commit("Adding question and chunk embeddings")
    yield repo


@pytest.fixture
def question_embeddings_remote_repo_fully_pushed(
    question_embeddings_local_repo_fully_committed, empty_remote_repo
):
    local_repo = question_embeddings_local_repo_fully_committed
    remote_repo = empty_remote_repo

    remote_name = "origin"
    branch_name = "main"
    local_repo.set_remote(remote_name, remote_repo.url)
    local_repo.push(remote_name, branch_name)

    yield local_repo, remote_repo


@pytest.fixture
def parquet_files_local_repo_no_commits(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "parquet")
    repo = Repo(repo_dir)
    repo.init()

    yield repo


@pytest.fixture
def parquet_files_local_repo_fully_committed(parquet_files_local_repo_no_commits):
    repo = parquet_files_local_repo_no_commits

    repo.add(repo.path)
    repo.commit("Adding parquet files")
    yield repo


@pytest.fixture
def parquet_files_remote_repo_fully_pushed(
    parquet_files_local_repo_fully_committed, empty_remote_repo
):
    local_repo = parquet_files_local_repo_fully_committed
    remote_repo = empty_remote_repo

    remote_name = "origin"
    branch_name = "main"
    local_repo.set_remote(remote_name, remote_repo.url)
    local_repo.push(remote_name, branch_name)

    yield local_repo, remote_repo


@pytest.fixture
def chat_bot_local_repo_no_commits(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "ChatBot")
    repo = Repo(repo_dir)
    repo.init()

    yield repo


@pytest.fixture
def chat_bot_local_repo_fully_committed(chat_bot_local_repo_no_commits):
    repo = chat_bot_local_repo_no_commits

    repo.add(os.path.join(repo.path, "examples.tsv"))
    repo.add(os.path.join(repo.path, "prompt.txt"))
    repo.add(os.path.join(repo.path, "formats"))
    repo.commit("Adding all data")
    yield repo


@pytest.fixture
def chat_bot_remote_repo_fully_pushed(
    chat_bot_local_repo_fully_committed, empty_remote_repo
):
    local_repo = chat_bot_local_repo_fully_committed
    remote_repo = empty_remote_repo

    remote_name = "origin"
    branch_name = "main"
    local_repo.set_remote(remote_name, remote_repo.url)
    local_repo.push(remote_name, branch_name)

    yield local_repo, remote_repo


@pytest.fixture
def house_prices_local_repo_no_commits(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "HousePrices")
    repo = Repo(repo_dir)
    repo.init()

    yield repo


@pytest.fixture
def house_prices_local_repo_fully_committed(house_prices_local_repo_no_commits):
    repo = house_prices_local_repo_no_commits

    repo.add(os.path.join(repo.path, "prices.csv"))
    repo.commit("Add prices.csv")

    yield repo


@pytest.fixture
def empty_remote_repo():
    repo_name = f"py-ox/test_repo_{str(uuid.uuid4())}"
    repo = RemoteRepo(repo_name, host=TEST_HOST, scheme=TEST_SCHEME)
    repo.create(empty=True)
    yield repo
    repo.delete()


@pytest.fixture
def celeba_local_repo_one_image_committed(celeba_local_repo_no_commits):
    repo = celeba_local_repo_no_commits

    image_file = str(PurePath("images", "1.jpg"))
    full_path = os.path.join(repo.path, image_file)
    repo.add(full_path)
    repo.commit("Adding first image")
    yield repo


@pytest.fixture
def celeba_local_repo_fully_committed(celeba_local_repo_no_commits):
    repo = celeba_local_repo_no_commits

    repo.add(os.path.join(repo.path, "images"))
    repo.add(os.path.join(repo.path, "annotations"))
    repo.commit("Adding all data")
    yield repo


@pytest.fixture
def celeba_remote_repo_one_image_pushed(
    celeba_local_repo_one_image_committed, empty_remote_repo
):
    local_repo = celeba_local_repo_one_image_committed
    remote_repo = empty_remote_repo

    remote_name = "origin"
    branch_name = "main"
    local_repo.set_remote(remote_name, remote_repo.url)
    local_repo.push(remote_name, branch_name)

    remote_repo = RemoteRepo(remote_repo.identifier, host=TEST_HOST, scheme=TEST_SCHEME)

    yield local_repo, remote_repo


@pytest.fixture
def celeba_remote_repo_fully_pushed(
    celeba_local_repo_fully_committed, empty_remote_repo
):
    local_repo = celeba_local_repo_fully_committed
    remote_repo = empty_remote_repo

    remote_name = "origin"
    branch_name = "main"
    local_repo.set_remote(remote_name, remote_repo.url)
    local_repo.push(remote_name, branch_name)

    remote_repo = RemoteRepo(remote_repo.identifier, host=TEST_HOST, scheme=TEST_SCHEME)

    yield local_repo, remote_repo
