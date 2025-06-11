import oxen
import os
import toml


def test_create_user(shared_datadir):
    path = os.path.join(shared_datadir, "config", "user_config.toml")
    os.makedirs(os.path.dirname(path), exist_ok=True)

    oxen.user.config_user("test user", "test_user@test.co", path)
    config = toml.load(path)
    assert config["name"] == "test user"
    assert config["email"] == "test_user@test.co"


def test_add_host(shared_datadir):
    path = os.path.join(shared_datadir, "config", "user_config.toml")
    os.makedirs(os.path.dirname(path), exist_ok=True)

    oxen.user.config_user("test user", "test_user@test.co", path)
    oxen.auth.config_auth("abcdefghijklmnop", host="test_host", path=path)
    config = toml.load(path)
    print(config)
    assert "test_host" in set([c["host"] for c in config["host_configs"]])
    assert "abcdefghijklmnop" in set([c["auth_token"] for c in config["host_configs"]])


def test_double_create_should_update(shared_datadir):
    path = os.path.join(shared_datadir, "config", "user_config.toml")
    os.makedirs(os.path.dirname(path), exist_ok=True)

    oxen.user.config_user("test user", "test_user@test.co", path)
    oxen.user.config_user("new", "new@s.co", path)
    config = toml.load(path)
    assert config["name"] == "new"
    assert config["email"] == "new@s.co"
