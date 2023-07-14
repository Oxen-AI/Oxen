from .oxen import auth
from typing import Optional


def create_user_config(name: str, email: str, path: Optional[str] = None):
    if path is None:
        path = f"{auth.get_oxen_home_dir()}/user_config.toml"
    if not path.endswith(".toml"):
        raise ValueError("Path must end with .toml")
    auth.create_user_config(name, email, path)


def add_host_auth(host: str, token: str, path: Optional[str] = None):
    if path is None:
        path = f"{auth.get_oxen_home_dir()}/user_config.toml"
    if not path.endswith(".toml"):
        raise ValueError("Path must end with .toml")
    auth.add_host_auth(host, token, path)
