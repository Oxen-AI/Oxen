from .oxen import auth, util
from oxen.user import config_user
from typing import Optional
import os
import requests


def config_auth(token: str, host: str = "hub.oxen.ai", path: Optional[str] = None):
    """
    Configures authentication for a host.

    Args:
        token: `str`
            The token to use for authentication.
        host: `str`
            The host to configure authentication for. Default: 'hub.oxen.ai'
        path: `Optional[str]`
            The path to save the authentication config to.
            Defaults to $HOME/.config/oxen/auth_config.toml
    """
    if path is None:
        path = os.path.join(util.get_oxen_config_dir(), "auth_config.toml")
    if not path.endswith(".toml"):
        raise ValueError("Path must end with .toml")
    auth.config_auth(host, token, path)

    # Only fetch user if the host is the hub
    if "hub.oxen.ai" == host:
        # Fetch the user from the hub and save it to the config
        url = f"https://{host}/api/authorize"
        # make request with token
        headers = {"Authorization": f"Bearer {token}"}
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            raise Exception(f"Failed to fetch user from {host}.")
        user = r.json()["user"]
        name = user["name"]
        email = user["email"]
        # save user to config
        config_user(name, email)
