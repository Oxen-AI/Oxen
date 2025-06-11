from .oxen import user, util
from typing import Optional
import os


def config_user(name: str, email: str, path: Optional[str] = None):
    """
    Configures user for a host.

    Args:
        name: `str`
            The name to use for user.
        email: `str`
            The email to use for user.
        path: `Optional[str]`
            The path to save the user config to.
            Defaults to $HOME/.config/oxen/user_config.toml
    """
    if path is None:
        path = os.path.join(util.get_oxen_config_dir(), "user_config.toml")

    if not path.endswith(".toml"):
        raise ValueError(f"Path {path} must end with .toml")
    return user.config_user(name, email, path)


def current_user(path: Optional[str] = None):
    """
    Gets the current user.

    Args:
        path: `Optional[str]`
            The path to load the user config from.
            Defaults to $HOME/.config/oxen/user_config.toml
    """
    if path is None:
        path = os.path.join(util.get_oxen_config_dir(), "user_config.toml")
    if not path.endswith(".toml"):
        raise ValueError(f"Path {path} must end with .toml")
    return user.current_user(path)
