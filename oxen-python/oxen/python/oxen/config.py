from .oxen import util
import os


def is_configured():
    """
    Checks if the user and auth is configured.

    Returns:
        `bool`: True if the user and auth is configured, False otherwise.
    """

    auth_path = os.path.join(util.get_oxen_config_dir(), "auth_config.toml")
    user_path = os.path.join(util.get_oxen_config_dir(), "user_config.toml")

    return os.path.exists(auth_path) and os.path.exists(user_path)
