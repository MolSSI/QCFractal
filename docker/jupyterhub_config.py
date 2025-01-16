"""Jupyterhub Config file for hosting QCPortal.

This authenticates using QCArchive server usernames and passwords.
"""

c = get_config()  # noqa

from qcarchive_authenticator import QCArchiveAuthenticator

c.JupyterHub.authenticator_class = QCArchiveAuthenticator

# don't cache static files
c.JupyterHub.tornado_settings = {
    "no_cache_static": True,
    "slow_spawn_timeout": 0,
}

c.JupyterHub.allow_named_servers = True
c.JupyterHub.default_url = "/hub/home"

c.Authenticator.allow_all = True