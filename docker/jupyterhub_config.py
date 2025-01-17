"""Jupyterhub Config file for hosting QCPortal.

This authenticates using QCArchive server usernames and passwords.
"""

import os

c = get_config()  # noqa

# Get the base url from the environment
base_url = os.getenv("QCFRACTAL_JHUB_BASE_URL", "/")
c.JupyterHub.base_url = base_url

from qcarchive_authenticator import QCArchiveAuthenticator

c.JupyterHub.authenticator_class = QCArchiveAuthenticator

# don't cache static files
c.JupyterHub.tornado_settings = {
    "no_cache_static": True,
    "slow_spawn_timeout": 0,
}

c.JupyterHub.allow_named_servers = True

c.Authenticator.allow_all = True
