"""sample jupyterhub config file for testing

configures jupyterhub with dummyauthenticator and simplespawner
to enable testing without administrative privileges.
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

# make sure admin UI is available and any user can login
#c.Authenticator.admin_users = {"admin"}
c.Authenticator.allow_all = True