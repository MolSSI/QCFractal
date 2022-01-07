"""
DQM Client base folder
"""

# We are running inside QCPortal repo
try:
    # The _version file exists only in the QCPortal package
    from ._version import get_versions

    versions = get_versions()
    __version__ = versions["version"]
    __git_revision__ = versions["full-revisionid"]
    del get_versions, versions

# We are running inside QCFractal
except ImportError:
    from .. import __version__, __git_revision__

# Add imports here
from .client import PortalClient, PortalRequestError
from .manager_client import ManagerClient

# from . import collections
