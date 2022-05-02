"""
Client class for QCArchive/QCFractal
"""

# We are running inside QCPortal repo
# The _version file exists only in the QCPortal package
from ._version import get_versions

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions

# Add imports here
from .client import PortalClient
from .client_base import PortalRequestError
from .manager_client import ManagerClient
