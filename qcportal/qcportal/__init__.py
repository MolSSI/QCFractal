"""
Client for QCArchive/QCFractal
"""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("qcportal")
except PackageNotFoundError:
    # Part of larger "qcfractal" install
    __version__ = version("qcfractal")

# Add imports here
from .client import PortalClient
from .client_base import PortalRequestError
from .manager_client import ManagerClient
