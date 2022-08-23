"""
Client for QCArchive/QCFractal
"""

from importlib.metadata import version

__version__ = version("qcportal")

# Add imports here
from .client import PortalClient
from .client_base import PortalRequestError
from .manager_client import ManagerClient
