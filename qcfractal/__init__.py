"""
Main init function for qcfractal
"""

from . import interface
from . import testing

# Import modules
from .storage_sockets import storage_socket_factory

# Handle top level object imports
from .server import FractalServer
from .queue import QueueManager

# Handle versioneer
from ._version import get_versions
versions = get_versions()
__version__ = versions['version']
__git_revision__ = versions['full-revisionid']
del get_versions, versions
