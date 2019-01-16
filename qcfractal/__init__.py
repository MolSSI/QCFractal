"""
Main init function for qcfractal
"""

from . import interface
from . import testing
# Handle versioneer
from ._version import get_versions
from .server import FractalServer
# Import modules
from .storage_sockets import storage_socket_factory

versions = get_versions()
__version__ = versions['version']
__git_revision__ = versions['full-revisionid']
del get_versions, versions
