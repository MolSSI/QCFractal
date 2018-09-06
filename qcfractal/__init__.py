"""
Main init function for qcfractal
"""

from . import interface
from . import testing
# Handle versioneer
from ._version import get_versions
# Import modules
from .storage_sockets import storage_socket_factory
from .server import FractalServer

# from . import mongo_helper
# from . import database
# from . import test_util
# from . import constants
# from . import visualization
# from . import handlers
# from . import compute
#
# # Move classes up a level
# from .molecule import Molecule
# from .database import Database
# from .client import Client
# from .mongo_helper import MongoSocket
versions = get_versions()
__version__ = versions['version']
__git_revision__ = versions['full-revisionid']
del get_versions, versions
