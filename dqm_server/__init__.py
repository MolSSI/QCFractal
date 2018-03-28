"""
Main init function for dqm_server
"""

# Import modules
from .dbsockets import db_socket_factory 
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

# Handle versioneer
from ._version import get_versions
versions = get_versions()
__version__ = versions['version']
__git_revision__ = versions['full-revisionid']
del get_versions, versions
