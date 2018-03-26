"""
Main init function for dqm_server
"""

# Import modules
from . import mongo_helper
from . import database
from . import test_util
from . import constants
from . import visualization
from . import handlers
from . import compute

# Move classes up a level
from .molecule import Molecule
from .database import Database
from .client import Client
from .mongo_helper import MongoSocket

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
