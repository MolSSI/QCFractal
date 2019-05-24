"""
Main init function for qcfractal
"""

from . import interface

# Import modules
from .storage_sockets import storage_socket_factory

# Handle top level object imports
from .server import FractalServer
from .snowflake import FractalSnowflake, FractalSnowflakeHandler
from .queue import QueueManager

# Handle versioneer
from .extras import get_information
__version__ = get_information('version')
__git_revision__ = get_information('git_revision')
del get_information
