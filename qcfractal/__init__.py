"""
Main init function for qcfractal
"""

#from . import interface
from . import portal

# Handle versioneer
from .extras import get_information

# Handle top level object imports
from .postgres_harness import PostgresHarness, TemporaryPostgres
from .qc_queue import QueueManager

# from .server import FractalServer
from .snowflake import FractalSnowflake

# Import modules
from .storage_sockets import storage_socket_factory

__version__ = get_information("version")
__git_revision__ = get_information("git_revision")
del get_information
