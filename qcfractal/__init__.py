"""
Main init function for qcfractal
"""

###################################################
# The version stuff must be handled first.
# Other packages that we import later will need it
###################################################

# Handle versioneer
from ._version import get_versions

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions

# from . import interface
from . import portal

# Handle top level object imports
from .postgres_harness import PostgresHarness, TemporaryPostgres
from .qc_queue import QueueManager

# from .server import FractalServer
from .snowflake import FractalSnowflake
