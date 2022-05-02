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

# Handle top level object imports
from .postgres_harness import PostgresHarness, TemporaryPostgres

# from .server import FractalServer
from .snowflake import FractalSnowflake

# The location of this file
import os

qcfractal_topdir = os.path.abspath(os.path.dirname(__file__))
