###################################################
# The version stuff must be handled first.
# Other packages that we import later will need it
###################################################

from importlib.metadata import version

__version__ = version("qcfractal")

# Handle top level object imports
from .postgres_harness import PostgresHarness, TemporaryPostgres

# from .server import FractalServer
from .snowflake import FractalSnowflake

# The location of this file
import os

qcfractal_topdir = os.path.abspath(os.path.dirname(__file__))
