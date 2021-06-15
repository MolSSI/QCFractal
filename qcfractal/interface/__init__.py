"""
DQM Client base folder
"""

###################################################
# The version stuff must be handled first.
# Other packages that we import later will need it
###################################################

# We are running inside QCPortal repo
try:
    # The _version file exists only in the QCPortal package
    from ._version import get_versions

    versions = get_versions()
    __version__ = versions["version"]
    __git_revision__ = versions["full-revisionid"]
    del get_versions, versions

# We are running inside QCFractal
except ImportError:
    from .. import __version__, __git_revision__

from . import collections, data, models, util

# Add imports here
from .client import FractalClient
from .models import Molecule
