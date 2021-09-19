"""
DQM Client base folder
"""

###################################################
# The version stuff must be handled first.
# Other packages that we import later will need it
###################################################

# Always use the _version module in this directory. It should
# always match the one a directory above, so no need to differentiate.
from ._version import get_versions

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions


from . import collections, data, models, util

# Add imports here
from .client import FractalClient
from .models import Molecule
