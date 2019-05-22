"""
DQM Client base folder
"""

from . import collections
from . import data
from . import dict_utils
from . import models

# Add imports here
from .client import FractalClient
from .models import Molecule

# We are running inside QCPortal repo
try:
    from . import _version
    versions = _version.get_versions()
    __version__ = versions["version"]
    __git_revision__ = versions["full-revisionid"]
    _isportal = True

# We are running inside QCFractal
except ImportError:
    from ..extras import get_information
    __version__ = "inplace-{}".format(get_information('version'))
    __git_revision__ = get_information('git_revision')
    _isportal = False
    del get_information
