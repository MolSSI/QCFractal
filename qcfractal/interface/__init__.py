"""
DQM Client base folder
"""

from . import collections, data, models, util

# Add imports here
from .client import FractalClient
from .models import Molecule

# We are running inside QCPortal repo
try:
    # The _version file exists only in the QCPortal package
    from . import _version  # lgtm [py/import-own-module]

    versions = _version.get_versions()
    __version__ = versions["version"]
    __git_revision__ = versions["full-revisionid"]
    _isportal = True

# We are running inside QCFractal
except ImportError:
    from ..extras import get_information

    __version__ = "inplace-{}".format(get_information("version"))
    __git_revision__ = get_information("git_revision")
    _isportal = False
    del get_information
