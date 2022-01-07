"""
Initializer for the queue_handler folder
"""

# We are running inside QCFractalCompute repo
try:
    # The _version file exists only in the QCFractalCompute package
    from ._version import get_versions

    versions = get_versions()
    __version__ = versions["version"]
    __git_revision__ = versions["full-revisionid"]
    del get_versions, versions

# We are running inside QCFractal
except ImportError:
    from .. import __version__, __git_revision__

from .adapters import build_queue_adapter
from .managers import QueueManager
