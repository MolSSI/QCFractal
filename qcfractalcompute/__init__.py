"""
Initializer for the queue_handler folder
"""

# We are running inside QCFractalCompute repo
# The _version file exists only in the QCFractalCompute package
from ._version import get_versions

versions = get_versions()
__version__ = versions["version"]
__git_revision__ = versions["full-revisionid"]
del get_versions, versions

from .adapters import build_queue_adapter
from .managers import ComputeManager
from .qcfractal_manager_cli import _initialize_signals_process_pool
