"""
Compute worker and manager for QCArchive/QCFractal
"""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("qcfractalcompute")
except PackageNotFoundError:
    # Part of larger "qcfractal" install
    __version__ = version("qcfractal")


from .adapters import build_queue_adapter
from .managers import ComputeManager
from .qcfractal_manager_cli import _initialize_signals_process_pool
