"""
Compute worker and manager for QCArchive/QCFractal
"""

from importlib.metadata import version

__version__ = version("qcfractalcompute")

from .adapters import build_queue_adapter
from .managers import ComputeManager
from .qcfractal_manager_cli import _initialize_signals_process_pool
