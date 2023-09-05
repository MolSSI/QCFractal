"""
Compute worker and manager for QCArchive/QCFractal
"""

from importlib.metadata import version

__version__ = version("qcfractalcompute")

from .compute_manager import ComputeManager
from .generic_wrapper import wrap_generic_function
