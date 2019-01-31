"""
Common models for QCPortal/Fractal
"""

import numpy as np
from pydantic import BaseModel
from typing import Any, Dict, List, Optional

from qcelemental.models import Molecule, Provenance

__all__ = ["QCMeta", "json_encoder"]

# Add in QCElemental models
__all__.extend(["Molecule", "Provenance"])


json_encoders = {np.ndarray: lambda v: v.flatten().tolist()}

class QCMeta(BaseModel):
    """
    The basic quantum chemistry meta specification
    """
    driver: str
    method: str
    basis: Optional[str] = None
    options: Optional[str] = None
    program: str