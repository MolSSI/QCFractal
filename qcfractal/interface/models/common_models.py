"""
Common models for QCPortal/Fractal
"""
import hashlib
import json
import numpy as np
from pydantic import BaseModel
from typing import Optional

from qcelemental.models import Molecule, Provenance

__all__ = ["QCMeta", "json_encoders", "hash_dictionary"]

# Add in QCElemental models
__all__.extend(["Molecule", "Provenance"])

json_encoders = {np.ndarray: lambda v: v.flatten().tolist()}


def hash_dictionary(data):
    m = hashlib.sha1()
    m.update(json.dumps(data, sort_keys=True).encode("UTF-8"))
    return m.hexdigest()


class QCMeta(BaseModel):
    """
    The basic quantum chemistry meta specification
    """
    driver: str
    method: str
    basis: Optional[str] = None
    options: Optional[str] = None
    program: str
