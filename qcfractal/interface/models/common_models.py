"""
Common models for QCPortal/Fractal
"""
import hashlib
import json
import numpy as np
from pydantic import BaseModel, validator
from typing import Any, Dict, Optional

from qcelemental.models import Molecule, Provenance

__all__ = ["QCMeta", "json_encoders", "hash_dictionary", "Option"]

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


class Option(BaseModel):
    """
    An options object for the QCArchive ecosystem
    """
    program: str
    hash_index: str
    options: Dict[str, Any]

    class Config:
        allow_extra = True
        allow_mutation = False

    def __init__(self, **data):

        build_index = False
        if ("hash_index" not in data) or data.pop("build_index", False):
            build_index = True
            data["hash_index"] = "placeholder"

        BaseModel.__init__(self, **data)

        if build_index:
            self.__values__["hash_index"] = self.get_hash_index()

    @validator('program')
    def check_program(cls, v):
        return v.lower()

    @validator('options', whole=True)
    def check_options(cls, v):
        return {k.lower(): v for k, v in v.items()}

    def get_hash_index(self):
        packet = self.options.copy()
        packet["program"] = self.program
        return hash_dictionary(packet)