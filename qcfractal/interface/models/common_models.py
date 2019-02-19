"""
Common models for QCPortal/Fractal
"""
import hashlib
import json
from typing import Any, Dict, Optional

import numpy as np
from pydantic import BaseModel, validator
from qcelemental.models import Molecule, Provenance, Result, ResultInput

__all__ = ["QCSpecification", "OptimizationSpecification", "json_encoders", "hash_dictionary", "KeywordSet"]

# Add in QCElemental models
__all__.extend(["Molecule", "Provenance"])

json_encoders = {np.ndarray: lambda v: v.flatten().tolist()}


def recursive_hash_prep(value: Any, **kwargs: Dict[str, Any]) -> Any:
    """
    Prepare a structure for hashing by lowercasing all values and round all floats
    """
    digits = kwargs.get("digits", 10)
    lowercase = kwargs.get("lowercase", True)

    if isinstance(value, (int, type(None))):
        pass

    elif isinstance(value, str):
        if lowercase:
            value = value.lower()

    elif isinstance(value, (list, tuple)):
        value = [recursive_hash_prep(x, **kwargs) for x in value]

    elif isinstance(value, dict):
        ret = {}
        for k, v in value.items():
            if lowercase:
                k = k.lower()
            ret[k] = recursive_hash_prep(v, **kwargs)
        value = ret

    elif isinstance(value, float):
        if digits:
            value = round(value, digits)
            if value == 0.0:  # Values rounded to zero
                value = 0

    else:
        raise TypeError("Invalid type in KeywordSet ({type(value)}), only simply Python types are allowed.")

    return value


def hash_dictionary(data: Dict[str, Any]) -> str:
    m = hashlib.sha1()
    m.update(json.dumps(data, sort_keys=True).encode("UTF-8"))
    return m.hexdigest()


class QCSpecification(BaseModel):
    """
    The basic quantum chemistry meta specification
    """
    driver: str
    method: str
    basis: Optional[str] = None
    keywords: Optional[str] = None
    program: str

    class Config:
        extra = "forbid"
        allow_mutation = False


class OptimizationSpecification(BaseModel):
    """
    GridOptimization options
    """
    program: str
    keywords: Optional[Dict[str, Any]] = None

    class Config:
        extra = "forbid"
        allow_mutation = False


class KeywordSet(BaseModel):
    """
    An options object for the QCArchive ecosystem
    """
    id: Optional[str] = None
    program: str
    hash_index: str
    values: Dict[str, Any]
    lowercase: bool = True
    exact_floats: bool = False

    class Config:
        extra = "allow"
        allow_mutation = False

    def __init__(self, **data):

        build_index = False
        if ("hash_index" not in data) or data.pop("build_index", False):
            build_index = True
            data["hash_index"] = "placeholder"

        BaseModel.__init__(self, **data)

        # Overwrite options with massaged values
        kwargs = {"lowercase": self.lowercase}
        if self.exact_floats:
            kwargs["digits"] = False

        self.__values__["values"] = recursive_hash_prep(self.values, **kwargs)

        # Build a hash index if we need it
        if build_index:
            self.__values__["hash_index"] = self.get_hash_index()

    @validator('program')
    def check_program(cls, v):
        return v.lower()

    def get_hash_index(self):
        packet = self.values.copy()
        packet["program"] = self.program
        return hash_dictionary(packet)

    def json_dict(self, *args, **kwargs):
        return json.loads(self.json(*args, **kwargs))
