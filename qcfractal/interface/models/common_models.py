"""
Common models for QCPortal/Fractal
"""
import json
from enum import Enum
from typing import Any, Dict, Optional, Union, List, Tuple

from pydantic import BaseModel, validator
from qcelemental.models import Molecule, Provenance

from .model_utils import hash_dictionary, prepare_basis, recursive_normalizer

__all__ = ["QCSpecification", "OptimizationSpecification", "KeywordSet", "ObjectId"]

# Add in QCElemental models
__all__.extend(["Molecule", "Provenance"])


class ObjectId(str):
    _valid_hex = set("0123456789abcdef")

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if (isinstance(v, str) and (len(v) == 24) and (set(v) <= cls._valid_hex)):
            return v
        elif isinstance(v, int):
            return str(v)
        elif isinstance(v, str) and v.isdigit():
            return (v)
        else:
            raise TypeError("The string {} is not a valid 24-character hexadecimal or integer ObjectId!".format(v))


class DriverEnum(str, Enum):
    """The possible driver configurations of a single quantum chemistry
    computation.
    """
    energy = 'energy'
    gradient = 'gradient'
    hessian = 'hessian'
    properties = 'properties'


class QCSpecification(BaseModel):
    """
    The basic quantum chemistry meta specification
    """
    driver: DriverEnum
    method: str
    basis: Optional[str] = None
    keywords: Optional[ObjectId] = None
    program: str

    @validator('basis')
    def check_basis(cls, v):
        return prepare_basis(v)

    @validator('program')
    def check_program(cls, v):
        return v.lower()

    @validator('method')
    def check_method(cls, v):
        return v.lower()

    class Config:
        extra = "forbid"
        allow_mutation = False

    def form_schema_object(self, keywords: Optional['KeywordSet'] = None, checks=True) -> Dict[str, Any]:
        if checks and self.keywords:
            assert keywords.id == self.keywords

        ret = {
            "driver": str(self.driver.name),
            "program": self.program,
            "model": {
                "method": self.method
            }
        } # yapf: disable
        if self.basis:
            ret["model"]["basis"] = self.basis

        if keywords:
            ret["keywords"] = keywords.values
        else:
            ret["keywords"] = {}

        return ret


class OptimizationSpecification(BaseModel):
    """
    GridOptimizationRecord options
    """
    program: str
    keywords: Optional[Dict[str, Any]] = None

    @validator('program')
    def check_program(cls, v):
        return v.lower()

    @validator('keywords')
    def check_keywords(cls, v):
        if v is not None:
            v = recursive_normalizer(v)
        return v

    class Config:
        extra = "forbid"
        allow_mutation = False


class KeywordSet(BaseModel):
    """
    An options object for the QCArchive ecosystem
    """
    id: Optional[ObjectId] = None
    hash_index: str
    values: Dict[str, Any]
    lowercase: bool = True
    exact_floats: bool = False
    comments: Optional[str] = None

    class Config:
        extra = "forbid"
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

        self.__values__["values"] = recursive_normalizer(self.values, **kwargs)

        # Build a hash index if we need it
        if build_index:
            self.__values__["hash_index"] = self.get_hash_index()

    def get_hash_index(self):
        return hash_dictionary(self.values.copy())

    def json_dict(self, *args, **kwargs):
        return json.loads(self.json(*args, **kwargs))
