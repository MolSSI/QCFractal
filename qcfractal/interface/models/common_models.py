"""
Common models for QCPortal/Fractal
"""
import json
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, validator, Schema
from qcelemental.models import Molecule, Provenance

from .model_utils import hash_dictionary, prepare_basis, recursive_normalizer

__all__ = ["QCSpecification", "OptimizationSpecification", "KeywordSet", "ObjectId"]

# Add in QCElemental models
__all__.extend(["Molecule", "Provenance"])


class ObjectId(str):
    """
    The Id of the object in the data.
    """
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
    The quantum chemistry metadata specification for individual computations such as energy, gradient, and Hessians.
    """
    driver: DriverEnum = Schema(
        ...,
        description="The type of calculation that is being performed."
    )
    method: str = Schema(
        ...,
        description="The quantum chemistry method to evaluate (e.g., B3LYP, PBE, ...)."
    )
    basis: Optional[str] = Schema(
        None,
        description="The quantum chemistry basis set to evaluate (e.g., 6-31g, cc-pVDZ, ...). Can be ``None`` for "
                    "methods without basis sets."
    )
    keywords: Optional[ObjectId] = Schema(
        None,
        description="The ID of the :class:`KeywordSet` registered in the database to run this calculation with. This "
                    "ID must exist in the database."
    )
    program: str = Schema(
        ...,
        description="The quantum chemistry program to evaluate the computation with. Not all quantum chemistry programs"
                    " support all combinations of driver/method/basis."
    )

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
    Metadata describing a geometry optimization.
    """
    program: str = Schema(
        ...,
        description="Optimization program to run the optimization with"
    )
    keywords: Optional[Dict[str, Any]] = Schema(
        None,
        description="Dictionary of keyword arguments to pass into the ``program`` when the program runs. "
                    "Note that unlike :class:`QCSpecification` this is a dictionary of keywords, not the ID for a "
                    ":class:`KeywordSet`. "
    )

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
    A key:value storage object for Keywords.
    """
    id: Optional[ObjectId] = Schema(
        None,
        description="The ID of this object, will be automatically asigned when added to the database."
    )
    hash_index: str = Schema(
        ...,
        description="The hash of this keyword set to store and check for collisions. This string is automatically "
                    "computed."
    )
    values: Dict[str, Any] = Schema(
        ...,
        description="The key-value pairs which make up this KeywordSet. There is no direct relation between this "
                    "dictionary and applicable program/spec it can be used on."
    )
    lowercase: bool = Schema(
        True,
        description="If ``True``, normalizes the string keys of the ``values`` to all lowercase. Assists in matching "
                    "against other :class:`KeywordSet` objects in the database."
    )
    exact_floats: bool = Schema(
        False,
        description="If ``False``, rounds all floating point numbers to 1.e-10. Assists in matching against other "
                    ":class:`KeywordSet` objects in the database."
    )
    comments: Optional[str] = Schema(
        None,
        description="Additional comments for this KeywordSet. Intended for pure human/user consumption "
                    "and clarity."
    )

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
