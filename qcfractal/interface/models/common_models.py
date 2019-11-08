"""
Common models for QCPortal/Fractal
"""
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import Field, validator

from qcelemental.models import AutodocBaseSettings, Molecule, ProtoModel, Provenance
from qcelemental.models.procedures import OptimizationProtocols
from qcelemental.models.results import ResultProtocols

from .model_utils import hash_dictionary, prepare_basis, recursive_normalizer

__all__ = ["QCSpecification", "OptimizationSpecification", "KeywordSet", "ObjectId", "DriverEnum", "Citation"]

# Add in QCElemental models
__all__.extend(["Molecule", "Provenance", "ProtoModel"])

# Autodoc
__all__.extend(["OptimizationProtocols", "ResultProtocols"])


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
        if isinstance(v, str) and (len(v) == 24) and (set(v) <= cls._valid_hex):
            return v
        elif isinstance(v, int):
            return str(v)
        elif isinstance(v, str) and v.isdigit():
            return v
        else:
            raise TypeError("The string {} is not a valid 24-character hexadecimal or integer ObjectId!".format(v))


class DriverEnum(str, Enum):
    """
    The type of calculation that is being performed (e.g., energy, gradient, Hessian, ...).
    """

    energy = "energy"
    gradient = "gradient"
    hessian = "hessian"
    properties = "properties"


class QCSpecification(ProtoModel):
    """
    The quantum chemistry metadata specification for individual computations such as energy, gradient, and Hessians.
    """

    driver: DriverEnum = Field(..., description=str(DriverEnum.__doc__))
    method: str = Field(..., description="The quantum chemistry method to evaluate (e.g., B3LYP, PBE, ...).")
    basis: Optional[str] = Field(
        None,
        description="The quantum chemistry basis set to evaluate (e.g., 6-31g, cc-pVDZ, ...). Can be ``None`` for "
        "methods without basis sets.",
    )
    keywords: Optional[ObjectId] = Field(
        None,
        description="The Id of the :class:`KeywordSet` registered in the database to run this calculation with. This "
        "Id must exist in the database.",
    )
    protocols: ResultProtocols = Field(ResultProtocols(), description=str(ResultProtocols.__base_doc__))
    program: str = Field(
        ...,
        description="The quantum chemistry program to evaluate the computation with. Not all quantum chemistry programs"
        " support all combinations of driver/method/basis.",
    )

    def dict(self, *args, **kwargs):
        ret = super().dict(*args, **kwargs)

        # Maintain hash compatability
        if len(ret["protocols"]) == 0:
            ret.pop("protocols", None)

        return ret

    @validator("basis")
    def _check_basis(cls, v):
        return prepare_basis(v)

    @validator("program")
    def _check_program(cls, v):
        return v.lower()

    @validator("method")
    def _check_method(cls, v):
        return v.lower()

    def form_schema_object(self, keywords: Optional["KeywordSet"] = None, checks=True) -> Dict[str, Any]:
        if checks and self.keywords:
            assert keywords.id == self.keywords

        ret = {
            "driver": str(self.driver.name),
            "program": self.program,
            "model": {"method": self.method},
        }  # yapf: disable
        if self.basis:
            ret["model"]["basis"] = self.basis

        if keywords:
            ret["keywords"] = keywords.values
        else:
            ret["keywords"] = {}

        return ret


class OptimizationSpecification(ProtoModel):
    """
    Metadata describing a geometry optimization.
    """

    program: str = Field(..., description="Optimization program to run the optimization with")
    keywords: Optional[Dict[str, Any]] = Field(
        None,
        description="Dictionary of keyword arguments to pass into the ``program`` when the program runs. "
        "Note that unlike :class:`QCSpecification` this is a dictionary of keywords, not the Id for a "
        ":class:`KeywordSet`. ",
    )
    protocols: OptimizationProtocols = Field(
        OptimizationProtocols(), description=str(OptimizationProtocols.__base_doc__)
    )

    def dict(self, *args, **kwargs):
        ret = super().dict(*args, **kwargs)

        # Maintain hash compatability
        if len(ret["protocols"]) == 0:
            ret.pop("protocols", None)

        return ret

    @validator("program")
    def _check_program(cls, v):
        return v.lower()

    @validator("keywords")
    def _check_keywords(cls, v):
        if v is not None:
            v = recursive_normalizer(v)
        return v


class KeywordSet(ProtoModel):
    """
    A key:value storage object for Keywords.
    """

    id: Optional[ObjectId] = Field(
        None, description="The Id of this object, will be automatically assigned when added to the database."
    )
    hash_index: str = Field(
        ...,
        description="The hash of this keyword set to store and check for collisions. This string is automatically "
        "computed.",
    )
    values: Dict[str, Optional[Any]] = Field(
        ...,
        description="The key-value pairs which make up this KeywordSet. There is no direct relation between this "
        "dictionary and applicable program/spec it can be used on.",
    )
    lowercase: bool = Field(
        True,
        description="String keys are in the ``values`` dict are normalized to lowercase if this is True. Assists in "
        "matching against other :class:`KeywordSet` objects in the database.",
    )
    exact_floats: bool = Field(
        False,
        description="All floating point numbers are rounded to 1.e-10 if this is False."
        "Assists in matching against other :class:`KeywordSet` objects in the database.",
    )
    comments: Optional[str] = Field(
        None,
        description="Additional comments for this KeywordSet. Intended for pure human/user consumption " "and clarity.",
    )

    def __init__(self, **data):

        build_index = False
        if ("hash_index" not in data) or data.pop("build_index", False):
            build_index = True
            data["hash_index"] = "placeholder"

        ProtoModel.__init__(self, **data)

        # Overwrite options with massaged values
        kwargs = {"lowercase": self.lowercase}
        if self.exact_floats:
            kwargs["digits"] = False

        self.__dict__["values"] = recursive_normalizer(self.values, **kwargs)

        # Build a hash index if we need it
        if build_index:
            self.__dict__["hash_index"] = self.get_hash_index()

    def get_hash_index(self):
        return hash_dictionary(self.values.copy())


class Citation(ProtoModel):
    """ A literature citation.  """

    acs_citation: Optional[
        str
    ] = None  # hand-formatted citation in ACS style. In the future, this could be bibtex, rendered to different formats.
    bibtex: Optional[str] = None  # bibtex blob for later use with bibtex-renderer
    doi: Optional[str] = None
    url: Optional[str] = None

    def to_acs(self) -> str:
        """ Returns an ACS-formatted citation """
        return self.acs_citation
