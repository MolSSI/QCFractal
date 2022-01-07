"""
Common models for QCPortal/Fractal
"""

from __future__ import annotations

import re
import json

# For compression
import lzma
import bz2
import gzip

from enum import Enum

from pydantic import Field, validator
from qcelemental.models import (
    AutodocBaseSettings,
    Molecule,
    ProtoModel,
    Provenance,
    ComputeError,
    FailedOperation,
    AtomicInput,
    AtomicResult,
    OptimizationInput,
    OptimizationResult,
)

from qcelemental.models.molecule import Identifiers as MoleculeIdentifiers

from qcelemental.models.procedures import OptimizationProtocols
from qcelemental.models.results import AtomicResultProtocols
from qcportal.utils import recursive_normalizer

from typing import Any, Dict, Optional, Union

AllResultTypes = Union[FailedOperation, AtomicResult, OptimizationResult]
AllInputTypes = Union[AtomicInput, OptimizationInput]


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
    protocols: Optional[AtomicResultProtocols] = Field(
        AtomicResultProtocols(), description=str(AtomicResultProtocols.__base_doc__)
    )
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
    protocols: Optional[OptimizationProtocols] = Field(
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


class Citation(ProtoModel):
    """A literature citation."""

    acs_citation: Optional[
        str
    ] = None  # hand-formatted citation in ACS style. In the future, this could be bibtex, rendered to different formats.
    bibtex: Optional[str] = None  # bibtex blob for later use with bibtex-renderer
    doi: Optional[str] = None
    url: Optional[str] = None

    def to_acs(self) -> str:
        """Returns an ACS-formatted citation"""
        return self.acs_citation
