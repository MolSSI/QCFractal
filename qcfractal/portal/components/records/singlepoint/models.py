from pydantic import BaseModel, Field, constr, validator
from typing import Optional, Dict, Union, Any, List
from qcfractal.portal.components.keywords import KeywordSet
from qcelemental.models.results import AtomicResultProtocols, AtomicResult
from qcelemental.models import Molecule
from enum import Enum


class SinglePointDriver(str, Enum):
    # Copied from qcelemental to add "deferred"
    energy = "energy"
    gradient = "gradient"
    hessian = "hessian"
    properties = "properties"
    deferred = "deferred"


class SinglePointSpecification(BaseModel):
    program: constr(to_lower=True) = Field(
        ...,
        description="The quantum chemistry program to evaluate the computation with. Not all quantum chemistry programs"
        " support all combinations of driver/method/basis.",
    )
    driver: SinglePointDriver = Field(...)
    method: constr(to_lower=True) = Field(
        ..., description="The quantum chemistry method to evaluate (e.g., B3LYP, PBE, ...)."
    )
    basis: Optional[constr(to_lower=True)] = Field(
        ...,
        description="The quantum chemistry basis set to evaluate (e.g., 6-31g, cc-pVDZ, ...). Can be ``None`` for "
        "methods without basis sets.",
    )
    keywords: Union[int, KeywordSet] = Field(
        KeywordSet(values={}),
        description="Keywords to use. Can be an ID of the keywords on the server or a KeywordSet object",
    )
    protocols: AtomicResultProtocols = Field(
        AtomicResultProtocols(), description=str(AtomicResultProtocols.__base_doc__)
    )

    @validator("basis", pre=True)
    def _convert_basis(cls, v):
        # Convert empty string to None
        # Lowercasing is handled by constr
        return None if v == "" else v


class SinglePointInput(BaseModel):
    specification: SinglePointSpecification
    molecules: List[Union[int, Molecule]]
