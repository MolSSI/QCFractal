from pydantic import BaseModel
from typing import Dict, List, Tuple
from enum import Enum
from qcelemental.models import Molecule
from qcelemental.models.common_models import ndarray_encoder

__all__ = ["MoleculeGETBody", "MoleculeGETResponse"]


class ResponseMeta(BaseModel):
    errors: List[Tuple[str, str]]
    n_found: int
    success: bool
    error_description: str
    missing: List[str]


## Molecule response


class MoleculeIndices(Enum):
    id = "id"
    molecule_hash = "molecule_hash"
    molecular_formula = "molecular_formula"


class MoleculeGETBody(BaseModel):
    class Meta(BaseModel):
        index: MoleculeIndices

    meta: Meta
    data: List[str]


class MoleculeGETResponse(BaseModel):
    meta: ResponseMeta
    data: List[Molecule]

    class Config:
        json_encoders = {**ndarray_encoder}
