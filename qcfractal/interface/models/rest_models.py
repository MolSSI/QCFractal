from pydantic import BaseModel, validator
from typing import Dict, List, Tuple, Union
from enum import Enum
from qcelemental.models import Molecule
from qcelemental.models.common_models import ndarray_encoder

__all__ = ["MoleculeGETBody", "MoleculeGETResponse", "MoleculePOSTBody", "MoleculePOSTResponse"]


class ResponseMeta(BaseModel):
    errors: List[Tuple[str, str]]
    success: bool
    error_description: Union[str, bool]


class ResponseGETMeta(ResponseMeta):
    missing: List[str]
    n_found: int


class ResponsePOSTMeta(ResponseMeta):
    n_inserted: int
    duplicates: List[str]
    validation_errors: List[str]


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
    meta: ResponseGETMeta
    data: List[Molecule]

    class Config:
        json_encoders = {**ndarray_encoder}


class MoleculePOSTBody(BaseModel):

    meta: dict = None
    data: Dict[str, Molecule]

    @validator('meta')
    def meta_must_be_empty(cls, v):
        """Ensure meta for molecule POST is empty (might be overkill, we could just ignore)"""
        if v:
            raise ValueError("No molecule POST meta options are available at this time")

    class Config:
        json_encoders = {**ndarray_encoder}


class MoleculePOSTResponse(BaseModel):
    meta: ResponsePOSTMeta
    data: Dict[str, str]
