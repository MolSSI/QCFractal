from pydantic import BaseModel, validator
from typing import Dict, List, Tuple, Union, Any
from enum import Enum
from qcelemental.models import Molecule
from qcelemental.models.common_models import ndarray_encoder

__all__ = ["MoleculeGETBody", "MoleculeGETResponse", "MoleculePOSTBody", "MoleculePOSTResponse",
           "OptionGETBody", "OptionGETResponse", "OptionPOSTBody", "OptionPOSTResponse"]


### Generic and Common Models

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


### Molecule response


class MoleculeIndices(Enum):
    id = "id"
    molecule_hash = "molecule_hash"
    molecular_formula = "molecular_formula"


class MoleculeGETBody(BaseModel):
    class Meta(BaseModel):
        index: MoleculeIndices

    data: List[str]
    meta: Meta


class MoleculeGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: List[Molecule]

    class Config:
        json_encoders = {**ndarray_encoder}


class MoleculePOSTBody(BaseModel):
    meta: Dict = None
    data: Dict[str, Molecule]

    class Config:
        json_encoders = {**ndarray_encoder}


class MoleculePOSTResponse(BaseModel):
    meta: ResponsePOSTMeta
    data: Dict[str, str]


### Options

class OptionGETBody(BaseModel):
    meta: Dict = None
    data: Dict[str, Any]


class OptionGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: List[Dict[str, Any]]


class OptionPOSTBody(BaseModel):
    meta: Dict = None
    data: List[Dict[str, Any]]

    @validator("data", whole=True, pre=True)
    def cast_dict_to_list_of_dict(cls, v):
        if isinstance(v, dict):
            return [v]
        return v


class OptionPOSTResponse(BaseModel):
    data: List[str]
    meta: ResponsePOSTMeta
