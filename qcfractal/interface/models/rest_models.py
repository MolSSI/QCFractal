"""
Models for the REST interface
"""
from pydantic import BaseModel, validator
from typing import Dict, List, Tuple, Union, Any
from enum import Enum

from .common_models import Molecule, json_encoders

__all__ = [
    "MoleculeGETBody", "MoleculeGETResponse", "MoleculePOSTBody", "MoleculePOSTResponse",
    "OptionGETBody", "OptionGETResponse", "OptionPOSTBody", "OptionPOSTResponse",
    "CollectionGETBody", "CollectionGETResponse", "CollectionPOSTBody", "CollectionPOSTResponse",
    "ResultGETBody", "ResultGETResponse", "ResultPOSTBody", "ResultPOSTResponse",
    "ProcedureGETBody", "ProcedureGETReponse"
] # yapf: disable


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
        json_encoders = json_encoders


class MoleculePOSTBody(BaseModel):
    meta: Dict = None
    data: Dict[str, Molecule]

    class Config:
        json_encoders = json_encoders


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


### Collections


class CollectionGETBody(BaseModel):
    class Data(BaseModel):
        collection: str = None
        name: str = None

        @validator("collection")
        def cast_to_lower(cls, v):
            return v.lower()

    meta: Dict = None
    data: Data


class CollectionGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: List[Dict[str, Any]]

    @validator("data", whole=True)
    def ensure_collection_name_in_data_get_res(cls, v):
        for col in v:
            if "name" not in col or "collection" not in col:
                raise ValueError("Dicts in 'data' must have both 'collection' and 'name'")
        return v


class CollectionPOSTBody(BaseModel):
    class Meta(BaseModel):
        overwrite: bool = False

    class Data(BaseModel):
        id: str = "local"  # Auto blocks overwriting in mongoengine_socket
        collection: str
        name: str

        @validator("collection")
        def cast_to_lower(cls, v):
            return v.lower()

        class Config:
            # Maps effectively Dict[str, Any] but enforces the collection and name fields
            allow_extra = True

    meta: Meta = Meta()
    data: Data


class CollectionPOSTResponse(BaseModel):
    data: Union[str, None]
    meta: ResponsePOSTMeta


### Result


class ResultGETBody(BaseModel):
    class Meta(BaseModel):
        projection: Dict[str, Any] = None

    meta: Meta = Meta()
    data: Dict[str, Any]

    @validator("data", whole=True)
    def only_data_keys(cls, v):
        # We should throw a warning here for unused keys
        valid_keys = {
            "program", "molecule", "driver", "method", "basis", "options", "hash_index", "task_id", "id", "status"
        }
        data = {key: v[key] for key in (v.keys() & valid_keys)}
        return data


class ResultGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: List[Dict[str, Any]]

    @validator("data", whole=True, pre=True)
    def ensure_list_of_dict(cls, v):
        if isinstance(v, dict):
            return [v]
        return v


class ResultPOSTBody(BaseModel):
    class Meta(BaseModel):
        overwrite: bool = False

    class Data(BaseModel):
        id: str = "local"  # Auto blocks overwriting
        collection: str
        name: str

        @validator("collection")
        def cast_to_lower(cls, v):
            return v.lower()

        class Config:
            # Maps effectively Dict[str, Any] but enforces the collection and name fields
            allow_extra = True

    meta: Meta = Meta()
    data: Data


class ResultPOSTResponse(BaseModel):
    data: Union[str, None]
    meta: ResponsePOSTMeta


### Procedures


class ProcedureGETBody(BaseModel):
    meta: Dict[str, Any] = {}
    data: Dict[str, Any]


class ProcedureGETReponse(BaseModel):
    meta: ResponseGETMeta
    data: List[Dict[str, Any]]

    @validator("data", whole=True, pre=True)
    def convert_dict_to_list_of_dict(cls, v):
        if isinstance(v, dict):
            return [v]
        return v
