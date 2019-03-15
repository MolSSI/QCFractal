"""
Models for the REST interface
"""
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseConfig, BaseModel, validator

from .common_models import KeywordSet, Molecule, ObjectId
from .gridoptimization import GridOptimizationInput
from .model_utils import json_encoders
from .task_models import TaskRecord
from .torsiondrive import TorsionDriveInput
from .records import ResultRecord

__all__ = [
    "ResponseGETMeta",
    "MoleculeGETBody", "MoleculeGETResponse", "MoleculePOSTBody", "MoleculePOSTResponse",
    "KeywordGETBody", "KeywordGETResponse", "KeywordPOSTBody", "KeywordPOSTResponse",
    "CollectionGETBody", "CollectionGETResponse", "CollectionPOSTBody", "CollectionPOSTResponse",
    "ResultGETBody", "ResultGETResponse",
    "ProcedureGETBody", "ProcedureGETReponse",
    "TaskQueueGETBody", "TaskQueueGETResponse", "TaskQueuePOSTBody", "TaskQueuePOSTResponse",
    "ServiceQueueGETBody", "ServiceQueueGETResponse", "ServiceQueuePOSTBody", "ServiceQueuePOSTResponse",
    "QueueManagerGETBody", "QueueManagerGETResponse", "QueueManagerPOSTBody", "QueueManagerPOSTResponse",
    "QueueManagerPUTBody", "QueueManagerPUTResponse"
]  # yapf: disable


### Generic and Common Models

QueryStr = Optional[Union[List[str], str]]
QueryInt = Optional[Union[List[int], int]]
QueryObjectId = Optional[Union[List[ObjectId], ObjectId]]


class RESTConfig(BaseConfig):
    json_encoders = json_encoders
    extra = "forbid"


class ResponseMeta(BaseModel):
    errors: List[Tuple[str, str]]
    success: bool
    error_description: Union[str, bool]


class ResponseGETMeta(ResponseMeta):
    missing: List[str]
    n_found: int


class ResponsePOSTMeta(ResponseMeta):
    n_inserted: int
    duplicates: Union[List[str], List[Tuple[str, str]]]
    validation_errors: List[str]


### KVStore


class KVStoreGETBody(BaseModel):
    data: List[ObjectId]
    meta: Dict[str, Any]


class KVStoreGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: Dict[str, Any]

    class Config:
        json_encoders = json_encoders


### Molecule response


class MoleculeGETBody(BaseModel):

    data: Dict[str, Any]
    meta: Dict[str, Any]


class MoleculeGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: List[Molecule]

    class Config:
        json_encoders = json_encoders


class MoleculePOSTBody(BaseModel):
    meta: Dict[str, Any] = None
    data: List[Molecule]

    class Config:
        json_encoders = json_encoders


class MoleculePOSTResponse(BaseModel):
    meta: ResponsePOSTMeta
    data: List[str]


### Keywords


class KeywordGETBody(BaseModel):
    meta: Dict[str, Any] = None
    data: Dict[str, Any]


class KeywordGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: List[KeywordSet]


class KeywordPOSTBody(BaseModel):
    meta: Dict[str, Any] = None
    data: List[KeywordSet]

    # @validator("data", whole=True, pre=True)
    # def ensure_list_of_dict(cls, v):
    #     if isinstance(v, dict):
    #         return [v]
    #     return v


class KeywordPOSTResponse(BaseModel):
    data: List[Optional[str]]
    meta: ResponsePOSTMeta


### Collections


class CollectionGETBody(BaseModel):
    class Data(BaseModel):
        collection: str = None
        name: str = None

        @validator("collection")
        def cast_to_lower(cls, v):
            return v.lower()

    class Meta(BaseModel):
        projection: Dict[str, Any] = None

    meta: Meta = None
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
            extra = "allow"

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
        valid_keys = {"program", "molecule", "driver", "method", "basis", "keywords", "task_id", "id", "status"}
        data = {key: v[key] for key in (v.keys() & valid_keys)}
        if "keywords" in data and data["keywords"] is None:
            data["keywords"] = 'null'
        if "basis" in data and ((data["basis"] is None) or (data["basis"] == "")):
            data["basis"] = 'null'
        return data


class ResultGETResponse(BaseModel):
    meta: ResponseGETMeta
    # Either a record or dict depending if projection
    data: Union[List[ResultRecord], List[Dict[str, Any]]]

    @validator("data", whole=True, pre=True)
    def ensure_list_of_dict(cls, v):
        if isinstance(v, dict):
            return [v]
        return v


### Procedures


class ProcedureGETBody(BaseModel):
    meta: Dict[str, Any] = {}
    data: Dict[str, Any]


class ProcedureGETReponse(BaseModel):
    meta: ResponseGETMeta
    data: List[Dict[str, Any]]

    @validator("data", whole=True, pre=True)
    def ensure_list_of_dict(cls, v):
        if isinstance(v, dict):
            return [v]
        return v


### Task Queue


class TaskQueueGETBody(BaseModel):
    class Meta(BaseModel):
        projection: Dict[str, Any] = None

    meta: Meta = Meta()
    data: Dict[str, Any]


class TaskQueueGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: Union[List[TaskRecord], List[Dict[str, Any]]]

    @validator("data", whole=True, pre=True)
    def ensure_list_of_dict(cls, v):
        if isinstance(v, dict):
            return [v]
        return v


class TaskQueuePOSTBody(BaseModel):

    meta: Dict[str, Any]
    data: List[Union[str, Molecule]]

    class Config:
        json_encoders = json_encoders

    @validator("data", whole=True, pre=True)
    def ensure_list_of_dict(cls, v):
        if not isinstance(v, list):
            return [v]
        return v


class TaskQueuePOSTResponse(BaseModel):
    class Data(BaseModel):
        ids: List[Optional[str]]
        submitted: List[str]
        existing: List[str]

    meta: ResponsePOSTMeta
    data: Data


### Service Queue


class ServiceQueueGETBody(BaseModel):
    class Meta(BaseModel):
        projection: Optional[Dict[str, bool]] = None

    class Data(BaseModel):
        id: QueryObjectId = None
        procedure_id: QueryObjectId = None
        hash_index: QueryStr = None
        status: QueryStr = None

    meta: Meta
    data: Data

class ServiceQueueGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: List[Dict[str, Any]]

    @validator("data", whole=True, pre=True)
    def ensure_list_of_dict(cls, v):
        if isinstance(v, dict):
            return [v]
        return v


class ServiceQueuePOSTBody(BaseModel):
    class Meta(BaseModel):
        tag: Optional[str] = None
        priority: Union[str, int, None] = None

    meta: Meta
    data: List[Union[TorsionDriveInput, GridOptimizationInput]]

    class Config(RESTConfig):
        pass


class ServiceQueuePOSTResponse(BaseModel):
    class Data(BaseModel):
        ids: List[Optional[str]]
        submitted: List[str]
        existing: List[str]

    meta: ResponsePOSTMeta
    data: Data

    class Config(RESTConfig):
        pass


### Queue Manager


class QueueManagerMeta(BaseModel):
    # Name data
    cluster: str
    hostname: str
    uuid: str

    # Username
    username: Optional[str] = None

    # Version info
    qcengine_version: str
    manager_version: str

    # search info
    programs: List[str]
    procedures: List[str]
    tag: Optional[str] = None

    class Config(RESTConfig):
        pass


class QueueManagerGETBody(BaseModel):
    class Data(BaseModel):
        limit: int

    meta: QueueManagerMeta
    data: Data

    class Config(RESTConfig):
        pass


class QueueManagerGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: List[Dict[str, Any]]

    @validator("data", whole=True, pre=True)
    def ensure_list_of_dict(cls, v):
        if isinstance(v, dict):
            return [v]
        return v


class QueueManagerPOSTBody(BaseModel):
    meta: QueueManagerMeta
    data: Dict[str, Any]

    class Config:
        json_encoders = json_encoders


class QueueManagerPOSTResponse(BaseModel):
    meta: ResponsePOSTMeta
    data: bool


class QueueManagerPUTBody(BaseModel):
    class Data(BaseModel):
        operation: str

    meta: QueueManagerMeta
    data: Data


class QueueManagerPUTResponse(BaseModel):
    meta: Dict[str, Any] = {}
    # Order on Union[] is important. Union[bool, Dict[str, int]] -> True if the input dict is not empty since
    # Python can resolve dict -> bool since it passes a `is` test. Will not cast bool -> dict[str, int], so make Dict[]
    # check first
    data: Union[Dict[str, int], bool]
