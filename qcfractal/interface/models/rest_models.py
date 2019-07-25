"""
Models for the REST interface
"""
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseConfig, BaseModel, constr, validator

from .common_models import KeywordSet, Molecule, ObjectId
from .gridoptimization import GridOptimizationInput
from .model_utils import json_encoders
from .records import ResultRecord
from .task_models import PriorityEnum, TaskRecord
from .torsiondrive import TorsionDriveInput

__all__ = ["ComputeResponse", "rest_model", "QueryStr", "QueryObjectId", "QueryProjection"]

### Utility functions

__rest_models = {}


def register_model(name: str, rest: str, body: 'BaseModel', response: 'BaseModel') -> None:
    """
    Register a REST model.

    Parameters
    ----------
    name : str
        The REST endpoint name.
    rest : str
        The REST endpoint type.
    body : BaseModel
        The REST query body model.
    response : BaseModel
        The REST query response model.

    """

    name = name.lower()
    rest = rest.upper()

    if (name in __rest_models) and (rest in __rest_models[name]):
        raise KeyError(f"Model name {name} already registered.")

    if name not in __rest_models:
        __rest_models[name] = {}

    __rest_models[name][rest] = (body, response)


def rest_model(name: str, rest: str) -> Tuple['BaseModel', 'BaseModel']:
    """Aquires a REST Model

    Parameters
    ----------
    name : str
        The REST endpoint name.
    rest : str
        The REST endpoint type.

    Returns
    -------
    Tuple['BaseModel', 'BaseModel']
        The (body, response) models of the REST request.

    """
    try:
        return __rest_models[name.lower()][rest.upper()]
    except KeyError:
        raise KeyError(f"REST Model {name.lower()}:{rest.upper()} could not be found.")


### Generic Types and Common Models

nullstr = constr(regex='null')

QueryStr = Optional[Union[List[str], str]]
QueryInt = Optional[Union[List[int], int]]
QueryObjectId = Optional[Union[List[ObjectId], ObjectId]]
QueryNullObjectId = Optional[Union[List[ObjectId], ObjectId, List[nullstr], nullstr]]
QueryProjection = Optional[Dict[str, bool]]


class RESTConfig(BaseConfig):
    json_encoders = json_encoders
    extra = "forbid"


class EmptyMeta(BaseModel):
    class Config(RESTConfig):
        pass


class ResponseMeta(BaseModel):
    errors: List[Tuple[str, str]]
    success: bool
    error_description: Union[str, bool]

    class Config(RESTConfig):
        pass


class ResponseGETMeta(ResponseMeta):
    missing: List[str]
    n_found: int

    class Config(RESTConfig):
        pass


class ResponsePOSTMeta(ResponseMeta):
    n_inserted: int
    duplicates: Union[List[str], List[Tuple[str, str]]]
    validation_errors: List[str]

    class Config(RESTConfig):
        pass


class QueryMeta(BaseModel):
    limit: Optional[int] = None
    skip: int = 0

    class Config(RESTConfig):
        pass

class QueryMetaProjection(QueryMeta):
    projection: QueryProjection = None

    class Config(RESTConfig):
        pass


class ComputeResponse(BaseModel):
    ids: List[Optional[ObjectId]]
    submitted: List[ObjectId]
    existing: List[ObjectId]

    class Config(RESTConfig):
        pass

    def __str__(self) -> str:
        return f"ComputeResponse(nsubmitted={len(self.submitted)} nexisting={len(self.existing)})"

    def __repr__(self) -> str:
        return f"<{self}>"

    def merge(self, other: 'ComputeResponse') -> 'ComputeResponse':
        """Merges two ComputeResponse objects together. The first takes precedence and order is maintained.

        Parameters
        ----------
        other : ComputeResponse
            The compute response to merge

        Returns
        -------
        ComputeResponse
            The merged compute response
        """
        return ComputeResponse(
            ids=(self.ids + other.ids),
            submitted=(self.submitted + other.submitted),
            existing=(self.existing + other.existing))

### KVStore


class InformationGETBody(BaseModel):

    class Config(RESTConfig):
        pass


class InformationGETResponse(BaseModel):

    class Config(RESTConfig):
        extra = "allow"


register_model("information", "GET", InformationGETBody, InformationGETResponse)

### KVStore


class KVStoreGETBody(BaseModel):
    class Data(BaseModel):
        id: QueryObjectId = None

    meta: EmptyMeta = {}
    data: Data

    class Config(RESTConfig):
        pass


class KVStoreGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: Dict[str, Any]

    class Config(RESTConfig):
        pass


register_model("kvstore", "GET", KVStoreGETBody, KVStoreGETResponse)

### Molecule response


class MoleculeGETBody(BaseModel):
    class Data(BaseModel):
        id: QueryObjectId = None
        molecule_hash: QueryStr = None
        molecular_formula: QueryStr = None

        class Config(RESTConfig):
            pass

    meta: QueryMeta = QueryMeta()
    data: Data

    class Config(RESTConfig):
        pass


class MoleculeGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: List[Molecule]

    class Config(RESTConfig):
        pass


register_model("molecule", "GET", MoleculeGETBody, MoleculeGETResponse)


class MoleculePOSTBody(BaseModel):
    meta: EmptyMeta = {}
    data: List[Molecule]

    class Config(RESTConfig):
        pass


class MoleculePOSTResponse(BaseModel):
    meta: ResponsePOSTMeta
    data: List[ObjectId]

    class Config(RESTConfig):
        pass


register_model("molecule", "POST", MoleculePOSTBody, MoleculePOSTResponse)

### Keywords


class KeywordGETBody(BaseModel):
    class Data(BaseModel):
        id: QueryObjectId = None
        hash_index: QueryStr = None

        class Config(RESTConfig):
            pass

    meta: QueryMeta = QueryMeta()
    data: Data

    class Config(RESTConfig):
        pass


class KeywordGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: List[KeywordSet]

    class Config(RESTConfig):
        pass


register_model("keyword", "GET", KeywordGETBody, KeywordGETResponse)


class KeywordPOSTBody(BaseModel):
    meta: EmptyMeta = {}
    data: List[KeywordSet]

    class Config(RESTConfig):
        pass


class KeywordPOSTResponse(BaseModel):
    data: List[Optional[ObjectId]]
    meta: ResponsePOSTMeta

    class Config(RESTConfig):
        pass


register_model("keyword", "POST", KeywordPOSTBody, KeywordPOSTResponse)

### Collections


class CollectionGETBody(BaseModel):
    class Data(BaseModel):
        collection: str = None
        name: str = None

        @validator("collection")
        def cast_to_lower(cls, v):
            return v.lower()

        class Config(RESTConfig):
            pass

    class Meta(BaseModel):
        projection: Dict[str, Any] = None

        class Config(RESTConfig):
            pass

    meta: Meta = None
    data: Data

    class Config(RESTConfig):
        pass


class CollectionGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: List[Dict[str, Any]]

    @validator("data", whole=True)
    def ensure_collection_name_in_data_get_res(cls, v):
        for col in v:
            if "name" not in col or "collection" not in col:
                raise ValueError("Dicts in 'data' must have both 'collection' and 'name'")
        return v

    class Config(RESTConfig):
        pass


register_model("collection", "GET", CollectionGETBody, CollectionGETResponse)


class CollectionPOSTBody(BaseModel):
    class Meta(BaseModel):
        overwrite: bool = False

        class Config(RESTConfig):
            pass

    class Data(BaseModel):
        id: str = "local"  # Auto blocks overwriting in a socket
        collection: str
        name: str

        @validator("collection")
        def cast_to_lower(cls, v):
            return v.lower()

        class Config(RESTConfig):
            extra = "allow"

    meta: Meta = Meta()
    data: Data

    class Config(RESTConfig):
        pass


class CollectionPOSTResponse(BaseModel):
    data: Union[str, None]
    meta: ResponsePOSTMeta

    class Config(RESTConfig):
        pass


register_model("collection", "POST", CollectionPOSTBody, CollectionPOSTResponse)

### Result


class ResultGETBody(BaseModel):
    class Data(BaseModel):
        id: QueryObjectId = None
        task_id: QueryObjectId = None

        program: QueryStr = None
        molecule: QueryObjectId = None
        driver: QueryStr = None
        method: QueryStr = None
        basis: QueryStr = None
        keywords: QueryNullObjectId = None

        status: QueryStr = "COMPLETE"

        class Config(RESTConfig):
            pass

        @validator('keywords', pre=True)
        def validate_keywords(cls, v):
            if v is None:
                v = 'null'
            return v

        @validator('basis', pre=True)
        def validate_basis(cls, v):
            if (v is None) or (v == ""):
                v = 'null'
            return v

    meta: QueryMetaProjection = QueryMetaProjection()
    data: Data

    class Config(RESTConfig):
        pass


class ResultGETResponse(BaseModel):
    meta: ResponseGETMeta
    # Either a record or dict depending if projection
    data: Union[List[ResultRecord], List[Dict[str, Any]]]

    @validator("data", whole=True, pre=True)
    def ensure_list_of_dict(cls, v):
        if isinstance(v, dict):
            return [v]
        return v

    class Config(RESTConfig):
        pass


register_model("result", "GET", ResultGETBody, ResultGETResponse)

### Procedures


class ProcedureGETBody(BaseModel):
    class Data(BaseModel):
        id: QueryObjectId = None
        task_id: QueryObjectId = None

        procedure: QueryStr = None
        program: QueryStr = None
        hash_index: QueryStr = None

        status: QueryStr = "COMPLETE"

        class Config(RESTConfig):
            pass

    meta: QueryMetaProjection = QueryMetaProjection()
    data: Data

    class Config(RESTConfig):
        pass


class ProcedureGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: List[Dict[str, Any]]

    class Config(RESTConfig):
        pass


register_model("procedure", "GET", ProcedureGETBody, ProcedureGETResponse)

### Task Queue


class TaskQueueGETBody(BaseModel):
    class Data(BaseModel):
        id: QueryObjectId = None
        hash_index: QueryStr = None
        program: QueryStr = None
        status: QueryStr = None
        base_result: QueryStr = None

        class Config(RESTConfig):
            pass

    meta: QueryMetaProjection = QueryMetaProjection()
    data: Data


class TaskQueueGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: Union[List[TaskRecord], List[Dict[str, Any]]]

    class Config(RESTConfig):
        pass


register_model("task_queue", "GET", TaskQueueGETBody, TaskQueueGETResponse)


class TaskQueuePOSTBody(BaseModel):
    class Meta(BaseModel):
        procedure: str
        program: str

        tag: Optional[str] = None
        priority: Union[PriorityEnum, None] = None

        class Config(RESTConfig):
            allow_extra = "allow"

        @validator('priority', pre=True)
        def munge_priority(cls, v):
            if isinstance(v, str):
                v = PriorityEnum[v.upper()]
            return v


    meta: Meta
    data: List[Union[ObjectId, Molecule]]

    class Config(RESTConfig):
        pass


class TaskQueuePOSTResponse(BaseModel):

    meta: ResponsePOSTMeta
    data: ComputeResponse

    class Config(RESTConfig):
        pass


register_model("task_queue", "POST", TaskQueuePOSTBody, TaskQueuePOSTResponse)

class TaskQueuePUTBody(BaseModel):
    class Data(BaseModel):
        id: QueryObjectId = None
        base_result: QueryObjectId = None

        class Config(RESTConfig):
            pass

    class Meta(BaseModel):
        operation: str

        class Config(RESTConfig):
            pass

        @validator("operation")
        def cast_to_lower(cls, v):
            return v.lower()

    meta: Meta
    data: Data

    class Config(RESTConfig):
        pass


class TaskQueuePUTResponse(BaseModel):
    class Data(BaseModel):
        n_updated: int

        class Config(RESTConfig):
            pass

    meta: ResponseMeta
    data: Data

    class Config(RESTConfig):
        pass


register_model("task_queue", "PUT", TaskQueuePUTBody, TaskQueuePUTResponse)

### Service Queue


class ServiceQueueGETBody(BaseModel):
    class Data(BaseModel):
        id: QueryObjectId = None
        procedure_id: QueryObjectId = None
        hash_index: QueryStr = None
        status: QueryStr = None

    meta: QueryMeta = QueryMeta()
    data: Data

    class Config(RESTConfig):
        pass


class ServiceQueueGETResponse(BaseModel):
    meta: ResponseGETMeta
    data: List[Dict[str, Any]]

    class Config(RESTConfig):
        pass


register_model("service_queue", "GET", ServiceQueueGETBody, ServiceQueueGETResponse)


class ServiceQueuePOSTBody(BaseModel):
    class Meta(BaseModel):
        tag: Optional[str] = None
        priority: Union[str, int, None] = None

        class Config(RESTConfig):
            pass

    meta: Meta
    data: List[Union[TorsionDriveInput, GridOptimizationInput]]

    class Config(RESTConfig):
        pass


class ServiceQueuePOSTResponse(BaseModel):

    meta: ResponsePOSTMeta
    data: ComputeResponse

    class Config(RESTConfig):
        pass


register_model("service_queue", "POST", ServiceQueuePOSTBody, ServiceQueuePOSTResponse)

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


register_model("queue_manager", "GET", QueueManagerGETBody, QueueManagerGETResponse)


class QueueManagerPOSTBody(BaseModel):
    meta: QueueManagerMeta
    data: Dict[ObjectId, Any]

    class Config:
        json_encoders = json_encoders


class QueueManagerPOSTResponse(BaseModel):
    meta: ResponsePOSTMeta
    data: bool


register_model("queue_manager", "POST", QueueManagerPOSTBody, QueueManagerPOSTResponse)


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


register_model("queue_manager", "PUT", QueueManagerPUTBody, QueueManagerPUTResponse)
