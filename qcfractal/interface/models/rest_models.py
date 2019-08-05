"""
Models for the REST interface
"""
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseConfig, BaseModel, constr, validator, Schema

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
    errors: List[Tuple[str, str]] = Schema(
        ...,
        description="A list of error pairs in the form of [(error type, error message), ...]"
    )
    success: bool = Schema(
        ...,
        description="Indicates if the passed information was successful in its duties. This is contextual to the "
                    "data being passed in."
    )
    error_description: Union[str, bool] = Schema(
        ...,
        description="Details about the error if ``success`` is ``False``, otherwise this is ``False`` in the event "
                    "of no errors."
    )

    class Config(RESTConfig):
        pass


class ResponseGETMeta(ResponseMeta):
    missing: List[str] = Schema(
        ...,
        description="ID's of the objects which were not found in the database."
    )
    n_found: int = Schema(
        ...,
        description="The number of entries which were already found in the database from the set which was provided."
    )

    class Config(RESTConfig):
        pass


class ResponsePOSTMeta(ResponseMeta):
    n_inserted: int = Schema(
        ...,
        description="The number of new objects amongst the inputs which did not exist already, and are now in the "
                    "database."
    )
    duplicates: Union[List[str], List[Tuple[str, str]]] = Schema(
        ...,
        description="The IDs of the objects which already exist in the database amongst the set which were passed in."
    )
    validation_errors: List[str] = Schema(
        ...,
        description="All errors with validating submitted objects will be documented here."
    )

    class Config(RESTConfig):
        pass


class QueryMeta(BaseModel):
    limit: Optional[int] = Schema(
        None,
        description="Limit to the number of objects which can be returned with this query"
    )
    skip: int = Schema(
        0,
        description="The number of records to skip on the query."
    )

    class Config(RESTConfig):
        pass


class QueryMetaProjection(QueryMeta):
    projection: QueryProjection = Schema(
        None,
        description="Additional projection information to pass to the query. Expert-level object."
    )

    class Config(RESTConfig):
        pass


class ComputeResponse(BaseModel):
    ids: List[Optional[ObjectId]] = Schema(...,
                                           description="The ID's of the records to be computed")
    submitted: List[ObjectId] = Schema(
        ...,
        description="Which object IDs which were submitted as new entries to the database"
    )
    existing: List[ObjectId] = Schema(
        ...,
        description="When object IDs already existed within the database"
    )

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
        id: QueryObjectId = Schema(
            None,
            description="ID of the Key/Value Storage object to get"
        )

    meta: EmptyMeta = Schema(
        {},
        description="There is no metadata with this, so an empty metadata is sent for completion."
    )
    data: Data = Schema(
        ...,
        description="Data of the KV Get field, consists of a dict for ID of the Key/Value object to fetch"
    )

    class Config(RESTConfig):
        pass


class KVStoreGETResponse(BaseModel):
    meta: ResponseGETMeta = Schema(
        ...,
        description="Metadata associated with the response for fetching a Key/Value object"
    )
    data: Dict[str, Any] = Schema(
        ...,
        description="The entries of Key/Value object requested."
    )

    class Config(RESTConfig):
        pass


register_model("kvstore", "GET", KVStoreGETBody, KVStoreGETResponse)

### Molecule response


class MoleculeGETBody(BaseModel):
    class Data(BaseModel):
        id: QueryObjectId = Schema(
            None,
            description="Exact ID of the Molecule to fetch from the database"
        )
        molecule_hash: QueryStr = Schema(
            None,
            description="Hash of the Molecule to search for in the database. Can be computed from the Molecule object "
                        "directly without direct access to the Database itself"
        )
        molecular_formula: QueryStr = Schema(
            None,
            description="Make a query based on simple molecular formula. This is based on just the formula itself and "
                        "contains no connectivity information."
        )

        class Config(RESTConfig):
            pass

    meta: QueryMeta = Schema(
        QueryMeta(),
        description="Meta data for querying a Molecule"
    )
    data: Data = Schema(
        ...,
        description="Data fields for a Molecule query."  # Because Data is internal, this may not document sufficiently
    )

    class Config(RESTConfig):
        pass


class MoleculeGETResponse(BaseModel):
    meta: ResponseGETMeta = Schema(
        ...,
        description="The response from a Molecule Query"
    )
    data: List[Molecule] = Schema(
        ...,
        description="The List of Molecule objects found by the Query"
    )

    class Config(RESTConfig):
        pass


register_model("molecule", "GET", MoleculeGETBody, MoleculeGETResponse)


class MoleculePOSTBody(BaseModel):
    meta: EmptyMeta = Schema(
        {},
        description="There is no metadata with this, so an empty metadata is sent for completion."
    )
    data: List[Molecule] = Schema(
        ...,
        description="A list of Molecule objects to add to the Database"
    )

    class Config(RESTConfig):
        pass


class MoleculePOSTResponse(BaseModel):
    meta: ResponsePOSTMeta = Schema(
        ...,
        description="The response from adding Molecules to the database"
    )
    data: List[ObjectId] = Schema(
        ...,
        description="A list of ID's assigned to the Molecule objects passed in which serves as a unique identifier "
                    "in the database. If the Molecule was already in the database, then the ID returned is its "
                    "existing ID (entries are not duplicated)"
    )

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

    meta: QueryMeta = Schema(
        QueryMeta(),
        description="Standard query metadata"
    )
    data: Data = Schema(
        ...,
        description="The formal query for a Keyword fetch, contains ``id`` or ``hash_index`` for the object to fetch."
    )

    class Config(RESTConfig):
        pass


class KeywordGETResponse(BaseModel):
    meta: ResponseGETMeta = Schema(
        ...,
        description="Additional response information from the server for the :class:`KeywordSet` fetch request."
    )
    data: List[KeywordSet] = Schema(
        ...,
        description="The :class:`KeywordSet` found from in the database based on the query."
    )

    class Config(RESTConfig):
        pass


register_model("keyword", "GET", KeywordGETBody, KeywordGETResponse)


class KeywordPOSTBody(BaseModel):
    meta: EmptyMeta = Schema(
        {},
        description="There is no metadata with this, so an empty metadata is sent for completion."
    )
    data: List[KeywordSet] = Schema(
        ...,
        description="The list of :class:`KeywordSet` objects to add to the database."
    )

    class Config(RESTConfig):
        pass


class KeywordPOSTResponse(BaseModel):
    data: List[Optional[ObjectId]] = Schema(
        ...,
        description="The IDs assigned to the added :class:`KeywordSet` objects. In the event of duplicates, the ID "
                    "will be the one already found in the database."
    )
    meta: ResponsePOSTMeta = Schema(
        ...,
        description="Standard metadata from attempting to add an object to the Database."
    )

    class Config(RESTConfig):
        pass


register_model("keyword", "POST", KeywordPOSTBody, KeywordPOSTResponse)

### Collections


class CollectionGETBody(BaseModel):
    class Data(BaseModel):
        collection: str = Schema(
            None,
            description="The specific collection to look up as its identified in the database."
        )
        name: str = Schema(
            None,
            description="The common name of the collection to look up"
        )

        @validator("collection")
        def cast_to_lower(cls, v):
            return v.lower()

        class Config(RESTConfig):
            pass

    class Meta(BaseModel):
        projection: Dict[str, Any] = Schema(
            None,
            description="Additional projection information to pass to the query. Expert-level object."
        )

        class Config(RESTConfig):
            pass

    meta: Meta = Schema(
        None,
        description="Additional metadata to make with the query. Collections can only have a ``projection`` key in its "
                    "meta."
    )
    data: Data = Schema(
        ...,
        description="Information about the Collection to search the database with."
    )

    class Config(RESTConfig):
        pass


class CollectionGETResponse(BaseModel):
    meta: ResponseGETMeta = Schema(
        ...,
        description="Standard meta information for any fetch query"
    )
    data: List[Dict[str, Any]] = Schema(
        ...,
        description="The Collection objects returned by the server based on the query. "
    )

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
        overwrite: bool = Schema(
            False,
            description="Update the existing Collection object in the database or not."
        )

        class Config(RESTConfig):
            pass

    class Data(BaseModel):
        id: str = Schema(
            "local",  # Auto blocks overwriting in a socket
            description="The ID of the object to assign in the database. If 'local', then it will not overwrite "
                        "existing keys. There should be very little reason to ever touch this."
        )
        collection: str = Schema(
            ...,
            description="The specific identifier for this Collection as it will appear in database."
        )
        name: str = Schema(
            ...,
            description="The common name of this Collection"
        )

        @validator("collection")
        def cast_to_lower(cls, v):
            return v.lower()

        class Config(RESTConfig):
            extra = "allow"

    meta: Meta = Schema(
        Meta(),
        description="Collection addition metas can only accept ``overwrite`` as a key to choose to update existing "
                    "collections or not."
    )
    data: Data = Schema(
        ...,
        description="The data associated with this Collection to add to the database"
    )

    class Config(RESTConfig):
        pass


class CollectionPOSTResponse(BaseModel):
    data: Union[str, None] = Schema(
        ...,
        description="The ID of the Collection uniquely pointing to it in the Database. If the Collection was not added "
                    "(e.g. ``overwrite=False`` for existing Collection), then a None is returned."
    )
    meta: ResponsePOSTMeta = Schema(
        ...,
        description="Standard metadata for adding entries."
    )

    class Config(RESTConfig):
        pass


register_model("collection", "POST", CollectionPOSTBody, CollectionPOSTResponse)

### Result


class ResultGETBody(BaseModel):
    class Data(BaseModel):
        id: QueryObjectId = Schema(
            None,
            description="The exact ID to fetch from the database. If this is set as a search condition, there is no "
                        "reason to set anything else as this will be unique in the database, if it exists."
        )
        task_id: QueryObjectId = Schema(
            None,
            description="The exact ID of the task which carried out this Result's computation. If this is set as a "
                        "search condition, there is no reason to set anything else as this will be unique in the "
                        "database, if it exists. See also :class:`TaskRecord`"
        )

        program: QueryStr = Schema(
            None,
            description="Find Results based on the quantum chemistry software which carried out the calculation."
        )
        molecule: QueryObjectId = Schema(
            None,
            description="Find Results based on Molecule ID which was computed on."
        )
        driver: QueryStr = Schema(
            None,
            description="Find Results based on what class of computation was done. See :class:`DriverEnum` for more "
                        "information"
        )
        method: QueryStr = Schema(
            None,
            description="Find Results based on the quantum chemistry method executed to compute the value."
        )
        basis: QueryStr = Schema(
            None,
            description="Find Results based on specific basis sets which were used to compute the values."
        )
        keywords: QueryNullObjectId = Schema(
            None,
            description="Find Results based on which :class:`KeywordSet` was used to run the computation"
        )

        status: QueryStr = Schema(
            "COMPLETE",
            description="Find Results based on where they are in the compute pipeline. See the "
                        ":class:`RecordStatusEnum` for valid statuses."
        )

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

    meta: QueryMetaProjection = Schema(
        QueryMetaProjection(),
        description="Standard metadata from retrieval queries."
    )
    data: Data = Schema(
        ...,
        description="The keys with data to search the database on for individual quantum chemistry computations."
    )

    class Config(RESTConfig):
        pass


class ResultGETResponse(BaseModel):
    meta: ResponseGETMeta = Schema(
        ...,
        description="Standard metadata for a fetch query."
    )
    # Either a record or dict depending if projection
    data: Union[List[ResultRecord], List[Dict[str, Any]]] = Schema(
        ...,
        description="Results found from the query. This is a list of :class:`ResultRecord` in most cases, however, "
                    "if a projection was specified in the GET request, then a dict is returned with mappings based "
                    "on the projection."
    )

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
        id: QueryObjectId = Schema(
            None,
            description="The exact ID to fetch from the database. If this is set as a search condition, there is no "
                        "reason to set anything else as this will be unique in the database, if it exists."
        )
        task_id: QueryObjectId = Schema(  # TODO: Validate this description is correct.
            None,
            description="The exact ID of a task which carried out by this Procedure. If this is set as a "
                        "search condition, there is no reason to set anything else as this will be unique in the "
                        "database, if it exists. See also :class:`TaskRecord`"
        )

        procedure: QueryStr = Schema(
            None,
            description="Find procedure based on the name of the procedure"
        )
        program: QueryStr = Schema(
            None,
            description="Find a procedure based on the program which is the main manager of the procedure"
        )
        hash_index: QueryStr = Schema(
            None,
            description="Search the database based on a hash of the defined procedure. This is something which can "
                        "be generated by the Procedure spec itself and does not require server access to compute. "
                        "This should be unique in the database so there should be no reason to set anything else "
                        "if this is set as a query."
        )
        status: QueryStr = Schema(
            "COMPLETE",
            description="Find Procedures based on where they are in the compute pipeline. See the "
                        ":class:`RecordStatusEnum` for valid statuses."
        )

        class Config(RESTConfig):
            pass

    meta: QueryMetaProjection = Schema(
        QueryMetaProjection(),
        description="Standard metadata from retrieval queries."
    )
    data: Data = Schema(
        ...,
        description="The keys with data to search the database on for Procedures."
    )

    class Config(RESTConfig):
        pass


class ProcedureGETResponse(BaseModel):
    meta: ResponseGETMeta = Schema(
        ...,
        description="Standard metadata returned for fetch queries."
    )
    data: List[Dict[str, Any]] = Schema(
        ...,
        description="The list of Procedure specs found based on the query."
    )

    class Config(RESTConfig):
        pass


register_model("procedure", "GET", ProcedureGETBody, ProcedureGETResponse)

### Task Queue


class TaskQueueGETBody(BaseModel):
    class Data(BaseModel):
        id: QueryObjectId = Schema(
            None,
            description="The exact ID to fetch from the database. If this is set as a search condition, there is no "
                        "reason to set anything else as this will be unique in the database, if it exists."
        )
        hash_index: QueryStr = Schema(
            None,
            description="Search the database based on a hash of the defined Task. This is something which can "
                        "be generated by the Task spec itself and does not require server access to compute. "
                        "This should be unique in the database so there should be no reason to set anything else "
                        "if this is set as a query."
        )
        program: QueryStr = Schema(
            None,
            description="Find a Task based on the program which is responsible for executing this task"
        )
        status: QueryStr = Schema(
            None,
            description="Find Tasks based on where they are in the compute pipeline. See the "
                        ":class:`RecordStatusEnum` for valid statuses."
        )
        base_result: QueryStr = Schema(  # TODO: Validate this description is correct
            None,
            description="The exact ID of a result which this Task will ultimately write to. If this is set as a "
                        "search condition, there is no reason to set anything else as this will be unique in the "
                        "database, if it exists. See also :class:`ResultRecord`"
        )

        class Config(RESTConfig):
            pass

    meta: QueryMetaProjection = Schema(
        QueryMetaProjection(),
        description="Standard metadata from retrieval queries."
    )
    data: Data = Schema(
        ...,
        description="The keys with data to search the database on for Tasks."
    )


class TaskQueueGETResponse(BaseModel):
    meta: ResponseGETMeta = Schema(
        ...,
        description="Standard metadata returned for fetch queries."
    )
    data: Union[List[TaskRecord], List[Dict[str, Any]]] = Schema(
        ...,
        description="Tasks found from the query. This is a list of :class:`TaskRecord` in most cases, however, "
                    "if a projection was specified in the GET request, then a dict is returned with mappings based "
                    "on the projection."
    )

    class Config(RESTConfig):
        pass


register_model("task_queue", "GET", TaskQueueGETBody, TaskQueueGETResponse)


class TaskQueuePOSTBody(BaseModel):
    class Meta(BaseModel):
        procedure: str = Schema(
            ...,
            description="Name of the procedure which the Task will execute"
        )
        program: str = Schema(
            ...,
            description="The program which this Task will execute"
        )

        tag: Optional[str] = Schema(
            None,
            description="Tag to assign to this Task so that Queue Managers can pull only Tasks based on this entry."
                        "If no Tag is specified, any Queue Manager can pull this Task"
        )
        priority: Union[PriorityEnum, None] = Schema(
            None,
            description="Priority given to this Task. Higher priority will be pulled first."
        )

        class Config(RESTConfig):
            allow_extra = "allow"

        @validator('priority', pre=True)
        def munge_priority(cls, v):
            if isinstance(v, str):
                v = PriorityEnum[v.upper()]
            return v

    meta: Meta = Schema(
        ...,
        description="The additional specification information for the Task to add to the Database"
    )
    data: List[Union[ObjectId, Molecule]] = Schema(
        ...,
        description="The list of either Molecule objects or Molecule ID's (those already in the database) to submit as "
                    "part of this Task."
    )

    class Config(RESTConfig):
        pass


class TaskQueuePOSTResponse(BaseModel):

    meta: ResponsePOSTMeta = Schema(
        ...,
        description="Standard metadata response for an add action."
    )
    data: ComputeResponse = Schema(
        ...,
        description="Data returned from the server from adding a Task"
    )

    class Config(RESTConfig):
        pass


register_model("task_queue", "POST", TaskQueuePOSTBody, TaskQueuePOSTResponse)


class TaskQueuePUTBody(BaseModel):
    class Data(BaseModel):
        id: QueryObjectId = Schema(
            None,
            description="The exact ID to target in database. If this is set as a search condition, there is no "
                        "reason to set anything else as this will be unique in the database, if it exists."
        )
        base_result: QueryObjectId = Schema(  # TODO: Validate this description is correct
            None,
            description="The exact ID of a result which this Task is slated to write to. If this is set as a "
                        "search condition, there is no reason to set anything else as this will be unique in the "
                        "database, if it exists. See also :class:`ResultRecord`"
        )

        class Config(RESTConfig):
            pass

    class Meta(BaseModel):
        operation: str = Schema(
            ...,
            description="The specific action you are taking as part of this update"
        )

        class Config(RESTConfig):
            pass

        @validator("operation")
        def cast_to_lower(cls, v):
            return v.lower()

    meta: Meta = Schema(
        ...,
        description="The instructions to pass to the target Task from ``data``."
    )
    data: Data = Schema(
        ...,
        description="The information which contains the Task target in the database"
    )

    class Config(RESTConfig):
        pass


class TaskQueuePUTResponse(BaseModel):
    class Data(BaseModel):
        n_updated: int = Schema(
            ...,
            description="The number of tasks which were changed"
        )

        class Config(RESTConfig):
            pass

    meta: ResponseMeta = Schema(
        ...,
        description="Standard metadata returned from the database"
    )
    data: Data = Schema(
        ...,
        description="Information returned from attempting updates of Tasks"
    )

    class Config(RESTConfig):
        pass


register_model("task_queue", "PUT", TaskQueuePUTBody, TaskQueuePUTResponse)

### Service Queue


class ServiceQueueGETBody(BaseModel):
    class Data(BaseModel):
        id: QueryObjectId = Schema(
            None,
            description="The exact ID to fetch from the database. If this is set as a search condition, there is no "
                        "reason to set anything else as this will be unique in the database, if it exists."
        )
        procedure_id: QueryObjectId = Schema(  # TODO: Validate this description is correct
            None,
            description="The exact ID of the Procedure this Service is responsible for executing. If this is set as a "
                        "search condition, there is no reason to set anything else as this will be unique in the "
                        "database, if it exists."
        )
        hash_index: QueryStr = Schema(
            None,
            description="Search the database based on a hash of the defined Service. This is something which can "
                        "be generated by the Service spec itself and does not require server access to compute. "
                        "This should be unique in the database so there should be no reason to set anything else "
                        "if this is set as a query."
        )
        status: QueryStr = Schema(
            None,
            description="Find Tasks based on where they are in the compute pipeline. See the "
                        ":class:`RecordStatusEnum` for valid statuses."
        )

    meta: QueryMeta = Schema(
        QueryMeta(),
        description="Standard metadata from retrieval queries."
    )
    data: Data = Schema(
        ...,
        description="The keys with data to search the database on for Services."
    )

    class Config(RESTConfig):
        pass


class ServiceQueueGETResponse(BaseModel):
    meta: ResponseGETMeta = Schema(
        ...,
        description="Standard metadata returned for fetch queries."
    )
    data: List[Dict[str, Any]] = Schema(
        ...,
        description="The return of Services found in the database mapping their IDs to the Service spec."
    )

    class Config(RESTConfig):
        pass


register_model("service_queue", "GET", ServiceQueueGETBody, ServiceQueueGETResponse)


class ServiceQueuePOSTBody(BaseModel):
    class Meta(BaseModel):
        tag: Optional[str] = Schema(
            None,
            description="Tag to assign to the Tasks this Service will generate so that Queue Managers can pull only "
                        "Tasks based on this entry. If no Tag is specified, any Queue Manager can pull this Tasks "
                        "created by this Service."
        )
        priority: Union[str, int, None] = Schema(
            None,
            description="Priority given to this Tasks created by this Service. Higher priority will be pulled first."
        )

        class Config(RESTConfig):
            pass

    meta: Meta = Schema(
        ...,
        description="Metadata information for the Service for the Tag and Priority of Tasks this Service will create."
    )
    data: List[Union[TorsionDriveInput, GridOptimizationInput]] = Schema(
        ...,
        description="A list the specification for Procedures this Service will manage and generate Tasks for."
    )

    class Config(RESTConfig):
        pass


class ServiceQueuePOSTResponse(BaseModel):

    meta: ResponsePOSTMeta = Schema(
        ...,
        description="Standard metadata response for an add action."
    )
    data: ComputeResponse = Schema(
        ...,
        description="Data returned from the server from adding a Service"
    )

    class Config(RESTConfig):
        pass


register_model("service_queue", "POST", ServiceQueuePOSTBody, ServiceQueuePOSTResponse)

### Queue Manager


class QueueManagerMeta(BaseModel):
    # Name data
    cluster: str = Schema(
        ...,
        description="The Name of the Cluster the Queue Manager is running on"
    )
    hostname: str = Schema(
        ...,
        description="Hostname of the machine the Queue Manager is running on"
    )
    uuid: str = Schema(
        ...,
        description="A UUID assigned to the QueueManager to uniquely identify it."
    )

    # Username
    username: Optional[str] = Schema(
        None,
        description="Fractal Username the Manager is being executed under"
    )

    # Version info
    qcengine_version: str = Schema(
        ...,
        description="Version of QCEngine which the Manager has access to."
    )
    manager_version: str = Schema(
        ...,
        description="Version of the QueueManager (Fractal) which is getting and returning Jobs."
    )

    # search info
    programs: List[str] = Schema(
        ...,
        description="A list of programs which the QueueManager, and thus QCEngine, has access to. Affects which Tasks "
                    "the Manager can pull"
    )
    procedures: List[str] = Schema(
        ...,
        description="A list of procedures which the QueueManager has access to. Affects which Tasks "
                    "the Manager can pull"
    )
    tag: Optional[str] = Schema(
        None,
        description="Optional queue tag to pull Tasks from"
    )

    class Config(RESTConfig):
        pass


class QueueManagerGETBody(BaseModel):
    class Data(BaseModel):
        limit: int = Schema(
            ...,
            description="Max number of Queue Managers to get from the server"  # TODO: Verify this.
        )

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
