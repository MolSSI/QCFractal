"""
Models for the REST interface
"""
import functools
import re
import warnings
from enum import Enum

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import Field, constr, root_validator, validator
from qcelemental.util import get_base_docs

from .common_models import (
    AllResultTypes,
    KeywordSet,
    Molecule,
    ObjectId,
    ProtoModel,
    OutputStore,
)
from .task_models import (
    SingleProcedureSpecification,
    OptimizationProcedureSpecification,
    TaskRecord,
    ManagerStatusEnum,
)
from .task_models import PriorityEnum
from .records import SinglePointRecord, OptimizationRecord, RecordStatusEnum
from .gridoptimization import GridOptimizationInput, GridOptimizationRecord
from .torsiondrive import TorsionDriveInput, TorsionDriveRecord

AllRecordTypes = Union[SinglePointRecord, OptimizationRecord, TorsionDriveRecord, GridOptimizationRecord]

### Utility functions

__rest_models = {}


def register_model(name: str, rest: str, body: ProtoModel, response: ProtoModel) -> None:
    """
    Registers a new REST model.

    Parameters
    ----------
    name : str
        A regular expression describing the rest endpoint.
    rest : str
        The REST endpoint type.
    body : ProtoModel
        The REST query body model.
    response : ProtoModel
        The REST query response model.

    """
    rest = rest.upper()

    if (name in __rest_models) and (rest in __rest_models[name]):
        raise KeyError(f"Model name {name} already registered.")

    if name not in __rest_models:
        __rest_models[name] = {}

    __rest_models[name][rest] = (body, response)


# DD FIXME: if we can remove the need for regex matching at all, would you be simpler, more performant
#           should be possible since REST API is fully specified
@functools.lru_cache(1000, typed=True)
def rest_model(resource: str, rest: str) -> Tuple[ProtoModel, ProtoModel]:
    """
    Acquires a REST Model.

    Parameters
    ----------
    resource : str
        The REST endpoint resource name.
    rest : str
        The method to use on the REST endpoint: GET, POST, PUT, DELETE

    Returns
    -------
    Tuple[ProtoModel, ProtoModel]
        The (body, response) models of the REST request.

    """
    rest = rest.upper()
    matches = []
    # DD: we're doing a regex for each of these???
    # this is probably way slower than just a hash mapping (e.g. dict)
    # apparently due to View support; if we can separate View support from FractalServer
    # (make it it's own thing entirely), we can improve performance and reliability on FractalServer
    for model_re in __rest_models.keys():
        if re.fullmatch(model_re, resource):
            try:
                matches.append(__rest_models[model_re][rest])
            except KeyError:
                pass  # Could have different regexes for different endpoint types

    if len(matches) == 0:
        raise KeyError(f"REST Model for endpoint {resource} could not be found.")

    if len(matches) > 1:
        warnings.warn(
            f"Multiple REST models were matched for {rest} request at endpoint {resource}. "
            f"The following models will be used: {matches[0][0]}, {matches[0][1]}.",
            RuntimeWarning,
        )

    return matches[0]


### Generic Types and Common Models

nullstr = constr(regex="null")

QueryStr = Optional[Union[List[str], str]]
QueryInt = Optional[Union[List[int], int]]
QueryObjectId = Optional[Union[List[ObjectId], ObjectId]]
QueryNullObjectId = Optional[Union[List[ObjectId], ObjectId, List[nullstr], nullstr]]
QueryListStr = Optional[List[str]]


class EmptyMeta(ProtoModel):
    """
    There is no metadata accepted, so an empty metadata is sent for completion.
    """


class ResponseMeta(ProtoModel):
    """
    Standard Fractal Server response metadata
    """

    errors: List[Tuple[str, str]] = Field(
        ..., description="A list of error pairs in the form of [(error type, error message), ...]"
    )
    success: bool = Field(
        ...,
        description="Indicates if the passed information was successful in its duties. This is contextual to the "
        "data being passed in.",
    )
    error_description: Union[str, bool] = Field(
        ...,
        description="Details about the error if ``success`` is ``False``, otherwise this is ``False`` in the event "
        "of no errors.",
    )


class ResponseGETMeta(ResponseMeta):
    """
    Standard Fractal Server response metadata for GET/fetch type requests.
    """

    missing: List[str] = Field(..., description="The Id's of the objects which were not found in the database.")
    n_found: int = Field(
        ...,
        description="The number of entries which were already found in the database from the set which was provided.",
    )


class ResponsePOSTMeta(ResponseMeta):
    """
    Standard Fractal Server response metadata for POST/add type requests.
    """

    n_inserted: int = Field(
        ...,
        description="The number of new objects amongst the inputs which did not exist already, and are now in the "
        "database.",
    )
    duplicates: Union[List[str], List[Tuple[str, str]]] = Field(
        ...,
        description="The Ids of the objects which already exist in the database amongst the set which were passed in.",
    )
    validation_errors: List[str] = Field(
        ..., description="All errors with validating submitted objects will be documented here."
    )


class QueryMeta(ProtoModel):
    """
    Standard Fractal Server metadata for Database queries containing pagination information
    """

    limit: Optional[int] = Field(
        None, description="Limit to the number of objects which can be returned with this query."
    )
    skip: int = Field(0, description="The number of records to skip on the query.")


class QueryFilter(ProtoModel):
    """
    Standard Fractal Server metadata for column filtering
    """

    include: QueryListStr = Field(
        None,
        description="Return only these columns. Expert-level object. Only one of include and exclude may be specified.",
    )
    exclude: QueryListStr = Field(
        None,
        description="Return all but these columns. Expert-level object. Only one of include and exclude may be specified.",
    )

    @root_validator
    def check_include_or_exclude(cls, values):
        include = values.get("include")
        exclude = values.get("exclude")
        if (include is not None) and (exclude is not None):
            raise ValueError("Only one of include and exclude may be specified.")
        return values


class QueryMetaFilter(QueryMeta, QueryFilter):
    """
    Fractal Server metadata for Database queries allowing for filtering and pagination
    """


class ComputeResponse(ProtoModel):
    """
    The response model from the Fractal Server when new Compute or Services are added.
    """

    ids: List[Optional[ObjectId]] = Field(..., description="The Id's of the records to be computed.")
    submitted: List[ObjectId] = Field(
        ..., description="The object Ids which were submitted as new entries to the database."
    )
    existing: List[ObjectId] = Field(..., description="The list of object Ids which already existed in the database.")

    def __str__(self) -> str:
        return f"ComputeResponse(nsubmitted={len(self.submitted)} nexisting={len(self.existing)})"

    def __repr__(self) -> str:
        return f"<{self}>"

    def merge(self, other: "ComputeResponse") -> "ComputeResponse":
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
            existing=(self.existing + other.existing),
        )


common_docs = {
    EmptyMeta: str(get_base_docs(EmptyMeta)),
    ResponseMeta: str(get_base_docs(ResponseMeta)),
    ResponseGETMeta: str(get_base_docs(ResponseGETMeta)),
    ResponsePOSTMeta: str(get_base_docs(ResponsePOSTMeta)),
    QueryMeta: str(get_base_docs(QueryMeta)),
    QueryMetaFilter: str(get_base_docs(QueryMetaFilter)),
    ComputeResponse: str(get_base_docs(ComputeResponse)),
}

### Information


class InformationGETBody(ProtoModel):
    pass


class InformationGETResponse(ProtoModel):
    class Config(ProtoModel.Config):
        extra = "allow"


register_model("information", "GET", InformationGETBody, InformationGETResponse)

### Collections


class CollectionGETBody(ProtoModel):
    class Data(ProtoModel):
        collection: str = Field(
            None, description="The specific collection to look up as its identified in the database."
        )
        name: str = Field(None, description="The common name of the collection to look up.")

        @validator("collection")
        def cast_to_lower(cls, v):
            if v:
                v = v.lower()
            return v

    meta: QueryFilter = Field(
        None,
        description="Additional metadata to make with the query. Collections can only have an ``include/exclude`` key in its "
        "meta and therefore does not follow the standard GET metadata model.",
    )
    data: Data = Field(..., description="Information about the Collection to search the database with.")


class CollectionGETResponse(ProtoModel):
    meta: ResponseGETMeta = Field(..., description=common_docs[ResponseGETMeta])
    data: List[Dict[str, Optional[Any]]] = Field(
        ..., description="The Collection objects returned by the server based on the query."
    )

    @validator("data")
    def ensure_collection_name_in_data_get_res(cls, v):
        for col in v:
            if "name" not in col or "collection" not in col:
                raise ValueError("Dicts in 'data' must have both 'collection' and 'name'")
        return v


register_model("collection", "GET", CollectionGETBody, CollectionGETResponse)


class CollectionPOSTBody(ProtoModel):
    class Meta(ProtoModel):
        overwrite: bool = Field(
            False,
            description="The existing Collection in the database will be updated if this is True, otherwise will "
            "remain unmodified if it already exists.",
        )

    class Data(ProtoModel):
        id: str = Field(
            "local",  # Auto blocks overwriting in a socket
            description="The Id of the object to assign in the database. If 'local', then it will not overwrite "
            "existing keys. There should be very little reason to ever touch this.",
        )
        collection: str = Field(
            ..., description="The specific identifier for this Collection as it will appear in database."
        )
        name: str = Field(..., description="The common name of this Collection.")

        class Config(ProtoModel.Config):
            extra = "allow"

        @validator("collection")
        def cast_to_lower(cls, v):
            return v.lower()

    meta: Meta = Field(
        Meta(),
        description="Metadata to specify how the Database should handle adding this Collection if it already exists. "
        "Metadata model for adding Collections can only accept ``overwrite`` as a key to choose to update "
        "existing Collections or not.",
    )
    data: Data = Field(..., description="The data associated with this Collection to add to the database.")


class CollectionPOSTResponse(ProtoModel):
    data: Union[str, None] = Field(
        ...,
        description="The Id of the Collection uniquely pointing to it in the Database. If the Collection was not added "
        "(e.g. ``overwrite=False`` for existing Collection), then a None is returned.",
    )
    meta: ResponsePOSTMeta = Field(..., description=common_docs[ResponsePOSTMeta])


register_model("collection", "POST", CollectionPOSTBody, CollectionPOSTResponse)


class CollectionDELETEBody(ProtoModel):
    meta: EmptyMeta


class CollectionDELETEResponse(ProtoModel):
    meta: ResponseMeta


register_model("collection/[0-9]+", "DELETE", CollectionDELETEBody, CollectionDELETEResponse)

### Collection views


class CollectionSubresourceGETResponseMeta(ResponseMeta):
    """
    Response metadata for collection views functions.
    """

    msgpacked_cols: List[str] = Field(..., description="Names of columns which were serialized to msgpack-ext.")


class CollectionEntryGETBody(ProtoModel):
    class Data(ProtoModel):
        subset: QueryStr = Field(
            None,
            description="Not implemented. " "See qcfractal.interface.collections.dataset_view.DatasetView.get_entries",
        )

    meta: EmptyMeta = Field(EmptyMeta(), description=common_docs[EmptyMeta])
    data: Data = Field(..., description="Information about which entries to return.")


class CollectionEntryGETResponse(ProtoModel):
    meta: CollectionSubresourceGETResponseMeta = Field(
        ..., description=str(get_base_docs(CollectionSubresourceGETResponseMeta))
    )
    data: Optional[bytes] = Field(..., description="Feather-serialized bytes representing a pandas DataFrame.")


register_model("collection/[0-9]+/entry", "GET", CollectionEntryGETBody, CollectionEntryGETResponse)


class CollectionMoleculeGETBody(ProtoModel):
    class Data(ProtoModel):
        indexes: List[int] = Field(
            None,
            description="List of molecule indexes to return (returned by get_entries). "
            "See qcfractal.interface.collections.dataset_view.DatasetView.get_molecules",
        )

    meta: EmptyMeta = Field(EmptyMeta(), description=common_docs[EmptyMeta])
    data: Data = Field(..., description="Information about which molecules to return.")


class CollectionMoleculeGETResponse(ProtoModel):
    meta: CollectionSubresourceGETResponseMeta = Field(
        ..., description=str(get_base_docs(CollectionSubresourceGETResponseMeta))
    )
    data: Optional[bytes] = Field(..., description="Feather-serialized bytes representing a pandas DataFrame.")


register_model("collection/[0-9]+/molecule", "GET", CollectionMoleculeGETBody, CollectionMoleculeGETResponse)


class CollectionValueGETBody(ProtoModel):
    class Data(ProtoModel):
        class QueryData(ProtoModel):
            name: str
            driver: str
            native: bool

        queries: List[QueryData] = Field(
            None,
            description="List of queries to match against values columns. "
            "See qcfractal.interface.collections.dataset_view.DatasetView.get_values",
        )
        subset: QueryStr

    meta: EmptyMeta = Field(EmptyMeta(), description=common_docs[EmptyMeta])
    data: Data = Field(..., description="Information about which values to return.")


class CollectionValueGETResponse(ProtoModel):
    class Data(ProtoModel):
        values: bytes = Field(..., description="Feather-serialized bytes representing a pandas DataFrame.")
        units: Dict[str, str] = Field(..., description="Units of value columns.")

    meta: CollectionSubresourceGETResponseMeta = Field(
        ..., description=str(get_base_docs(CollectionSubresourceGETResponseMeta))
    )
    data: Optional[Data] = Field(..., description="Values and units.")


register_model("collection/[0-9]+/value", "GET", CollectionValueGETBody, CollectionValueGETResponse)


class CollectionListGETBody(ProtoModel):
    class Data(ProtoModel):
        pass

    meta: EmptyMeta = Field(EmptyMeta(), description=common_docs[EmptyMeta])
    data: Data = Field(..., description="Empty for now.")


class CollectionListGETResponse(ProtoModel):
    meta: CollectionSubresourceGETResponseMeta = Field(
        ..., description=str(get_base_docs(CollectionSubresourceGETResponseMeta))
    )
    data: Optional[bytes] = Field(..., description="Feather-serialized bytes representing a pandas DataFrame.")


register_model("collection/[0-9]+/list", "GET", CollectionListGETBody, CollectionListGETResponse)

### Result


class ResultGETBody(ProtoModel):
    class Data(ProtoModel):
        id: Optional[List[ObjectId]] = Field(
            None,
            description="The exact Id to fetch from the database. If this is set as a search condition, there is no "
            "reason to set anything else as this will be unique in the database, if it exists.",
        )
        program: Optional[List[constr(to_lower=True)]] = Field(
            None,
            description="Results will be searched to match the quantum chemistry software which carried out the "
            "calculation.",
        )
        molecule: Optional[List[ObjectId]] = Field(
            None, description="Results will be searched to match the Molecule Id which was computed on."
        )
        driver: Optional[List[constr(to_lower=True)]] = Field(
            None,
            description="Results will be searched to match what class of computation was done. "
            "See :class:`DriverEnum` for valid choices and more information.",
        )
        method: Optional[List[constr(to_lower=True)]] = Field(
            None,
            description="Results will be searched to match the quantum chemistry method executed to compute the value.",
        )
        basis: Optional[List[Optional[constr(to_lower=True)]]] = Field(
            None,
            description="Results will be searched to match specified basis sets which were used to compute the values.",
        )
        keywords: Optional[List[ObjectId]] = Field(
            None,
            description="Results will be searched based on which :class:`KeywordSet` was used to run the computation.",
        )
        status: Optional[List[RecordStatusEnum]] = Field(
            [RecordStatusEnum.complete],
            description="Results will be searched based on where they are in the compute pipeline. See the "
            ":class:`RecordStatusEnum` for valid statuses and more information.",
        )

        @validator("basis", each_item=True, pre=True)
        def validate_basis(cls, v):
            if (v is None) or (v == ""):
                v = None
            return v

    meta: QueryMetaFilter = Field(QueryMetaFilter(), description=common_docs[QueryMetaFilter])
    data: Data = Field(
        ..., description="The keys with data to search the database on for individual quantum chemistry computations."
    )


class ResultGETResponse(ProtoModel):
    meta: ResponseGETMeta = Field(..., description=common_docs[ResponseGETMeta])
    data: List[Dict[str, Optional[Any]]] = Field(..., description="Results found from the query.")


register_model("result", "GET", ResultGETBody, ResultGETResponse)

### Procedures


class ProcedureGETBody(ProtoModel):
    class Data(ProtoModel):
        id: Optional[List[ObjectId]] = Field(
            None,
            description="The exact Id to fetch from the database. If this is set as a search condition, there is no "
            "reason to set anything else as this will be unique in the database, if it exists.",
        )
        procedure: Optional[List[constr(to_lower=True)]] = Field(
            None, description="Procedures will be searched based on the name of the procedure."
        )
        status: Optional[List[RecordStatusEnum]] = Field(
            None,
            description="Procedures will be searched based on where they are in the compute pipeline. See the "
            ":class:`RecordStatusEnum` for valid statuses.",
        )

    meta: QueryMetaFilter = Field(QueryMetaFilter(), description=common_docs[QueryMetaFilter])
    data: Data = Field(..., description="The keys with data to search the database on for Procedures.")


class ProcedureGETResponse(ProtoModel):
    meta: ResponseGETMeta = Field(..., description=common_docs[ResponseGETMeta])
    data: List[Dict[str, Any]] = Field(..., description="The list of Procedure specs found based on the query.")


class OptimizationGETBody(ProtoModel):
    class Data(ProtoModel):
        id: Optional[List[ObjectId]] = Field(
            None,
            description="The exact Id to fetch from the database. If this is set as a search condition, there is no "
            "reason to set anything else as this will be unique in the database, if it exists.",
        )
        status: Optional[List[RecordStatusEnum]] = Field(
            [RecordStatusEnum.complete],
            description="Procedures will be searched based on where they are in the compute pipeline. See the "
            ":class:`RecordStatusEnum` for valid statuses.",
        )

    meta: QueryMetaFilter = Field(QueryMetaFilter(), description=common_docs[QueryMetaFilter])
    data: Data = Field(..., description="The keys with data to search the database on for Procedures.")


class OptimizationGETResponse(ProtoModel):
    meta: ResponseGETMeta = Field(..., description=common_docs[ResponseGETMeta])
    data: List[Dict[str, Any]] = Field(..., description="The list of Procedure specs found based on the query.")


register_model("procedure", "GET", ProcedureGETBody, ProcedureGETResponse)
register_model("optimization", "GET", OptimizationGETBody, OptimizationGETResponse)

### Task Queue


class TaskQueueGETBody(ProtoModel):
    class Data(ProtoModel):
        id: Optional[List[ObjectId]] = Field(
            None,
            description="The exact Id to fetch from the database. If this is set as a search condition, there is no "
            "reason to set anything else as this will be unique in the database, if it exists.",
        )
        program: Optional[List[str]] = Field(
            None, description="Tasks will be searched based on the program responsible for executing this task."
        )
        status: Optional[List[RecordStatusEnum]] = Field(
            None,
            description="Tasks will be search based on where they are in the compute pipeline. See the "
            ":class:`RecordStatusEnum` for valid statuses.",
        )
        base_result: Optional[List[ObjectId]] = Field(
            None,
            description="The exact Id of the Result which this Task is linked to. If this is set as a "
            "search condition, there is no reason to set anything else as this will be unique in the "
            "database, if it exists. See also :class:`SinglePointRecord`.",
        )
        tag: Optional[List[str]] = Field(None, description="Tasks will be searched based on their associated tag.")
        manager: Optional[List[str]] = Field(
            None, description="Tasks will be searched based on the manager responsible for executing the task."
        )

    meta: QueryMetaFilter = Field(QueryMetaFilter(), description=common_docs[QueryMetaFilter])
    data: Data = Field(..., description="The keys with data to search the database on for Tasks.")


class TaskQueueGETResponse(ProtoModel):
    meta: ResponseGETMeta = Field(..., description=common_docs[ResponseGETMeta])
    data: Union[List[TaskRecord], List[Dict[str, Any]]] = Field(
        ...,
        description="Tasks found from the query. This is a list of :class:`TaskRecord` in most cases, however, "
        "if a projection was specified in the GET request, then a dict is returned with mappings based "
        "on the projection.",
    )


register_model("task_queue", "GET", TaskQueueGETBody, TaskQueueGETResponse)


class TaskQueuePOSTBody(ProtoModel):
    meta: Union[SingleProcedureSpecification, OptimizationProcedureSpecification]
    data: List[Union[ObjectId, Molecule]] = Field(
        ...,
        description="The list of either Molecule objects or Molecule Id's (those already in the database) to submit as "
        "part of this Task.",
    )


class TaskQueuePOSTResponse(ProtoModel):

    meta: ResponsePOSTMeta = Field(..., description=common_docs[ResponsePOSTMeta])
    data: ComputeResponse = Field(..., description="Data returned from the server from adding a Task.")


register_model("task_queue", "POST", TaskQueuePOSTBody, TaskQueuePOSTResponse)


class TaskQueuePUTBody(ProtoModel):
    class Data(ProtoModel):
        id: Optional[List[ObjectId]] = Field(
            None,
            description="The exact Id to target in database. If this is set as a search condition, there is no "
            "reason to set anything else as this will be unique in the database, if it exists.",
        )
        base_result: Optional[List[ObjectId]] = Field(  # TODO: Validate this description is correct
            None,
            description="The exact Id of a result which this Task is slated to write to. If this is set as a "
            "search condition, there is no reason to set anything else as this will be unique in the "
            "database, if it exists. See also :class:`SinglePointRecord`.",
        )
        new_tag: Optional[str] = Field(
            None,
            description="Change the tag of an existing or regenerated task to be this value",
        )
        new_priority: Optional[PriorityEnum] = Field(
            None,
            description="Change the priority of an existing or regenerated task to this value",
        )

    class Meta(ProtoModel):
        operation: str = Field(..., description="The specific action you are taking as part of this update.")

        @validator("operation")
        def cast_to_lower(cls, v):
            return v.lower()

    meta: Meta = Field(..., description="The instructions to pass to the target Task from ``data``.")
    data: Data = Field(..., description="The information which contains the Task target in the database.")


class TaskQueuePUTResponse(ProtoModel):
    class Data(ProtoModel):
        n_updated: int = Field(..., description="The number of tasks which were changed.")

    meta: ResponseMeta = Field(..., description=common_docs[ResponseMeta])
    data: Data = Field(..., description="Information returned from attempting updates of Tasks.")


register_model("task_queue", "PUT", TaskQueuePUTBody, TaskQueuePUTResponse)

### Service Queue


class ServiceQueueGETBody(ProtoModel):
    class Data(ProtoModel):
        id: Optional[List[ObjectId]] = Field(
            None,
            description="The exact Id to fetch from the database. If this is set as a search condition, there is no "
            "reason to set anything else as this will be unique in the database, if it exists.",
        )
        procedure_id: Optional[List[ObjectId]] = Field(
            None,
            description="The exact Id of the Procedure this Service is responsible for executing. If this is set as a "
            "search condition, there is no reason to set anything else as this will be unique in the "
            "database, if it exists.",
        )
        status: Optional[List[RecordStatusEnum]] = Field(
            None,
            description="Services are searched based on where they are in the compute pipeline. See the "
            ":class:`RecordStatusEnum` for valid statuses.",
        )

    meta: QueryMeta = Field(QueryMeta(), description=common_docs[QueryMeta])
    data: Data = Field(..., description="The keys with data to search the database on for Services.")


class ServiceQueueGETResponse(ProtoModel):
    meta: ResponseGETMeta = Field(..., description=common_docs[ResponseGETMeta])
    data: List[Dict[str, Optional[Any]]] = Field(
        ..., description="The return of Services found in the database mapping their Ids to the Service spec."
    )


register_model("service_queue", "GET", ServiceQueueGETBody, ServiceQueueGETResponse)


class ServiceQueuePOSTBody(ProtoModel):
    class Meta(ProtoModel):
        tag: Optional[str] = Field(
            None,
            description="Tag to assign to the Tasks this Service will generate so that Queue Managers can pull only "
            "Tasks based on this entry. If no Tag is specified, any Queue Manager can pull this Tasks "
            "created by this Service.",
        )
        priority: PriorityEnum = Field(
            PriorityEnum.normal,
            description="Priority given to this Tasks created by this Service. Higher priority will be pulled first.",
        )

    meta: Meta = Field(
        ...,
        description="Metadata information for the Service for the Tag and Priority of Tasks this Service will create.",
    )
    data: List[Union[TorsionDriveInput, GridOptimizationInput]] = Field(
        ..., description="A list the specification for Procedures this Service will manage and generate Tasks for."
    )


class ServiceQueuePOSTResponse(ProtoModel):

    meta: ResponsePOSTMeta = Field(..., description=common_docs[ResponsePOSTMeta])
    data: ComputeResponse = Field(..., description="Data returned from the server from adding a Service.")


register_model("service_queue", "POST", ServiceQueuePOSTBody, ServiceQueuePOSTResponse)


class ServiceQueuePUTBody(ProtoModel):
    class Data(ProtoModel):
        id: QueryObjectId = Field(None, description="The Id of the Service.")
        procedure_id: QueryObjectId = Field(None, description="The Id of the Procedure that the Service is linked to.")

    class Meta(ProtoModel):
        operation: str = Field(..., description="The update action to perform.")

        @validator("operation")
        def cast_to_lower(cls, v):
            return v.lower()

    meta: Meta = Field(..., description="The instructions to pass to the targeted Service.")
    data: Data = Field(..., description="The information which contains the Service target in the database.")


class ServiceQueuePUTResponse(ProtoModel):
    class Data(ProtoModel):
        n_updated: int = Field(..., description="The number of services which were changed.")

    meta: ResponseMeta = Field(..., description=common_docs[ResponseMeta])
    data: Data = Field(..., description="Information returned from attempting updates of Services.")


register_model("service_queue", "PUT", ServiceQueuePUTBody, ServiceQueuePUTResponse)

### Queue Manager


class QueueManagerMeta(ProtoModel):
    """
    Validation and identification Meta information for the Queue Manager's communication with the Fractal Server.
    """

    # Name data
    cluster: str = Field(..., description="The Name of the Cluster the Queue Manager is running on.")
    hostname: str = Field(..., description="Hostname of the machine the Queue Manager is running on.")
    uuid: str = Field(..., description="A UUID assigned to the QueueManager to uniquely identify it.")

    # Username
    username: Optional[str] = Field(None, description="Fractal Username the Manager is being executed under.")

    # Version info
    qcengine_version: str = Field(..., description="Version of QCEngine which the Manager has access to.")
    manager_version: str = Field(
        ..., description="Version of the QueueManager (Fractal) which is getting and returning Jobs."
    )

    # search info
    programs: Dict[str, Optional[str]] = Field(
        ...,
        description="A list of programs which the QueueManager, and thus QCEngine, has access to. Affects which Tasks "
        "the Manager can pull.",
    )
    tag: Optional[List[str]] = Field(
        None,
        description="Optional queue tag to pull Tasks from. If None, tasks are pulled from all tags. "
        "If a list of tags is provided, tasks are pulled in order of tags. (This does not "
        "guarantee tasks will be executed in that order, however.)",
    )

    # Statistics
    total_worker_walltime: Optional[float] = Field(None, description="The total worker walltime in core-hours.")
    total_task_walltime: Optional[float] = Field(None, description="The total task walltime in core-hours.")
    active_tasks: Optional[int] = Field(None, description="The total number of active running tasks.")
    active_cores: Optional[int] = Field(None, description="The total number of active cores.")
    active_memory: Optional[float] = Field(None, description="The total amount of active memory in GB.")


# Add the new QueueManagerMeta to the docs
common_docs[QueueManagerMeta] = str(get_base_docs(QueueManagerMeta))


# TODO - badly named. THis is for pulling tasks
class QueueManagerGETBody(ProtoModel):
    class Data(ProtoModel):
        limit: int = Field(..., description="Max number of Queue Managers to get from the server.")

    meta: QueueManagerMeta = Field(..., description=common_docs[QueueManagerMeta])
    data: Data = Field(
        ...,
        description="A model of Task request data for the Queue Manager to fetch. Accepts ``limit`` as the maximum "
        "number of tasks to pull.",
    )


class QueueManagerGETResponse(ProtoModel):
    meta: ResponseGETMeta = Field(..., description=common_docs[ResponseGETMeta])
    data: List[Dict[str, Optional[Any]]] = Field(
        ..., description="A list of tasks retrieved from the server to compute."
    )


register_model("queue_manager", "GET", QueueManagerGETBody, QueueManagerGETResponse)


class QueueManagerPOSTBody(ProtoModel):
    meta: QueueManagerMeta = Field(..., description=common_docs[QueueManagerMeta])
    data: Dict[ObjectId, AllResultTypes] = Field(..., description="A Dictionary of tasks to return to the server.")


class QueueManagerPOSTResponse(ProtoModel):
    meta: ResponsePOSTMeta = Field(..., description=common_docs[ResponsePOSTMeta])
    data: bool = Field(..., description="A True/False return on if the server accepted the returned tasks.")


register_model("queue_manager", "POST", QueueManagerPOSTBody, QueueManagerPOSTResponse)


class QueueManagerPUTBody(ProtoModel):
    class Data(ProtoModel):
        operation: str
        configuration: Optional[Dict[str, Any]] = None

    meta: QueueManagerMeta = Field(..., description=common_docs[QueueManagerMeta])
    data: Data = Field(
        ...,
        description="The update action which the Queue Manager requests the Server take with respect to how the "
        "Queue Manager is tracked.",
    )


class QueueManagerPUTResponse(ProtoModel):
    meta: Dict[str, Any] = Field({}, description=common_docs[EmptyMeta])
    # Order on Union[] is important. Union[bool, Dict[str, int]] -> True if the input dict is not empty since
    # Python can resolve dict -> bool since it passes a `is` test. Will not cast bool -> dict[str, int], so make Dict[]
    # check first
    data: Union[Dict[str, int], bool] = Field(
        ...,
        description="The response from the Server attempting to update the Queue Manager's server-side status. "
        "Response type is a function of the operation made from the PUT request.",
    )


register_model("queue_manager", "PUT", QueueManagerPUTBody, QueueManagerPUTResponse)


class ManagerInfoGETBody(ProtoModel):
    class Data(ProtoModel):
        name: Optional[List[str]] = Field(None, description="Name(s) of managers to query for.")
        status: Optional[List[ManagerStatusEnum]] = Field(
            None,
            description="Managers will be searched based on status. See :class:`ManagerStatusEnum` for valid statuses.",
        )

    meta: QueryMeta = Field(QueryMeta(), description=common_docs[QueryMeta])
    data: Data = Field(..., description="The keys with data to search the database on for Managers.")


class ManagerInfoGETResponse(ProtoModel):
    meta: ResponseGETMeta = Field(..., description=common_docs[ResponseGETMeta])
    data: List[Dict[str, Any]] = Field(..., description="Information about the requested managers")


register_model(r"manager", "GET", ManagerInfoGETBody, ManagerInfoGETResponse)


class AccessLogGETBody(ProtoModel):
    class Data(ProtoModel):
        access_type: Optional[List[str]] = Field(None, description="Access types/endpoints to query for")
        access_method: Optional[List[str]] = Field(None, description="Access methods (GET, POST) to query for")
        after: Optional[datetime] = Field(None, description="Query for records after this date")
        before: Optional[datetime] = Field(None, description="Query for records before this date")

    meta: QueryMeta = Field(QueryMeta(), description=common_docs[QueryMeta])
    data: Data = Field(..., description="Search parameters for the access log.")


class AccessLogGETResponse(ProtoModel):
    meta: ResponseGETMeta = Field(..., description=common_docs[ResponseGETMeta])
    data: List[Dict[str, Any]] = Field(..., description="Individual entries from the access log")


class ServerStatsGETBody(ProtoModel):
    class Data(ProtoModel):
        after: Optional[datetime] = Field(None, description="Query for records after this date")
        before: Optional[datetime] = Field(None, description="Query for records before this date")

    meta: QueryMeta = Field(QueryMeta(), description=common_docs[QueryMeta])
    data: Data = Field(..., description="Search parameters for the server stats.")


class ServerStatsGETResponse(ProtoModel):
    meta: ResponseGETMeta = Field(..., description=common_docs[ResponseGETMeta])
    data: List[Dict[str, Any]] = Field(..., description="Individual entries from server stats table")


class InternalErrorLogGETBody(ProtoModel):
    class Data(ProtoModel):
        id: Optional[List[ObjectId]] = Field(None, description="Query for errors with these ids")
        user: Optional[List[str]] = Field(None, description="Query for errors belonging to this user")
        after: Optional[datetime] = Field(None, description="Query for records after this date")
        before: Optional[datetime] = Field(None, description="Query for records before this date")

    meta: QueryMeta = Field(QueryMeta(), description=common_docs[QueryMeta])
    data: Data = Field(..., description="Search parameters for the access log.")


class InternalErrorLogGETResponse(ProtoModel):
    meta: ResponseGETMeta = Field(..., description=common_docs[ResponseGETMeta])
    data: List[Dict[str, Any]] = Field(..., description="Individual entries from the error log")


register_model(r"server_stats", "GET", ServerStatsGETBody, ServerStatsGETResponse)
register_model(r"access/log", "GET", AccessLogGETBody, AccessLogGETResponse)
register_model(r"error", "GET", InternalErrorLogGETBody, InternalErrorLogGETResponse)


class GroupByEnum(str, Enum):
    user = "user"
    day = "day"
    hour = "hour"
    country = "country"
    subdivision = "subdivision"


class AccessSummaryGETBody(ProtoModel):
    class Data(ProtoModel):
        group_by: GroupByEnum = Field(GroupByEnum.day, descriptoin="How to group the log summaries")
        after: Optional[datetime] = Field(None, description="Query for records after this date")
        before: Optional[datetime] = Field(None, description="Query for records before this date")

    meta: EmptyMeta = Field(EmptyMeta(), description=common_docs[EmptyMeta])
    data: Data = Field(..., description="Search parameters for the access log")


class AccessSummaryGETResponse(ProtoModel):
    meta: EmptyMeta = Field(EmptyMeta(), description=common_docs[EmptyMeta])
    data: Dict[str, Any] = Field({}, description="A summary of accesses in the access logs")


register_model(r"access/summary", "GET", AccessSummaryGETBody, AccessSummaryGETResponse)
