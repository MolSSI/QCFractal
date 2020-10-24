"""
Utilities and base functions for Services.
"""

import abc
import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import validator
from qcelemental.models import ComputeError

from ..interface.models import ObjectId, ProtoModel
from ..interface.models.rest_models import TaskQueuePOSTBody
from ..interface.models.task_models import PriorityEnum
from ..procedures import get_procedure_parser


class TaskManager(ProtoModel):

    storage_socket: Optional[Any] = None
    logger: Optional[Any] = None

    required_tasks: Dict[str, str] = {}
    tag: Optional[str] = None
    priority: PriorityEnum = PriorityEnum.HIGH

    class Config(ProtoModel.Config):
        allow_mutation = True
        serialize_default_excludes = {"storage_socket", "logger"}

    def done(self) -> bool:
        """
        Check if requested tasks are complete.
        """

        if len(self.required_tasks) == 0:
            return True

        task_query = self.storage_socket.get_procedures(
            id=list(self.required_tasks.values()), include=["status", "error"]
        )

        status_values = set(x["status"] for x in task_query["data"])
        if status_values == {"COMPLETE"}:
            return True

        elif "ERROR" in status_values:
            for x in task_query["data"]:
                if x["status"] != "ERROR":
                    continue

            self.logger.debug("Error in service compute as follows:")
            tasks = self.storage_socket.get_queue()["data"]
            for x in tasks:
                if "error" not in x:
                    continue

                self.logger.debug(x["error"]["error_message"])

            raise KeyError("All tasks did not execute successfully.")
        else:
            return False

    def get_tasks(self) -> Dict[str, Any]:
        """
        Pulls currently held tasks.
        """

        ret = {}
        for k, id in self.required_tasks.items():
            ret[k] = self.storage_socket.get_procedures(id=id)["data"][0]

        return ret

    def submit_tasks(self, procedure_type: str, tasks: Dict[str, Any]) -> bool:
        """
        Submits new tasks to the queue and provides a waiter until there are done.
        """
        procedure_parser = get_procedure_parser(procedure_type, self.storage_socket, self.logger)

        required_tasks = {}

        # Add in all new tasks
        for key, packet in tasks.items():
            packet["meta"].update({"tag": self.tag, "priority": self.priority})
            # print("Check tag and priority:", packet)
            packet = TaskQueuePOSTBody(**packet)

            # Turn packet into a full task, if there are duplicates, get the ID
            r = procedure_parser.submit_tasks(packet)

            if len(r["meta"]["errors"]):
                raise KeyError("Problem submitting task: {}.".format(errors))

            # print("Submission:", r["data"])
            required_tasks[key] = r["data"]["ids"][0]

        self.required_tasks = required_tasks

        return True


class BaseService(ProtoModel, abc.ABC):

    # Excluded fields
    storage_socket: Optional[Any]
    logger: Optional[Any]

    # Base identification
    id: Optional[ObjectId] = None
    hash_index: str
    service: str
    program: str
    procedure: str

    # Output data
    output: Any

    # Links
    task_id: Optional[ObjectId] = None
    procedure_id: Optional[ObjectId] = None

    # Task manager
    task_tag: Optional[str] = None
    task_priority: PriorityEnum
    task_manager: TaskManager = TaskManager()

    status: str = "WAITING"
    error: Optional[ComputeError] = None
    stdout: str = ""
    tag: Optional[str] = None

    # Sorting and priority
    priority: PriorityEnum = PriorityEnum.NORMAL
    modified_on: datetime.datetime = None
    created_on: datetime.datetime = None

    class Config(ProtoModel.Config):
        allow_mutation = True
        serialize_default_excludes = {"storage_socket", "logger"}

    def __init__(self, **data):

        dt = datetime.datetime.utcnow()
        data.setdefault("modified_on", dt)
        data.setdefault("created_on", dt)

        super().__init__(**data)
        self.task_manager.logger = self.logger
        self.task_manager.storage_socket = self.storage_socket
        self.task_manager.tag = self.task_tag
        self.task_manager.priority = self.task_priority

    @validator("task_priority", pre=True)
    def munge_priority(cls, v):
        if isinstance(v, str):
            v = PriorityEnum[v.upper()]
        elif v is None:
            v = PriorityEnum.HIGH
        return v

    @classmethod
    @abc.abstractmethod
    def initialize_from_api(cls, storage_socket, meta, molecule, tag=None, priority=None):
        """
        Initalizes a Service from the API.
        """

    @abc.abstractmethod
    def iterate(self):
        """
        Takes a "step" of the service. Should return False if not finished.
        """


def expand_ndimensional_grid(
    dimensions: Tuple[int, ...], seeds: Set[Tuple[int, ...]], complete: Set[Tuple[int, ...]]
) -> List[Tuple[Tuple[int, ...], Tuple[int, ...]]]:
    """
    Expands an n-dimensional key/value grid.

    Example
    -------
    >>> expand_ndimensional_grid((3, 3), {(1, 1)}, set())
    [((1, 1), (0, 1)), ((1, 1), (2, 1)), ((1, 1), (1, 0)), ((1, 1), (1, 2))]
    """

    dimensions = tuple(dimensions)
    compute = set()
    connections = []

    for d in range(len(dimensions)):

        # Loop over all compute seeds
        for seed in seeds:

            # Iterate both directions
            for disp in [-1, 1]:
                new_dim = seed[d] + disp

                # Bound check
                if new_dim >= dimensions[d]:
                    continue
                if new_dim < 0:
                    continue

                new = list(seed)
                new[d] = new_dim
                new = tuple(new)

                # Push out duplicates from both new compute and copmlete
                if new in compute:
                    continue
                if new in complete:
                    continue

                compute |= {new}
                connections.append((seed, new))

    return connections
