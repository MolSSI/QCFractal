"""
Utilities and base functions for Services.
"""

import abc
import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from qcelemental.models import ComputeError

from ..interface.models import (
    ObjectId,
    ProtoModel,
    PriorityEnum,
    RecordStatusEnum,
    AllProcedureSpecifications,
    Molecule,
)


class TaskManager(ProtoModel):

    storage_socket: Any

    required_tasks: Dict[str, str] = {}
    tag: Optional[str] = None
    priority: PriorityEnum = PriorityEnum.normal

    class Config(ProtoModel.Config):
        allow_mutation = True
        serialize_default_excludes = {"storage_socket"}

    def done(self) -> bool:
        """
        Check if requested tasks are complete.
        """

        if len(self.required_tasks) == 0:
            return True

        task_query = self.storage_socket.procedure.get(
            id=list(self.required_tasks.values()), include=["status", "error"]
        )

        status_values = set(x["status"] for x in task_query)
        if status_values == {RecordStatusEnum.complete}:
            return True

        elif RecordStatusEnum.error in status_values:
            raise RuntimeError("All tasks did not execute successfully.")
        else:
            return False

    def get_tasks(self) -> Dict[str, Any]:
        """
        Pulls currently held tasks.
        """

        ret = {}
        for k, id in self.required_tasks.items():
            ret[k] = self.storage_socket.procedure.get(id=[id])[0]

        return ret

    def submit_tasks(self, task_inputs: Dict[str, Tuple[Molecule, AllProcedureSpecifications]]):
        """
        Submits new tasks to the queue and provides a waiter until there are done.
        """

        required_tasks = {}

        # Add in all new tasks
        for key, (molecule, spec) in task_inputs.items():

            meta, added_ids = self.storage_socket.procedure.create([molecule], spec)

            if not meta.success:
                raise RuntimeError("Problem submitting task: {}.".format(meta.error_string))

            # print("Submission:", r["data"])
            required_tasks[key] = added_ids[0]

        self.required_tasks = required_tasks


class BaseService(ProtoModel, abc.ABC):

    # Excluded fields
    storage_socket: Optional[Any]

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

    status: str = RecordStatusEnum.waiting
    error: Optional[ComputeError] = None
    stdout: str = ""
    tag: Optional[str] = None

    # Sorting and priority
    priority: PriorityEnum = PriorityEnum.normal
    modified_on: datetime.datetime = None
    created_on: datetime.datetime = None

    class Config(ProtoModel.Config):
        allow_mutation = True
        serialize_default_excludes = {"storage_socket"}

    def __init__(self, **data):
        dt = datetime.datetime.utcnow()
        data.setdefault("modified_on", dt)
        data.setdefault("created_on", dt)

        super().__init__(**data)
        self.task_manager.storage_socket = self.storage_socket
        self.task_manager.tag = self.task_tag
        self.task_manager.priority = self.task_priority

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
