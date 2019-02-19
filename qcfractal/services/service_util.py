"""
Utilities and base functions for Services.
"""

import abc
import json
from typing import Any, Dict, List, Set, Tuple

from pydantic import BaseModel

from ..interface.models.rest_models import TaskQueuePOSTBody
from ..procedures import get_procedure_parser


class BaseService(BaseModel, abc.ABC):

    storage_socket: Any

    # Base information requiered by the class
    id: str = None
    hash_index: str
    status: str
    service: str
    program: str
    procedure: str
    output: Any

    @classmethod
    @abc.abstractmethod
    def initialize_from_api(cls, storage_socket, meta, molecule):
        """
        Initalizes a Service from the API
        """

    def dict(self, *args, **kwargs) -> Dict[str, Any]:
        kwargs["exclude"] = (kwargs.pop("exclude", None) or set()) | {"storage_socket"}
        return BaseModel.dict(self, *args, **kwargs)

    def json_dict(self, *args, **kwargs) -> str:
        return json.loads(self.json(*args, **kwargs))

    @abc.abstractmethod
    def iterate(self):
        """
        Takes a "step" of the service. Should return False if not finished
        """


class TaskManager(BaseModel):

    required_tasks: Dict[str, str] = {}

    def done(self, storage_socket) -> bool:
        """
        Check if requested tasks are complete
        """

        if len(self.required_tasks) == 0:
            return True

        task_query = storage_socket.get_procedures_by_id(
            id=list(self.required_tasks.values()),
            projection={"status": True,
                        "error": True,
                        "hash_index": True})

        if len(task_query["data"]) != len(self.required_tasks):
            return False

        elif "ERROR" in set(x["status"] for x in task_query["data"]):
            for x in task_query["data"]:
                if x["status"] != "ERROR":
                    continue
            tasks = storage_socket.get_queue()["data"]
            for x in tasks:
                if "error" not in x:
                    continue
                print(x["error"])

            raise KeyError("All tasks did not execute successfully.")

        return True

    def get_tasks(self, storage_socket) -> Dict[str, Any]:
        """
        Pulls currently held tasks
        """

        ret = {}
        for k, id in self.required_tasks.items():
            ret[k] = storage_socket.get_procedures_by_id(id=id)["data"][0]

        return ret

    def submit_tasks(self, storage_socket, procedure_type: str, tasks: Dict[str, Any]) -> bool:
        """
        Submits new tasks to the queue and provides a waiter until there are done.
        """
        procedure_parser = get_procedure_parser(procedure_type, storage_socket)

        required_tasks = {}

        # Add in all new tasks
        for key, packet in tasks.items():
            packet = TaskQueuePOSTBody(**packet)

            # Turn packet into a full task, if there are duplicates, get the ID
            r = procedure_parser.submit_tasks(packet)

            if len(r["meta"]["errors"]):
                raise KeyError("Problem submitting task: {}.".format(errors))

            required_tasks[key] = r["data"]["ids"][0]

        self.required_tasks = required_tasks

        return True


def expand_ndimensional_grid(dimensions: Tuple[int, ...], seeds: Set[Tuple[int, ...]],
                             complete: Set[Tuple[int, ...]]) -> List[Tuple[Tuple[int, ...], Tuple[int, ...]]]:
    """
    Expands an n-dimensional key/value grid

    Example:
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
