"""
Utilities and base functions for Services.
"""

import abc
import json

from qcfractal.procedures import get_procedure_parser

from typing import Any, Dict, List, Set, Tuple
from pydantic import BaseModel


class BaseService(BaseModel, abc.ABC):

    storage_socket: Any

    # Base information requiered by the class
    id: str = None
    hash_index: str
    status: str
    service: str
    program: str
    procedure: str

    @classmethod
    @abc.abstractmethod
    def initialize_from_api(cls, storage_socket, meta, molecule):
        """
        Initalizes a Service from the API
        """

    def dict(self, include=None, exclude=None, by_alias=False) -> Dict[str, Any]:
        return BaseModel.dict(self, exclude={"storage_socket"})

    def json_dict(self) -> str:
        return json.loads(self.json())

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

        task_query = storage_socket.get_queue(
            id=list(self.required_tasks.values()),
            status=["COMPLETE", "ERROR"],
            projection={"base_result": True,
                        "status": True,
                        "error": True})

        if len(task_query["data"]) != len(self.required_tasks):
            return False

        elif "ERROR" in set(x["status"] for x in task_query["data"]):
            for x in task_query["data"]:
                if x["status"] != "ERROR":
                    continue
                print(x["error"])
                print(x["error"]["error_message"])
            raise KeyError("All tasks did not execute successfully.")

        return True

    def get_tasks(self, storage_socket) -> Dict[str, Any]:
        """
        Pulls currently held tasks
        """

        ret = {}
        for k, task_id in self.required_tasks.items():
            ret[k] = storage_socket.get_procedures_by_task_id(task_id)["data"][0]

        return ret

    def submit_tasks(self, storage_socket, procedure_type: str, tasks: Dict[str, Any]) -> bool:
        """
        Submits new tasks to the queue and provides a waiter until there are done.
        """
        procedure_parser = get_procedure_parser(procedure_type, storage_socket)

        required_tasks = {}

        # Flat map of tasks
        new_task_keys = []
        new_tasks = []

        # Add in all new tasks
        for key, packet in tasks.items():

            # Turn packet into a full task, if there are duplicates, get the ID
            submitted, completed, errors = procedure_parser.parse_input(packet, duplicate_id="id")

            if len(errors):
                raise KeyError("Problem submitting task: {}.".format(errors))
            elif len(completed):
                required_tasks[key] = completed[0]["task_id"]
            else:
                new_task_keys.append(key)
                new_tasks.append(submitted[0])

        # Add tasks to Nanny and map back
        submit = storage_socket.queue_submit(new_tasks)
        if len(submit["meta"]["duplicates"]):
            raise RuntimeError("It appears that one of the tasks you submitted is already in the queue, but was "
                               "not there when the tasks were populated.\n"
                               "This should only happen if someone else submitted a similar or exact task "
                               "was submitted at the same time.\n"
                               "This is a corner case we have not solved yet. Please open a ticket with QCFractal"
                               "describing the conditions which yielded this message.")

        if len(submit["data"]) != len(new_task_keys):
            raise KeyError("Issue submitting new tasks, legnth of submitted and input tasks do not match.")

        for key, task_id in zip(new_task_keys, submit["data"]):
            required_tasks[key] = task_id

        if required_tasks.keys() != tasks.keys():
            raise KeyError("Issue submitting new tasks, submitted and input keys do not match.")

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