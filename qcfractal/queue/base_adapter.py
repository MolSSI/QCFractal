"""
Queue adapter for Parsl
"""

import abc
import importlib
import logging
import operator
from typing import Any, Callable, Dict, Hashable, List, Optional, Tuple


class BaseAdapter(abc.ABC):
    """A BaseAdapter for wrapping compute engines
    """

    def __init__(self,
                 client: Any,
                 logger: Optional[logging.Logger] = None,
                 cores_per_task: Optional[int] = None,
                 memory_per_task: Optional[float] = None,
                 **kwargs):
        """
        Parameters
        ----------
        client : parsl.config.Config
            A activate Parsl DataFlow
        logger : None, optional
            A optional logging object to write output to
        cores_per_task : int, optional, Default: None
            How many CPU cores per computation task to allocate for QCEngine
            None indicates "use however many you can detect"
            It is up to the specific Adapter implementation to handle this option
        memory_per_task: int, optional, Default: None
            How much memory, in GiB, per computation task to allocate for QCEngine
            None indicates "use however much you can consume"
            It is up to the specific Adapter implementation to handle this option
        """
        self.client = client
        self.logger = logger or logging.getLogger(self.__class__.__name__)

        self.queue = {}
        self.function_map = {}
        self.cores_per_task = cores_per_task
        self.memory_per_task = memory_per_task

    def __repr__(self) -> str:
        return "<BaseAdapter>"

    def get_function(self, function: str) -> Callable:
        """Obtains a Python function from a given string

        Parameters
        ----------
        function : str
            A full path to a function

        Returns
        -------
        callable
            The desired Python function

        Examples
        --------

        >>> get_function("numpy.einsum")
        <function einsum at 0x110406a60>
        """
        if function in self.function_map:
            return self.function_map[function]

        module_name, func_name = function.split(".", 1)
        module = importlib.import_module(module_name)
        self.function_map[function] = operator.attrgetter(func_name)(module)

        return self.function_map[function]

    @property
    def qcengine_local_options(self) -> Dict[str, Any]:
        """
        Helper property to return the local QCEngine Options based on number of cores and memory per task

        Individual adapters can overload this behavior
        Returns
        -------
        local_options : dict
            Dict of local options
        """
        local_options = {}
        if self.memory_per_task is not None:
            local_options["memory"] = self.memory_per_task
        if self.cores_per_task is not None:
            local_options["ncores"] = self.cores_per_task
        return local_options

    def submit_tasks(self, tasks: List[Dict[str, Any]]) -> List[str]:
        """Adds tasks to the queue

        Parameters
        ----------
        tasks : list of dict
            Canonical Fractal task with {"spec: {"function", "args", "kwargs"}} fields.

        Returns
        -------
        list of str
            The tags associated with the submitted tasks.
        """

        ret = []
        for task_spec in tasks:

            tag = task_spec["id"]
            if self._task_exists(tag):
                continue

            # Trap QCEngine Memory and CPU
            if task_spec["spec"]["function"].startswith("qcengine.compute") and self.qcengine_local_options:
                task_spec = task_spec.copy()  # Copy for safety
                task_spec["spec"]["kwargs"] = {**task_spec["spec"]["kwargs"], **{"local_options": self.qcengine_local_options}}

            queue_key, task = self._submit_task(task_spec)

            self.queue[queue_key] = (task, task_spec["parser"], task_spec["hooks"])
            self.logger.info("Adapter: Task submitted {}".format(tag))
            ret.append(tag)
        return ret

    @abc.abstractmethod
    def acquire_complete(self) -> List[Dict[str, Any]]:
        """Pulls complete tasks out of the Parsl queue.

        Returns
        -------
        list of dict
            The JSON structures of complete tasks
        """

    @abc.abstractmethod
    def await_results(self) -> bool:
        """Waits for all tasks to complete before returning.

        Returns
        -------
        bool
            True if the opertion was successful.
        """

    def list_tasks(self) -> List[str]:
        """Returns the tags for all active tasks

        Returns
        -------
        list of str
            Tags of all activate tasks.
        """
        return list(self.queue.keys())

    def task_count(self) -> int:
        """Counts all active tasks

        Returns
        -------
        int
            Count of active tasks
        """
        return len(self.queue)

    @abc.abstractmethod
    def close(self) -> bool:
        """Closes down the Client and Adapter objects

        Returns
        -------
        bool
            True if the closing was successful.
        """

    @abc.abstractmethod
    def _submit_task(self, task_spec: Dict[str, Any]) -> Tuple[Hashable, Any]:
        """
        Add a specific task to the queue

        Parameters
        ----------
        task_spec : dict
            Full description of the task in dictionary form

        Returns
        -------
        queue_key : Valid Dictionary Key
            Identifier for the queue to use for lookup of the task
        task
            Submitted task object for the adapter to look up later after its formatted it
        """

    def _task_exists(self, lookup) -> bool:
        """
        Check if the tasks exists helper function, adapters may use something different

        Parameters
        ----------
        lookup : key
            Lookup key

        Returns
        -------
        exists : bool

        """
        return lookup in self.queue
