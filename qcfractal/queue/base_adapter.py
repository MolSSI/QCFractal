"""
A BaseAdapter for wrapping compute engines.
"""

import abc
import importlib
import logging
import operator
from typing import Any, Callable, Dict, Hashable, List, Optional, Tuple


class BaseAdapter(abc.ABC):
    """A BaseAdapter for wrapping compute engines"""

    def __init__(
        self,
        client: Any,
        logger: Optional[logging.Logger] = None,
        cores_per_task: Optional[int] = None,
        memory_per_task: Optional[float] = None,
        scratch_directory: Optional[str] = None,
        cores_per_rank: Optional[int] = 1,
        retries: Optional[int] = 2,
        verbose: bool = False,
        nodes_per_task: int = 1,
        **kwargs,
    ):
        """
        Parameters
        ----------
        client : object
            A object wrapper for different distributed workflow types. The following input types are valid
             - Python Processes: "concurrent.futures.process.ProcessPoolExecutor"
             - Dask Distributed: "distributed.Client"
             - Fireworks: "fireworks.LaunchPad"
             - Parsl: "parsl.config.Config"
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
        scratch_directory: str, optional, Default: None
            Location of the scratch directory to compute QCEngine tasks in
            It is up to the specific Adapter implementation to handle this option
        retries : int, optional, Default: 2
            Number of retries that QCEngine will attempt for RandomErrors detected when running
            its computations. After this many attempts (or on any other type of error), the
            error will be raised.
        nodes_per_task : int, optional, Default:  1
            Number of nodes to allocate per task. Default is to use a single node per task
        cores_per_rank: Optional[int], optional
            How many CPUs per rank of an MPI application. Used only for node-parallel tasks
        verbose: bool, Default: True
            Increase verbosity of the logger
        """
        self.client = client
        self.logger = logger or logging.getLogger(self.__class__.__name__)

        self.queue = {}
        self.function_map = {}
        self.cores_per_task = cores_per_task
        self.memory_per_task = memory_per_task
        self.nodes_per_task = nodes_per_task
        self.scratch_directory = scratch_directory
        self.cores_per_rank = cores_per_rank
        self.retries = retries
        self.verbose = verbose
        if self.verbose:
            self.logger.setLevel("DEBUG")

    def __repr__(self) -> str:
        return "<BaseAdapter>"

    def get_function(self, function: str) -> Callable:
        """Obtains a Python function from a given string.

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
        Helper property to return the local QCEngine Options based on number of cores and memory per task.

        Individual adapters can overload this behavior.

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
        if self.scratch_directory is not None:
            local_options["scratch_directory"] = self.scratch_directory
        if self.retries is not None:
            local_options["retries"] = self.retries
        if self.nodes_per_task is not None:
            local_options["nnodes"] = self.nodes_per_task
        if self.cores_per_rank is not None:
            local_options["cores_per_rank"] = self.cores_per_rank
        return local_options

    def submit_tasks(self, tasks: List[Dict[str, Any]]) -> List[str]:
        """Adds tasks to the queue.

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
                task_spec["spec"]["kwargs"] = {
                    **task_spec["spec"]["kwargs"],
                    **{"local_options": self.qcengine_local_options},
                }

            queue_key, task = self._submit_task(task_spec)
            self.logger.debug(f"Submitted Task:\n{task_spec}\n")

            self.queue[queue_key] = task
            # self.logger.info("Adapter: Task submitted {}".format(tag))
            ret.append(tag)
        return ret

    @abc.abstractmethod
    def acquire_complete(self) -> Dict[str, Any]:
        """Pulls complete tasks out of the task queue.

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
        """Returns the tags for all active tasks.

        Returns
        -------
        list of str
            Tags of all activate tasks.
        """
        return list(self.queue.keys())

    def task_count(self) -> int:
        """Counts all active tasks.

        Returns
        -------
        int
            Count of active tasks
        """
        return len(self.queue)

    @abc.abstractmethod
    def close(self) -> bool:
        """Closes down the Client and Adapter objects.

        Returns
        -------
        bool
            True if the closing was successful.
        """

    def count_active_tasks(self) -> int:
        """
        Adapter-specific implementation to count the currently active tasks, helpful for resource consumption.
        May not be implemented or possible for each adapter, nor is it required for
        operation. As such, this it is not required to be implemented as an abstract method.

        Returns
        -------
        int
            Number of active tasks

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError("This adapter has not implemented this method yet")

    def count_active_task_slots(self) -> int:
        """
        Adapter-specific implementation to count the currently available task slots and ignores if they have an active task or not.

        May not be implemented or possible for each adapter, nor is it required for
        operation. As such, this it is not required to be implemented as an abstract method.

        Returns
        -------
        int
            Number of active task slots

        Raises
        ------
        NotImplementedError
        """
        raise NotImplementedError("This adapter has not implemented this method yet")

    @abc.abstractmethod
    def _submit_task(self, task_spec: Dict[str, Any]) -> Tuple[Hashable, Any]:
        """
        Add a specific task to the queue.

        Parameters
        ----------
        task_spec : dict
            Full description of the task in dictionary form

        Returns
        -------
        queue_key : Valid Dictionary Key
            Identifier for the queue to use for lookup of the task
        task
            Submitted task object for the adapter to look up later after it has formatted it
        """

    def _task_exists(self, lookup) -> bool:
        """
        Check if the task exists helper function, adapters may use something different

        Parameters
        ----------
        lookup : key
            Lookup key

        Returns
        -------
        exists : bool

        """
        return lookup in self.queue
