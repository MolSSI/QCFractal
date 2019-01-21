"""
Queue adapter for Parsl
"""

import abc
import importlib
import logging
import operator
from typing import Any, Dict, Optional, List, Callable


class BaseAdapter(abc.ABC):
    """A Adapter for Parsl
    """

    def __init__(self, client: Any, logger: Optional[logging.Logger] = None):
        """
        Parameters
        ----------
        client : parsl.config.Config
            A activate Parsl DataFlow
        logger : None, optional
            A optional logging object to write output to
        """
        self.client = client
        self.logger = logger or logging.getLogger(self.__class__.__name__)

        self.queue = {}
        self.function_map = {}

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

    @abc.abstractmethod
    def submit_tasks(self, tasks: Dict[str, Any]) -> List[str]:
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
        pass

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
        pass
