"""
Queue adapter for Dask
"""

import importlib
import logging
import traceback
import operator


def _get_future(future):
    if future.exception() is None:
        return future.result()
    else:
        msg = "".join(traceback.format_exception(TypeError, future.exception(), future.traceback()))
        ret = {"success": False, "error": msg}
        return ret


class DaskAdapter:
    """A Adapter for Dask
    """

    def __init__(self, dask_client, logger=None):
        """
        Parameters
        ----------
        dask_client : distributed.Client
            A activte Dask Distributed Client
        logger : None, optional
            A optional logging object to write output to
        """
        self.dask_client = dask_client
        self.queue = {}
        self.function_map = {}

        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('DaskAdapter')

    def get_function(self, function):
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

    def submit_tasks(self, tasks):
        """Adds tasks to the Dask queue

        Parameters
        ----------
        tasks : list of dict
            Canonical Fractal with {"spec: {"function", "args", "kwargs"}} fields.

        Returns
        -------
        list of str
            The tags associated with the submitted jobs.
        """
        ret = []
        for task in tasks:

            tag = task["id"]
            if tag in self.queue:
                continue

            # Form run tuple
            func = self.get_function(task["spec"]["function"])
            job = self.dask_client.submit(func, *task["spec"]["args"], **task["spec"]["kwargs"])

            self.queue[tag] = (job, task["parser"], task["hooks"])
            self.logger.info("Adapter: Task submitted {}".format(tag))
            ret.append(tag)
        return ret

    def aquire_complete(self):
        """Pulls complete tasks out of the Dask queue.

        Returns
        -------
        list of dict
            The JSON structures of complete jobs
        """
        ret = {}
        del_keys = []
        for key, (future, parser, hooks) in self.queue.items():
            if future.done():
                ret[key] = (_get_future(future), parser, hooks)
                del_keys.append(key)

        for key in del_keys:
            del self.queue[key]

        return ret

    def await_results(self):
        """Waits for all jobs to complete before returning

        Returns
        -------
        bool
            True if the opertions was successful.
        """
        from dask.distributed import wait
        futures = [v[0] for k, v in self.queue.items()]
        wait(futures)

        return True

    def list_tasks(self):
        """Returns the tags for all active tasks

        Returns
        -------
        list of str
            Tags of all activate tasks.
        """
        return list(self.queue.keys())

    def task_count(self):
        """Counts all active tasks

        Returns
        -------
        int
            Count of active tasks
        """
        return len(self.queue)
