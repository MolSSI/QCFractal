"""
Queue adapter for Dask
"""

import importlib
import logging
import operator
import traceback


def _get_future(future):
    if future.exception() is None:
        return future.result()
    else:
        msg = "".join(traceback.format_exception(TypeError, future.exception(), future.traceback()))
        ret = {"success": False, "error_message": msg}
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

        self.logger = logger or logging.getLogger('DaskAdapter')

    def __repr__(self):
        return "<DaskAdapter client={}>".format(self.dask_client)

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
            The tags associated with the submitted tasks.
        """
        ret = []
        for spec in tasks:

            tag = spec["id"]
            if tag in self.queue:
                continue

            # Form run tuple
            func = self.get_function(spec["spec"]["function"])
            task = self.dask_client.submit(func, *spec["spec"]["args"], **spec["spec"]["kwargs"])

            self.queue[tag] = (task, spec["parser"], spec["hooks"])
            self.logger.info("Adapter: Task submitted {}".format(tag))
            ret.append(tag)
        return ret

    def acquire_complete(self):
        """Pulls complete tasks out of the Dask queue.

        Returns
        -------
        list of dict
            The JSON structures of complete tasks
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
        """Waits for all tasks to complete before returning

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

    def close(self):
        """Closes down the DaskClient object

        Returns
        -------
        bool
            True if the closing was successful.
        """

        self.dask_client.close()
        return True
