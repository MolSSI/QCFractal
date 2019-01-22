"""
Queue adapter for Parsl
"""

import importlib
import logging
import operator
import time
import traceback


def _get_future(future):
    # if future.exception() is None: # This always seems to return None
    try:
        return future.result()
    except Exception as e:
        msg = "Caught Parsl Error:\n" + traceback.format_exc()
        ret = {"success": False, "error": msg}
        return ret


class ParslAdapter:
    """A Adapter for Parsl
    """

    def __init__(self, parsl_config, logger=None):
        """
        Parameters
        ----------
        parsl_dataflow : parsl.config.Config
            A activate Parsl DataFlow
        logger : None, optional
            A optional logging object to write output to
        """

        import parsl
        self.dataflow = parsl.dataflow.dflow.DataFlowKernel(parsl_config)
        self.queue = {}
        self.function_map = {}

        self.logger = logger or logging.getLogger('ParslAdapter')

    def __repr__(self):
        return "<ParslAdapter client=<DataFlow label='{}'>>".format(self.dataflow.config.executors[0].label)

    def get_function(self, function):
        """Obtains a Python function wrapped in a Parsl Python App

        Parameters
        ----------
        function : str
            A full path to a function

        Returns
        -------
        callable
            The desired AppFactory

        Examples
        --------

        >>> get_function("numpy.einsum")
        <class PythonApp"AppFactory for einsum>
        """

        from parsl.app.app import python_app

        if function in self.function_map:
            return self.function_map[function]

        module_name, func_name = function.split(".", 1)
        module = importlib.import_module(module_name)
        func = operator.attrgetter(func_name)(module)

        # TODO set walltime and the like
        self.function_map[function] = python_app(func, data_flow_kernel=self.dataflow)

        return self.function_map[function]

    def submit_tasks(self, tasks):
        """Adds tasks to the Parsl queue

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
        for spec in tasks:

            tag = spec["id"]
            if tag in self.queue:
                continue

            # Form run tuple
            func = self.get_function(spec["spec"]["function"])
            task = func(*spec["spec"]["args"], **spec["spec"]["kwargs"])

            self.queue[tag] = (task, spec["parser"], spec["hooks"])
            self.logger.info("Adapter: Task submitted {}".format(tag))
            ret.append(tag)
        return ret

    def acquire_complete(self):
        """Pulls complete tasks out of the Parsl queue.

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
            True if the opertion was successful.
        """

        for future in self.queue.values():
            while future[0].done() is False:
                time.sleep(0.1)

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
        """Closes down the DataFlow object

        Returns
        -------
        bool
            True if the closing was successful.
        """

        self.dataflow.atexit_cleanup()
        return True
