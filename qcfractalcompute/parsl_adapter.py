"""
Queue adapter for Parsl
"""

import logging
import time
import traceback
from typing import Any, Callable, Dict, Hashable, Optional, Tuple

from qcelemental.models import FailedOperation

from .base_adapter import BaseAdapter


def _get_future(future):
    # if future.exception() is None: # This always seems to return None
    try:
        return future.result()
    except Exception as e:
        msg = "Caught Parsl Error:\n" + traceback.format_exc()
        ret = FailedOperation(**{"success": False, "error": {"error_type": e.__class__.__name__, "error_message": msg}})
        return ret


class ParslAdapter(BaseAdapter):
    """An Adapter for Parsl."""

    def __init__(self, client: Any, logger: Optional[logging.Logger] = None, **kwargs):
        BaseAdapter.__init__(self, client, logger, **kwargs)

        import parsl

        self.client = parsl.dataflow.dflow.DataFlowKernel(self.client)
        self._parsl_states = parsl.dataflow.states.States
        self.app_map = {}

    def __repr__(self):
        return "<ParslAdapter client=<DataFlow label='{}'>>".format(self.client.config.executors[0].label)

    def get_app(self, function: str) -> Callable:
        """Obtains a Parsl python_application.

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

        >>> get_app("numpy.einsum")
        <class PythonApp"AppFactory for einsum>
        """

        from parsl.app.app import python_app

        if function in self.app_map:
            return self.app_map[function]

        func = self.get_function(function)

        # TODO set walltime and the like
        self.app_map[function] = python_app(func, data_flow_kernel=self.client)

        return self.app_map[function]

    def _submit_task(self, task_spec: Dict[str, Any]) -> Tuple[Hashable, Any]:

        # Form run tuple
        func = self.get_app(task_spec["spec"]["function"])
        task = func(*task_spec["spec"]["args"], **task_spec["spec"]["kwargs"])
        return task_spec["id"], task

    def count_active_task_slots(self) -> int:

        running = 0
        executor_running_task_map = {key: False for key in self.client.executors.keys()}
        for task in self.queue.values():
            status = self.client.tasks.get(task.tid, {}).get("status", None)
            if status == self._parsl_states.running:
                executor_running_task_map[task["executor"]] = True
            if all(executor_running_task_map.values()):
                # Efficiency loop break
                break

        found_readable = False
        for executor_key, executor in self.client.executors.items():
            if hasattr(executor, "connected_workers"):
                # Should return an int
                running += executor.connected_workers
                found_readable = True
            elif hasattr(executor, "max_threads") and executor_running_task_map[executor_key]:
                running += 1
                found_readable = True

        if not found_readable:
            raise NotImplementedError("Cannot accurately estimate consumption from executors")

        return running

    def acquire_complete(self) -> Dict[str, Any]:
        ret = {}
        del_keys = []
        for key, future in self.queue.items():
            if future.done():
                ret[key] = _get_future(future)
                del_keys.append(key)

        for key in del_keys:
            del self.queue[key]

        return ret

    def await_results(self) -> bool:
        for future in list(self.queue.values()):
            while future.done() is False:
                time.sleep(0.1)

        return True

    def close(self) -> bool:
        self.client.atexit_cleanup()
        return True
