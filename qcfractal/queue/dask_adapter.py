"""
Queue adapter for Dask
"""

import importlib
import logging
import operator
import traceback

from typing import Callable, Dict, List, Any, Optional

from .base_adapter import BaseAdapter


def _get_future(future):
    if future.exception() is None:
        return future.result()
    else:
        msg = "".join(traceback.format_exception(TypeError, future.exception(), future.traceback()))
        ret = {"success": False, "error_message": msg}
        return ret


class DaskAdapter(BaseAdapter):
    """A Queue Adapter for Dask
    """

    def __init__(self, client: Any, logger: Optional[logging.Logger]=None):
        BaseAdapter.__init__(self, client, logger)
        self.function_map = {}

    def __repr__(self):
        return "<DaskAdapter client={}>".format(self.client)

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

    def submit_tasks(self, tasks: Dict[str, Any]) -> List[str]:
        ret = []
        for spec in tasks:

            tag = spec["id"]
            if tag in self.queue:
                continue

            # Form run tuple
            func = self.get_function(spec["spec"]["function"])
            task = self.client.submit(func, *spec["spec"]["args"], **spec["spec"]["kwargs"])

            self.queue[tag] = (task, spec["parser"], spec["hooks"])
            self.logger.info("Adapter: Task submitted {}".format(tag))
            ret.append(tag)
        return ret

    def acquire_complete(self) -> List[Dict[str, Any]]:
        ret = {}
        del_keys = []
        for key, (future, parser, hooks) in self.queue.items():
            if future.done():
                ret[key] = (_get_future(future), parser, hooks)
                del_keys.append(key)

        for key in del_keys:
            del self.queue[key]

        return ret

    def await_results(self) -> bool:
        from dask.distributed import wait
        futures = [v[0] for k, v in self.queue.items()]
        wait(futures)

        return True

    def close(self) -> bool:
        for k, future in self.queue.items():
            future.cancel()

        self.client.close()
        return True
