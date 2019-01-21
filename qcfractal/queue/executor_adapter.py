"""
Queue adapter for Dask
"""

import logging
import traceback
from typing import Dict, List, Any, Optional

from .base_adapter import BaseAdapter


def _get_future(future):
    if future.exception() is None:
        return future.result()
    else:
        msg = "".join(traceback.format_exception(TypeError, future.exception(), future.traceback()))
        ret = {"success": False, "error_message": msg}
        return ret


class ExecutorAdapter(BaseAdapter):
    """A Queue Adapter for Python Executors
    """

    def __init__(self, client: Any, logger: Optional[logging.Logger] = None):
        BaseAdapter.__init__(self, client, logger)

    def __repr__(self):

        if hasattr(self.client, "_max_workers"):
            return "<ExecutorAdapter client=<{} max_workers={}>>".format(exec.__class__.__name__, self._max_workers)
        else:
            return "<ExecutorAdapter client={}>".format(self.client)

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
        for future in self.queue.values():
            while future[0].done() is False:
                time.sleep(0.1)

        return True

    def close(self) -> bool:
        for k, future in self.queue.items():
            future.cancel()

        self.client.close()
        return True


class DaskAdapter(ExecutorAdapter):
    """A Queue Adapter for Dask
    """

    def await_results(self) -> bool:
        from dask.distributed import wait
        futures = [v[0] for k, v in self.queue.items()]
        wait(futures)
        return True
