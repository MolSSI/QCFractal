"""
Queue adapter for Dask
"""

import time
import traceback
from typing import Dict, List, Any

from .base_adapter import BaseAdapter


def _get_future(future):
    try:
        return future.result()
    except Exception as e:
        msg = traceback.format_exc()
        ret = {"success": False, "error_message": msg}
        return ret


class ExecutorAdapter(BaseAdapter):
    """A Queue Adapter for Python Executors
    """

    def __repr__(self):

        return "<ExecutorAdapter client=<{} max_workers={}>>".format(self.client.__class__.__name__,
                                                                     self.client._max_workers)

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
        for future in self.queue.values():
            future[0].cancel()

        self.client.shutdown()
        return True


class DaskAdapter(ExecutorAdapter):
    """A Queue Adapter for Dask
    """

    def __repr__(self):

        return "<DaskAdapter client={}>".format(self.client)

    def await_results(self) -> bool:
        from dask.distributed import wait
        futures = [f[0] for f in self.queue.values()]
        wait(futures)
        return True

    def close(self) -> bool:

        self.client.close()
        return True