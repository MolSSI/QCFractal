"""
Handlers for Dask
"""

import logging
import qcengine
import dask


class DaskAdapter:
    def __init__(self, dask_server, logger=None):

        self.dask_server = dask_server
        self.queue = {}
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('DaskNanny')

    def submit_tasks(self, tasks):
        ret = []
        for tag, args in tasks.items():
            if tag in self.queue:
                continue

            self.queue[tag] = self.dask_server.submit(*args)
            self.logger.info("Adapter: Task submitted {}".format(tag))
            ret.append(tag)
        return ret

    def aquire_complete(self):
        ret = {}
        del_keys = []
        for key, future in self.queue.items():
            if future.done():
                ret[key] = future.result()
                del_keys.append(key)

        for key in del_keys:
            del self.queue[key]

        return ret

    def await_results(self):
        # Try to get each results
        ret = [v.result() for k, v in self.queue.items()]

    def list_tasks(self):
        return list(self.queue.keys())
