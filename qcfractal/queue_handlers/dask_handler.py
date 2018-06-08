"""
Handlers for Dask
"""

import logging
import qcengine

from ..web_handlers import APIHandler
from ..interface import schema



class DaskNanny:
    """
    This object can add to the Dask queue and watches for finished jobs. Jobs that are finished
    are automatically posted to the associated MongoDB and removed from the queue.
    """

    def __init__(self, queue_socket, mongod_socket, logger=None):

        self.queue_socket = queue_socket
        self.mongod_socket = mongod_socket
        self.queue = {}
        self.errors = {}

        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('DaskNanny')

    def add_future(self, tag, future):
        self.queue[tag] = future
        self.logger.info("MONGO ADD: FUTURE %s" % tag)
        return tag

    def update(self):
        del_keys = []
        new_results = []
        for key, future in self.queue.items():
            if future.done():
                try:
                    tmp_data = future.result()
                    if not tmp_data["success"]:
                        raise ValueError("Computation (%s, %s) did not complete successfully!:\n%s\n" %
                                         (tmp_data["molecule_hash"], tmp_data["modelchem"], tmp_data["error"]))
                    # res = self.mongod_socket.del_page_by_data(tmp_data)
                    new_result.append(tmp_data)
                    tag = (task[k] for k in result_indices)
                    self.logger.info("MONGO ADD: %s" % (tag))
                except Exception as e:
                    ename = str(type(e).__name__) + ":" + str(e)
                    msg = "".join(traceback.format_tb(e.__traceback__))
                    msg += str(type(e).__name__) + ":" + str(e)
                    self.errors[key] = msg
                    self.logger.info("MONGO ADD: ERROR\n%s" % msg)

                del_keys.append(key)

        res = self.mongod_socket.add_results(new_results)

        for key in del_keys:
            del self.queue[key]


class DaskScheduler(APIHandler):
    """
    Takes in a data packet the contains the molecule_hash, modelchem and options objects.
    """

    def post(self):

        # _check_auth(self.objects, self.request.headers)

        # Grab objects
        self.objects["mongod_socket"].set_project(header["project"])
        dask = self.objects["queue_socket"]
        queue_nanny = self.objects["queue_nanny"]

        # Submit
        ret = {}
        ret["error"] = []
        ret["submitted"] = []

        result_indices = schema.get_indices("result")

        # Loop over the tasks
        for task in tasks:
            tag = (task[k] for k in result_indices)
            fut = dask.submit(compute.computers[program], task)

            ret["submitted"].append(tag)
            queue_nanny.add_future(tag, fut)

        # Return anything of interest
        ret["success"] = True

        self.write(ret)

    # def get(self):

    #     # _check_auth(self.objects, self.request.headers)

    #     self.objects["mongod_socket"].set_project(header["project"])
    #     queue_nanny = self.objects["queue_nanny"]
    #     ret = {}
    #     ret["queue"] = list(queue_nanny.queue)
    #     ret["error"] = queue_nanny.errors
    #     self.write(ret)
