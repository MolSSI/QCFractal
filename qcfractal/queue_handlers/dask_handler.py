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
        db = self.objects["db_socket"]
        dask_client = self.objects["queue_socket"]
        queue_nanny = self.objects["queue_nanny"]
        result_indices = schema.get_indices("result")

        # Build metadata
        meta = {"errors": [], "n_inserted": 0, "success": False, "duplicates": [], "error_description": False}
        submitted = []

        # Check for errors or duplicates
        tasks = {(t[k] for k in result_indices): t for t in self.json["data"]}
        print(tasks)

        for t in tasks.keys():
            # We should also check for previously computed
            if t in queue_nanny.queue:
                meta["duplicates"].append(t)
                del tasks[t]

        # Pull out the needed molecules
        needed_mols = {x["molecule_id"] for x in tasks.items()}
        molecules = {x["id"]: x for x in db.get_molecules(needed_mols, index="id")}

        # Add molecules back into tasks
        for k, v in tasks.items():
            if v["molecule_id"] in molecules:
                v["molecule"] = molecules[v["molecule_id"]]
            else:
                meta["errors"].append((k, "Molecule not found"))
                del tasks[k]

        print("here")
        # Adds tasks to futures and Nanny
        for k, v in tasks.items():
            f = dask.submit(qcengine.compute, v)

            tag = queue_nanny.add_future(k, f)
            ret["submitted"].append(tag)

        # Return anything of interest
        meta["success"] = True
        meta["n_inserted"] = len(tasks)
        ret = {"meta": meta,
               "data": list(tasks.keys())}


        self.write(ret)

    # def get(self):

    #     # _check_auth(self.objects, self.request.headers)

    #     self.objects["mongod_socket"].set_project(header["project"])
    #     queue_nanny = self.objects["queue_nanny"]
    #     ret = {}
    #     ret["queue"] = list(queue_nanny.queue)
    #     ret["error"] = queue_nanny.errors
    #     self.write(ret)
