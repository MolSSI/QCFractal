"""
Handlers for Dask
"""

import logging
import qcengine
import dask
import traceback
import json

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
        self.logger.info("MONGO ADD: FUTURE {}".format(tag))
        return tag

    def update(self):
        del_keys = []
        new_results = {}
        for key, future in self.queue.items():
            if future.done():
                try:
                    tmp_data = future.result()
                    if not tmp_data["success"]:
                        raise ValueError("Computation (%s, %s) did not complete successfully!:\n%s\n" %
                                         (tmp_data["molecule_hash"], tmp_data["modelchem"], tmp_data["error"]))
                    # res = self.mongod_socket.del_page_by_data(tmp_data)

                    self.logger.info("MONGO ADD: {}".format(key))
                    new_results[key] = tmp_data
                except Exception as e:
                    ename = str(type(e).__name__) + ":" + str(e)
                    msg = "".join(traceback.format_tb(e.__traceback__))
                    msg += str(type(e).__name__) + ":" + str(e)
                    self.errors[key] = msg
                    self.logger.info("MONGO ADD: ERROR\n%s" % msg)

                del_keys.append(key)

        # Get molecule ID's
        mols = {k : v["molecule"] for k, v in new_results.items()}
        mol_ret = self.mongod_socket.add_molecules(mols)["data"]

        for k, v in new_results.items():

            # Flatten data back out
            v["method"] = v["model"]["method"]
            v["basis"] = v["model"]["basis"]

            v["options"] = k[-1]
            del v["keywords"]

            v["molecule_id"] = mol_ret[k]
            del v["molecule"]

            v["program"] = k[0]

        res = self.mongod_socket.add_results(list(new_results.values()))

        for key in del_keys:
            del self.queue[key]

    def await_compute(self):

        # Try to get each results
        ret = [v.result() for k, v in self.queue.items()]
        self.update()


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

        # Build return metadata
        meta = {"errors": [], "n_inserted": 0, "success": False, "duplicates": [], "error_description": False}

        # Dumps is faster than copy
        task_meta = json.dumps({k: self.json["meta"][k] for k in ["program", "driver", "method", "basis", "options"]})

        # Form runs
        tasks = {}
        for mol in self.json["data"]:
            data = json.loads(task_meta)
            data["molecule_id"] = mol

            tasks[schema.format_result_indices(data)] = data

        # Check for duplicates in queue or server
        for t in tasks.keys():
            # We should also check for previously computed
            if t in queue_nanny.queue:
                meta["duplicates"].append(t)
                del tasks[t]

        # Pull out the needed molecules
        needed_mols = list({x["molecule_id"] for x in tasks.values()})
        raw_molecules = db.get_molecules(needed_mols, index="id")
        molecules = {x["id"]: x for x in raw_molecules["data"]}

        # Add molecules back into tasks
        for k, v in tasks.items():
            if v["molecule_id"] in molecules:
                v["molecule"] = molecules[v["molecule_id"]]
                del v["molecule_id"]
            else:
                meta["errors"].append((k, "Molecule not found"))
                del tasks[k]

        # Pull out the needed options
        option_set = db.get_options([(self.json["meta"]["program"], self.json["meta"]["options"])])["data"][0]
        del option_set["name"]
        del option_set["program"]

        # Add options back into tasks
        for k, v in tasks.items():
            v["keywords"] = option_set
            del v["options"]


        submitted = []
        # Adds tasks to futures and Nanny
        for k, v in tasks.items():

            # Reformat model syntax
            v["schema_name"] = "qc_schema_input"
            v["schema_version"] = 1
            v["model"] = {"method": v["method"], "basis": v["basis"]}
            del v["method"]
            del v["basis"]

            f = dask_client.submit(qcengine.compute, v, self.json["meta"]["program"])

            tag = queue_nanny.add_future(k, f)
            submitted.append(tag)

        # Return anything of interest
        meta["success"] = True
        meta["n_inserted"] = len(tasks)
        ret = {"meta": meta,
               "data": submitted}

        self.write(ret)

    # def get(self):

    #     # _check_auth(self.objects, self.request.headers)

    #     self.objects["mongod_socket"].set_project(header["project"])
    #     queue_nanny = self.objects["queue_nanny"]
    #     ret = {}
    #     ret["queue"] = list(queue_nanny.queue)
    #     ret["error"] = queue_nanny.errors
    #     self.write(ret)
