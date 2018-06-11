"""
Handlers for Dask
"""

import logging
import qcengine
import dask
import traceback

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
            tmp_data["method"] = tmp_data["model"]["method"]
            tmp_data["basis"] = tmp_data["model"]["basis"]

            tmp_data["options"] = k[-1]
            del tmp_data["keywords"]

            tmp_data["molecule_id"] = mol_ret[k]
            del tmp_data["molecule"]

            tmp_data["program"] = k[0]

        res = self.mongod_socket.add_results(list(new_results.values()))

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

        # Build return metadata
        meta = {"errors": [], "n_inserted": 0, "success": False, "duplicates": [], "error_description": False}

        # Check for errors or duplicates
        program = self.json["meta"]["program"]
        tasks = {schema.format_result_indices(t, program=program): t for t in self.json["data"]}

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
        needed_options = list({(program, x["options"]) for x in tasks.values()})
        raw_options = db.get_options(needed_options)
        options = {x["name"]: x for x in raw_options["data"]}
        for k, v in options.items():
            del v["name"]
            del v["program"]

        # Add options back into tasks
        for k, v in tasks.items():
            if v["options"] in options:
                v["keywords"] = options[v["options"]]
                del v["options"]
            else:
                meta["errors"].append((k, "Option not found"))
                del tasks[k]


        submitted = []
        # Adds tasks to futures and Nanny
        for k, v in tasks.items():

            # Reformat model syntax
            v["schema_name"] = "qc_schema_input"
            v["schema_version"] = 1
            v["model"] = {"method": v["method"], "basis": v["basis"]}
            del v["method"]
            del v["basis"]

            f = dask_client.submit(qcengine.compute, v, program)

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
