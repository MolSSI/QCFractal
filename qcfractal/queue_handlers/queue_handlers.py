"""
Queue backend abstraction manager.
"""

import logging
import qcengine
import traceback
import json

from ..web_handlers import APIHandler
from ..interface import schema


class QueueNanny:
    """
    This object can add to the Dask queue and watches for finished jobs. Jobs that are finished
    are automatically posted to the associated MongoDB and removed from the queue.
    """

    def __init__(self, queue_adapter, db_socket, logger=None):

        self.queue_adapter = queue_adapter
        self.db_socket = db_socket
        self.queue = {}
        self.errors = {}

        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger('QueueNanny')

    def submit_tasks(self, tasks):
        return self.queue_adapter.submit_tasks(tasks)

    def update(self):
        del_keys = []
        new_results = {}

        for key, tmp_data in self.queue_adapter.aquire_complete().items():
            try:
                if not tmp_data["success"]:
                    raise ValueError("Computation (%s, %s) did not complete successfully!:\n%s\n" %
                                     (tmp_data["molecule_hash"], tmp_data["modelchem"], tmp_data["error"]))
                # res = self.db_socket.del_page_by_data(tmp_data)

                self.logger.info("MONGO ADD: {}".format(key))
                new_results[key] = tmp_data
            except Exception as e:
                ename = str(type(e).__name__) + ":" + str(e)
                msg = "".join(traceback.format_tb(e.__traceback__))
                msg += str(type(e).__name__) + ":" + str(e)
                self.errors[key] = msg
                self.logger.info("MONGO ADD: ERROR\n%s" % msg)

        # Get molecule ID's
        mols = {k: v["molecule"] for k, v in new_results.items()}
        mol_ret = self.db_socket.add_molecules(mols)["data"]

        for k, v in new_results.items():

            # Flatten data back out
            v["method"] = v["model"]["method"]
            v["basis"] = v["model"]["basis"]
            del v["model"]

            v["options"] = k[-1]
            del v["keywords"]

            v["molecule_id"] = mol_ret[k]
            del v["molecule"]

            v["program"] = k[0]

        ret = self.db_socket.add_results(list(new_results.values()))

    def await_results(self):
        self.queue_adapter.await_results()
        self.update()
        return True

    def list_current_tasks(self):
        return self.queue_adapter.list_tasks()


class QueueScheduler(APIHandler):
    """
    Takes in a data packet the contains the molecule_hash, modelchem and options objects.
    """

    def post(self):

        # _check_auth(self.objects, self.request.headers)

        # Grab objects
        db = self.objects["db_socket"]
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
        # for t in tasks.keys():
        #     # We should also check for previously computed
        #     if t in queue_nanny.queue:
        #         meta["duplicates"].append(t)
        #         del tasks[t]

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

        # Build out full and complete task list
        full_tasks = {}
        for k, v in tasks.items():
            # Reformat model syntax
            v["schema_name"] = "qc_schema_input"
            v["schema_version"] = 1
            v["model"] = {"method": v["method"], "basis": v["basis"]}
            del v["method"]
            del v["basis"]

            full_tasks[k] = (qcengine.compute, v, self.json["meta"]["program"])

        # Add tasks to Nanny
        submitted = queue_nanny.submit_tasks(full_tasks)

        # Return anything of interest
        meta["success"] = True
        meta["n_inserted"] = len(submitted)
        ret = {"meta": meta, "data": submitted}

        self.write(ret)

    # def get(self):

    #     # _check_auth(self.objects, self.request.headers)

    #     self.objects["db_socket"].set_project(header["project"])
    #     queue_nanny = self.objects["queue_nanny"]
    #     ret = {}
    #     ret["queue"] = list(queue_nanny.queue)
    #     ret["error"] = queue_nanny.errors
    #     self.write(ret)


def build_queue(queue_type, queue_socket, db_socket, **kwargs):

    if queue_type == "dask":
        try:
            import dask.distributed
        except ImportError:
            raise ImportError(
                "Dask.distributed not installed, please install dask.distributed for the dask queue client.")

        from . import dask_handler

        adapter = dask_handler.DaskAdapter(queue_socket)

    elif queue_type == "fireworks":
        try:
            import fireworks
        except ImportError:
            raise ImportError("Fireworks not installed, please install fireworks for the fireworks queue client.")

        from . import fireworks_handler

        nanny = fireworks_handler.FireworksNanny(queue_socket, db_socket, **kwargs)
        scheduler = fireworks_handler.FireworksScheduler

    else:
        raise KeyError("Queue type '{}' not understood".format(queue_type))

    nanny = QueueNanny(adapter, db_socket, **kwargs)
    scheduler = QueueScheduler

    return (nanny, scheduler)
