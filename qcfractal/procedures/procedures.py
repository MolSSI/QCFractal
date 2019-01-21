"""
All procedures tasks involved in on-node computation.
"""

import json
from typing import Union
from . import procedures_util


class SingleResultTasks:
    """Single is a simple Result
     Unique by: driver, method, basis, option (the name in the options table),
     and program.
    """

    def __init__(self, storage):
        self.storage = storage

    def parse_input(self, data):
        """Parse input json into internally approperiate format


        Format of the input data:
        data = {
            "meta": {
                "procedure": "single",
                "driver": "energy",
                "method": "HF",
                "basis": "sto-3g",
                "options": "default",
                "program": "psi4"
                },
            },
            "data": ["mol_id_1", "mol_id_2", ...],
        }

        """

        # format the data
        runs, errors = procedures_util.unpack_single_run_meta(self.storage, data["meta"], data["data"])

        # Remove duplicates
        query = {k: data["meta"][k] for k in ["driver", "method", "basis", "options", "program"]}
        result_stub = json.dumps(query)
        query["molecule"] = [x["molecule"]["id"] for x in runs.values()]
        query["status"] = None

        completed_results = self.storage.get_results(**query, projection={"molecule": True})["data"]
        completed_ids = set(x["molecule"] for x in completed_results)

        # Grab the tag if available
        tag = data["meta"].pop("tag", None)

        # Construct full tasks
        full_tasks = []
        for k, v in runs.items():
            if v["molecule"]["id"] in completed_ids:
                continue

            query["molecule"] = v["molecule"]["id"]
            keys, hash_index = procedures_util.single_run_hash(query)
            v["hash_index"] = hash_index  # TODO: this field to be removed

            # Build stub
            result_obj = json.loads(result_stub)
            result_obj["molecule"] = v["molecule"]["id"]
            result_obj["status"] = "INCOMPLETE"  # TODO: no need, it's default
            base_id = self.storage.add_results([result_obj])["data"][0]

            # Build task object
            task = {
                "hash_index": hash_index,  # todo: to be removed
                "hash_keys": keys,
                "spec": {
                    "function": "qcengine.compute",  # todo: add defaults in models
                    "args": [v, data["meta"]["program"]],
                    "kwargs": {}  # todo: add defaults in models
                },
                "hooks": [],  # todo: add defaults in models
                "tag": tag,
                "parser": "single",
                "base_result": ("results", base_id)
            }

            full_tasks.append(task)

        return full_tasks, completed_results, errors

    def parse_output(self, data):

        # Add new runs to database
        # Parse out hooks and data to same key/value
        rdata = {}
        rhooks = {}
        for data, hooks in data:
            key = data["queue_id"]
            rdata[key] = data
            if len(hooks):
                rhooks[key] = hooks

        # Add results to database
        results = procedures_util.parse_single_runs(self.storage, rdata)
        for k, v in results.items():
            v["status"] = "COMPLETE"
        ret = self.storage.add_results(list(results.values()), update_existing=True)

        # Sort out hook data
        hook_data = procedures_util.parse_hooks(results, rhooks)

        # Create a list of (queue_id, located) to update the queue with
        completed = list(results.keys())

        errors = []
        if len(ret["meta"]["errors"]):
            # errors = [(k, "Duplicate results found")]
            raise ValueError("TODO: Cannot yet handle queue result duplicates.")

        return completed, errors, hook_data

# ----------------------------------------------------------------------------


class OptimizationTasks(SingleResultTasks):

    def __init__(self, storage):
        super().__init__(storage)

    def parse_input(self, data, duplicate_id="hash_index"):
        """

        json_data = {
            "meta": {
                "procedure": "optimization",
                "option": "default",
                "program": "geometric",
                "qc_meta": {
                    "driver": "energy",
                    "method": "HF",
                    "basis": "sto-3g",
                    "options": "default",
                    "program": "psi4"
                },
            },
            "data": ["mol_id_1", "mol_id_2", ...],
        }

        qc_schema_input = {
            "schema_name": "qc_schema_input",
            "schema_version": 1,
            "molecule": {
                "geometry": [
                    0.0,  0.0, -0.6,
                    0.0,  0.0,  0.6,
                ],
                "symbols": ["H", "H"],
                "connectivity": [[0, 1, 1]]
            },
            "driver": "gradient",
            "model": {
                "method": "HF",
                "basis": "sto-3g"
            },
            "keywords": {},
        }
        json_data = {
            "schema_name": "qc_schema_optimization_input",
            "schema_version": 1,
            "keywords": {
                "coordsys": "tric",
                "maxiter": 100,
                "program": "psi4"
            },
        }

        """

        # Unpack individual QC tasks
        runs, errors = procedures_util.unpack_single_run_meta(self.storage, data["meta"]["qc_meta"], data["data"])

        if "options" in data["meta"]:
            if data["meta"]["options"] is None:
                keywords = {}
            else:  # TODO: why is the option guaranteed to exist? (implicit dependency)
                keywords = self.storage.get_options([(data["meta"]["program"], data["meta"]["options"])])["data"][0]
                del keywords["program"]
                del keywords["name"]
        elif "keywords" in data["meta"]:
            keywords = data["meta"]["keywords"]
        else:
            keywords = {}

        keywords["program"] = data["meta"]["qc_meta"]["program"]
        template = json.dumps({
            "schema_name": "qc_schema_optimization_input",
            "schema_version": 1,
            "keywords": keywords,
            "qcfractal_tags": data["meta"]
        })

        full_tasks = []
        duplicate_lookup = []
        for k, v in runs.items():

            # Coerce qc_template information
            packet = json.loads(template)
            packet["initial_molecule"] = v["molecule"]
            del v["molecule"]
            packet["input_specification"] = v

            # Unique nesting of args
            keys = {
                "type": "optimization",
                "program": data["meta"]["program"],
                "keywords": packet["keywords"],
                "single_key": k,
            }

            # Add to args document to carry through to self.storage
            hash_index = procedures_util.hash_procedure_keys(keys)
            packet["hash_index"] = hash_index
            duplicate_lookup.append(hash_index)

            task = {
                "hash_index": hash_index,
                "hash_keys": keys,
                "spec": {
                    "function": "qcengine.compute_procedure",
                    "args": [packet, data["meta"]["program"]],
                    "kwargs": {}
                },
                "hooks": [],
                "tag": None,
                "parser": "optimization"
            }

            full_tasks.append(task)

        # Find and handle duplicates
        completed_procedures = self.storage.get_procedures(
            {
                "hash_index": duplicate_lookup
            }, projection={"hash_index": True,
                           "id": True, "queue_id": True})["data"]

        duplicates = []
        if len(completed_procedures):
            found_hashes = set(x["hash_index"] for x in completed_procedures)

            # Filter out tasks
            new_tasks = []
            for task in full_tasks:
                if task["hash_index"] in found_hashes:
                    continue
                else:
                    new_tasks.append(task)

            # if duplicate_id == "hash_index":
            #     duplicates = list(found_hashes)
            # elif duplicate_id == "id":
            #     duplicates = [x["id"] for x in completed_procedures]
            # else:
            #     raise KeyError("Duplicate id '{}' not understood".format(duplicate_id))

            full_tasks = new_tasks

        # Add task stubs
        for task in full_tasks:
            stub = {"hash_index": task["hash_index"], "procedure": "optimization", "program": data["meta"]["program"]}
            ret = self.storage.add_procedures([stub])
            task["base_result"] = ("procedure", ret["data"][0])

        return full_tasks, completed_procedures, errors

    def parse_output(self, data):

        new_procedures = {}
        new_hooks = {}

        # Each optimization is a unique entry:
        for result, hooks in data:
            key = result["queue_id"]

            # Convert start/stop molecules to hash
            mols = {"initial": result["initial_molecule"], "final": result["final_molecule"]}
            mol_keys = self.storage.add_molecules(mols)["data"]
            result["initial_molecule"] = mol_keys["initial"]
            result["final_molecule"] = mol_keys["final"]

            # Parse trajectory computations and add queue_id
            traj_dict = {k: v for k, v in enumerate(result["trajectory"])}
            results = procedures_util.parse_single_runs(self.storage, traj_dict)
            for k, v in results.items():
                v["queue_id"] = key

            # Add trajectory results and return ids
            ret = self.storage.add_results(list(results.values()))
            result["trajectory"] = ret["data"]

            # Coerce tags
            result.update(result["qcfractal_tags"])
            del result["input_specification"]
            del result["qcfractal_tags"]
            # print("Adding optimization result")
            # print(json.dumps(v, indent=2))
            new_procedures[key] = result
            if len(hooks):
                new_hooks[key] = hooks

            self.storage.update_procedure(result["hash_index"], result)
        # print(list(new_procedures.values()))
        # raise Exception()
        # ret = self.storage.add_procedures(list(new_procedures.values()))

        # Create a list of (queue_id, located) to update the queue with
        completed = list(new_procedures.keys())

        errors = []
        # if len(ret["meta"]["errors"]):
        #     # errors = [(k, "Duplicate results found")]
        #     raise ValueError("TODO: Cannot yet handle queue result duplicates.")

        hook_data = procedures_util.parse_hooks(new_procedures, new_hooks)

        # return (ret, hook_data)
        return completed, errors, hook_data

# ----------------------------------------------------------------------------


supported_procedures = Union[SingleResultTasks, OptimizationTasks]


def get_procedure_parser(procedure_type: str, storage) -> supported_procedures:
    """A factory methods that returns the approperiate parser class
    for the supported procedure types (like single and optimization)

    Parameters
    ---------
    procedure_type: str, 'single' or 'optimization'
    storage: storage socket object
        such as MongoengineSocket object

    Returns
    -------
    A parser class corresponding to the procedure_type:
        'single' --> SingleResultTasks
        'optimization' --> OptimizationTasks
    """

    if procedure_type == 'single':
        return SingleResultTasks(storage)
    elif procedure_type == 'optimization':
        return OptimizationTasks(storage)
    else:
        raise KeyError("Procedure type ({}) is not suported yet.".format(procedure_type))
