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
        """Parse input json into internally appropriate format


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
        inputs, errors = procedures_util.unpack_single_run_meta(self.storage, data["meta"], data["data"])

        # Insert new results stubs
        result_stub = json.dumps({k: data["meta"][k] for k in ["driver", "method", "basis", "options", "program"]})

        # Grab the tag if available
        tag = data["meta"].pop("tag", None)

        # Construct full tasks
        new_tasks = []
        results_ids = []
        existing_ids = []
        for inp in inputs:
            if inp is None:
                results_ids.append(None)
                continue

            # Build stub
            result_obj = json.loads(result_stub)
            result_obj["molecule"] = str(inp.molecule.id)
            ret = self.storage.add_results([result_obj])

            base_id = ret["data"][0]
            results_ids.append(base_id)

            # Task is complete
            if len(ret["meta"]["duplicates"]):
                existing_ids.append(base_id)
                continue

            # Build task object
            task = {
                "spec": {
                    "function": "qcengine.compute",  # todo: add defaults in models
                    "args": [json.loads(inp.json()), data["meta"]["program"]], # todo: json_dict should come from results
                    "kwargs": {}  # todo: add defaults in models
                },
                "hooks": [],  # todo: add defaults in models
                "tag": tag,
                "parser": "single",
                "base_result": ("results", base_id)
            }

            new_tasks.append(task)

        return new_tasks, results_ids, existing_ids, errors

    def submit_tasks(self, data):

        new_tasks, results_ids, existing_ids, errors = self.parse_input(data)

        ret = self.storage.queue_submit(new_tasks)

        n_inserted = 0
        missing = []
        for num, x in enumerate(results_ids):
            if x is None:
                missing.append(num)
            else:
                n_inserted += 1

        results = {
            "meta": {
                "n_inserted": n_inserted,
                "duplicates": [],
                "validation_errors": [],
                "success": True,
                "error_description": False,
                "errors": errors
            },
            "data": {
                "ids": results_ids,
                "submitted": [x["base_result"][1] for x in new_tasks],
                "existing": existing_ids,
            }
        }

        return results

    def parse_output(self, data):

        # Add new runs to database
        # Parse out hooks and data to same key/value
        rdata = {}
        rhooks = {}
        for data, hooks in data:
            key = data["task_id"]
            rdata[key] = data
            if len(hooks):
                rhooks[key] = hooks

        # Add results to database
        results = procedures_util.parse_single_runs(self.storage, rdata)

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
    """
    Optimization task manipulation
    """

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
        template = json.dumps({"keywords": keywords, "qcfractal_tags": data["meta"]})

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
        completed_procedures = self.storage.get_procedures_by_id(
            hash_index=duplicate_lookup, projection={"hash_index": True,
                                                     "id": True,
                                                     "task_id": True})["data"]

        if len(completed_procedures):
            found_hashes = set(x["hash_index"] for x in completed_procedures)

            # Filter out tasks
            new_tasks = []
            for task in full_tasks:
                if task["hash_index"] in found_hashes:
                    continue
                else:
                    new_tasks.append(task)

            # Update returned list to exclude duplicates
            full_tasks = new_tasks

        # Add task stubs
        for task in full_tasks:
            stub = {"hash_index": task["hash_index"], "procedure": "optimization", "program": data["meta"]["program"]}
            ret = self.storage.add_procedures([stub])
            task["base_result"] = ("procedure", ret["data"][0])

        return full_tasks, completed_procedures, errors

    def parse_output(self, data):
        """Save the results of the procedure.
        It must make sure to save the results in the results table
        including the task_id in the TaskQueue table
        """

        new_procedures = {}
        new_hooks = {}

        # Each optimization is a unique entry:
        for procedure, hooks in data:
            task_id = procedure["task_id"]

            # Convert start/stop molecules to hash
            mols = {"initial": procedure["initial_molecule"], "final": procedure["final_molecule"]}
            mol_keys = self.storage.add_molecules(mols)["data"]
            procedure["initial_molecule"] = mol_keys["initial"]
            procedure["final_molecule"] = mol_keys["final"]

            # Parse trajectory computations and add task_id
            traj_dict = {k: v for k, v in enumerate(procedure["trajectory"])}
            results = procedures_util.parse_single_runs(self.storage, traj_dict)
            for k, v in results.items():
                v["task_id"] = task_id

            # Add trajectory results and return ids
            ret = self.storage.add_results(list(results.values()))
            procedure["trajectory"] = ret["data"]

            # Coerce tags
            procedure.update(procedure["qcfractal_tags"])
            del procedure["input_specification"]
            del procedure["qcfractal_tags"]
            # print("Adding optimization result")
            # print(json.dumps(v, indent=2))
            new_procedures[task_id] = procedure
            if len(hooks):
                new_hooks[task_id] = hooks

            self.storage.update_procedure(procedure["hash_index"], procedure)

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
