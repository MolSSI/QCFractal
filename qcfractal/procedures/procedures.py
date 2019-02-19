"""
All procedures tasks involved in on-node computation.
"""

import json
from typing import Union

from .procedures_util import hash_procedure_keys, parse_hooks, parse_single_tasks, unpack_single_task_spec


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
                "keywords": "default",
                "program": "psi4"
                },
            },
            "data": ["mol_id_1", "mol_id_2", ...],
        }

        """

        # format the data
        inputs, errors = unpack_single_task_spec(self.storage, data.meta, data.data)

        # Insert new results stubs
        result_stub = json.dumps({k: data.meta[k] for k in ["driver", "method", "basis", "keywords", "program"]})

        # Grab the tag if available
        tag = data.meta.pop("tag", None)

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
                    "args": [json.loads(inp.json()), data.meta["program"]],  # todo: json_dict should come from results
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

        self.storage.queue_submit(new_tasks)

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
        results = parse_single_tasks(self.storage, rdata)

        ret = self.storage.add_results(list(results.values()), update_existing=True)

        # Sort out hook data
        hook_data = parse_hooks(results, rhooks)

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
                    "keywords": "default",
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
        inputs, errors = unpack_single_task_spec(self.storage, data.meta["qc_spec"], data.data)

        # Unpack options
        if data.meta["keywords"] is None:
            keyword_set = {}
            keyword_id = None
        else:
            keyword_set = self.storage.get_add_keywords_mixed([data.meta["keywords"]])["data"][0]
            keyword_id = keyword_set["id"]
            keyword_set = keyword_set["values"]

        keyword_set["program"] = data.meta["qc_spec"]["program"]
        template = json.dumps({"keywords": keyword_set, "qcfractal_tags": data.meta})

        tag = data.meta.pop("tag", None)

        new_tasks = []
        results_ids = []
        existing_ids = []
        for inp in inputs:
            if inp is None:
                results_ids.append(None)
                continue

            inp = json.loads(inp.json())

            # Coerce qc_template information
            packet = json.loads(template)
            packet["initial_molecule"] = inp.pop("molecule")
            packet["input_specification"] = inp
            packet["procedure"] = "optimization"
            packet["program"] = data.meta["program"]

            single_keys = data.meta["qc_spec"].copy()
            single_keys["molecule"] = packet["initial_molecule"]["id"]

            # Add to args document to carry through to self.storage
            hash_index = hash_procedure_keys({
                "type": "optimization",
                "program": data.meta["program"],
                "keywords": keyword_id,
                "single_key": single_keys,
            })
            packet["hash_index"] = hash_index

            ret = self.storage.add_procedures([packet])
            base_id = ret["data"][0]
            results_ids.append(base_id)

            # Task is complete
            if len(ret["meta"]["duplicates"]):
                existing_ids.append(base_id)
                continue

            # Build task object
            task = {
                "spec": {
                    "function": "qcengine.compute_procedure",
                    "args": [packet, data.meta["program"]],
                    "kwargs": {}
                },
                "hooks": [],
                "tag": tag,
                "parser": "optimization",
                "base_result": ("procedure", base_id)
            }

            new_tasks.append(task)

        return new_tasks, results_ids, existing_ids, errors

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
            initial_mol, final_mol = self.storage.add_molecules(
                [procedure["initial_molecule"], procedure["final_molecule"]])["data"]
            procedure["initial_molecule"] = initial_mol
            procedure["final_molecule"] = final_mol

            # Parse trajectory computations and add task_id
            traj_dict = {k: v for k, v in enumerate(procedure["trajectory"])}
            results = parse_single_tasks(self.storage, traj_dict)
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

        hook_data = parse_hooks(new_procedures, new_hooks)

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
