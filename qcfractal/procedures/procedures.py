"""
A base for all procedures involved in on-node computation.
"""

import qcengine
import json
import hashlib

from . import procedures_util

_input_parsers = {}
_output_parsers = {}


def add_new_procedure(name, creator, unpacker):

    _input_parsers[name] = creator
    _output_parsers[name] = unpacker
    return True


def get_procedure_input_parser(name):

    return _input_parsers[name]


def get_procedure_output_parser(name):

    return _output_parsers[name]


### Add in the "standard procedures"
def procedure_single_input_parser(db, data):
    """

    json_data = {
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

    runs, errors = procedures_util.unpack_single_run_meta(db, data["meta"], data["data"])
    full_tasks = []
    for k, v in runs.items():

        keys = {"procedure_type": "single", "single_key": k}

        task = {
            "hash_index": procedures_util.hash_procedure_keys(keys),
            "hash_keys": keys,
            "spec": {
                "function": "qcengine.compute",
                "args": [v, data["meta"]["program"]],
                "kwargs": {}
            },
            "hooks": [],
            "tag": None,
            "parser": "single"
        }

        full_tasks.append(task)

    return (full_tasks, errors)


def procedure_single_output_parser(db, data):

    # Add new runs to database
    rdata = {k: v[0] for k, v in data.items()}
    results = procedures_util.parse_single_runs(db, rdata)
    ret = db.add_results(list(results.values()))

    hook_data = []
    for k, (data, hook) in data.items():

        # If no hooks skip it
        if len(hook) == 0:
            continue

        # Loop over hooks
        for h in hook:
            # Loop over individual commands
            for command in h["updates"]:
                if command[-1] == "$task_id":
                    command[-1] = results[k]["id"]

        hook_data.append(hook)

    return (ret, hook_data)


def procedure_optimization_input_parser(db, data):
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
        "input_specification": qc_schema_input
    }

    """

    # Unpack individual QC jobs
    runs, errors = procedures_util.unpack_single_run_meta(db, data["meta"]["qc_meta"], data["data"])

    if "options" in data["meta"]:
        keywords = db.get_options([(data["meta"]["program"], data["meta"]["options"])])["data"][0]
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
    for k, v in runs.items():

        # Coerce qc_template information
        packet = json.loads(template)
        packet["initial_molecule"] = v["molecule"]
        del v["molecule"]
        packet["input_specification"] = v

        # Unique nesting of args
        keys = {
            "procedure_type": "optimization",
            "single_key": k,
            "optimization_program": data["meta"]["program"],
            "optimization_kwargs": packet["keywords"]
        }

        task = {
            "hash_index": procedures_util.hash_procedure_keys(keys),
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

    return (full_tasks, errors)


def procedure_optimization_output_parser(db, data):

    new_procedures = {}

    # Each optimization is a unique entry:
    for k, (v, hooks) in data.items():

        # Convert start/stop molecules to hash
        mols = {"initial": v["initial_molecule"], "final": v["final_molecule"]}
        mol_keys = db.add_molecules(mols)["data"]
        v["initial_molecule"] = mol_keys["initial"]
        v["final_molecule"] = mol_keys["final"]

        # Add individual computations
        traj_dict = {k: v for k, v in enumerate(v["trajectory"])}
        results = procedures_util.parse_single_runs(db, traj_dict)

        ret = db.add_results(list(results.values()))
        v["trajectory"] = [x[1] for x in ret["data"]]

        # Coerce tags
        v.update(v["qcfractal_tags"])
        del v["input_specification"]
        del v["qcfractal_tags"]
        # print("Adding optimization result")
        # print(json.dumps(v, indent=2))
        new_procedures[k] = v

    ret = db.add_procedures(list(new_procedures.values()))

    hook_data = []
    for k, (data, hook) in data.items():

        # If no hooks skip it
        if len(hook) == 0:
            continue


        # Loop over hooks
        for h in hook:
            # Loop over individual commands
            for command in h["updates"]:
                if command[-1] == "$task_id":
                    command[-1] = new_procedures[k]["id"]

        hook_data.append(hook)

    return (ret, hook_data)


add_new_procedure("single", procedure_single_input_parser, procedure_single_output_parser)
add_new_procedure("optimization", procedure_optimization_input_parser, procedure_optimization_output_parser)
