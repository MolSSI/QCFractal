"""
A base for all procedures involved in on-node computation.
"""

import json

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
def procedure_single_input_parser(storage, data):
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

    runs, errors = procedures_util.unpack_single_run_meta(storage, data["meta"], data["data"])

    # Remove duplicates
    query = {k : data["meta"][k] for k in ["driver", "method", "basis", "options", "program"]}
    query["molecule_id"] = [x["molecule"]["id"] for x in runs.values()]

    search = storage.get_results(query, projection={"molecule_id": True})
    completed = set(x["molecule_id"] for x in search["data"])

    # Construct full tasks
    full_tasks = []
    for k, v in runs.items():
        if v["molecule"]["id"] in completed:
            continue

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

    return (full_tasks, completed, errors)


def procedure_single_output_parser(storage, data):

    # Add new runs to database
    rdata = {k: v[0] for k, v in data.items()}
    results = procedures_util.parse_single_runs(storage, rdata)
    ret = storage.add_results(list(results.values()))

    hook_data = procedures_util.parse_hooks(data, results)

    return (ret, hook_data)


def procedure_optimization_input_parser(storage, data, duplicate_id="hash_index"):
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
    runs, errors = procedures_util.unpack_single_run_meta(storage, data["meta"]["qc_meta"], data["data"])

    if "options" in data["meta"]:
        keywords = storage.get_options([(data["meta"]["program"], data["meta"]["options"])])["data"][0]
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

        # Add to args document to carry through to storage
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

    query = storage.get_procedures([{"hash_index": duplicate_lookup}], projection={"hash_index": True, "id": True})["data"]
    if len(query):
        found_hashes = set(x["hash_index"] for x in query)

        # Filter out tasks
        new_tasks = []
        for task in full_tasks:
            if task["hash_index"] in found_hashes:
                continue
            else:
                new_tasks.append(task)

        if duplicate_id == "hash_index":
            duplicates = list(found_hashes)
        elif duplicate_id == "id":
            duplicates = [x["id"] for x in query]
        else:
            raise KeyError("Duplicate id '{}' not understood".format(duplicate_id))

        return (new_tasks, duplicates, errors)

    else:
        return (full_tasks, [], errors)


def procedure_optimization_output_parser(storage, data):

    new_procedures = {}

    # Each optimization is a unique entry:
    for k, (v, hooks) in data.items():

        # Convert start/stop molecules to hash
        mols = {"initial": v["initial_molecule"], "final": v["final_molecule"]}
        mol_keys = storage.add_molecules(mols)["data"]
        v["initial_molecule"] = mol_keys["initial"]
        v["final_molecule"] = mol_keys["final"]

        # Add individual computations
        traj_dict = {k: v for k, v in enumerate(v["trajectory"])}
        results = procedures_util.parse_single_runs(storage, traj_dict)

        ret = storage.add_results(list(results.values()))
        v["trajectory"] = [x[1] for x in ret["data"]]

        # Coerce tags
        v.update(v["qcfractal_tags"])
        del v["input_specification"]
        del v["qcfractal_tags"]
        # print("Adding optimization result")
        # print(json.dumps(v, indent=2))
        new_procedures[k] = v

    ret = storage.add_procedures(list(new_procedures.values()))

    hook_data = procedures_util.parse_hooks(data, new_procedures)

    return (ret, hook_data)


# Add in all registered procedures
add_new_procedure("single", procedure_single_input_parser, procedure_single_output_parser)
add_new_procedure("optimization", procedure_optimization_input_parser, procedure_optimization_output_parser)
