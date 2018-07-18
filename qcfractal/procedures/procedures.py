"""
A base for all procedures involved in on-node computation.
"""

import qcengine
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
    full_tasks = {}
    for k, v in runs.items():
        key = ("single", ) + k
        full_tasks[key] = (qcengine.compute, v, data["meta"]["program"])

    return (full_tasks, errors)


def procedure_single_output_parser(db, data):
    results = procedures_util.parse_single_runs(db, data)
    ret = db.add_results(list(results.values()))
    return ret

def procedure_optimization_input_parser(db, data):
    """

    json_data = {
        "meta": {
            "procedure": "optimization",
            "options": "default",
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

    keywords = db.get_options([(data["meta"]["program"], data["meta"]["options"])])["data"][0]
    del keywords["program"]
    del keywords["name"]

    keywords["program"] = data["meta"]["qc_meta"]["program"]
    template = json.dumps({
        "schema_name": "qc_schema_optimization_input",
        "schema_version": 1,
        "keywords": keywords,
        "qcfractal_tags": data["meta"]
    })

    full_tasks = {}
    for k, v in runs.items():
        key = ("optimization", ) + k

        # Coerce qc_template information
        packet = json.loads(template)
        packet["initial_molecule"] = v["molecule"]
        del v["molecule"]
        packet["input_specification"] = v


        full_tasks[key] = (qcengine.compute_procedure, packet, data["meta"]["program"])

    return (full_tasks, errors)

def procedure_optimization_output_parser(db, data):

    new_procedures = []

    # Each optimization is a unique entry:
    for k, v in data.items():

        # Convert start/stop molecules to hash
        mols = {"initial": v["initial_molecule"], "final": v["final_molecule"]}
        mol_keys = db.add_molecules(mols)["data"]
        v["initial_molecule"] = mol_keys["initial"]
        v["final_molecule"] = mol_keys["final"]

        # Add individual computations
        traj_dict = {k : v for k, v in enumerate(v["trajectory"])}
        results = procedures_util.parse_single_runs(db, traj_dict)

        ret = db.add_results(list(results.values()))
        v["trajectory"] = [x[1] for x in ret["data"]]

        # Coerce tags
        v.update(v["qcfractal_tags"])
        del v["input_specification"]
        del v["keywords"]
        del v["qcfractal_tags"]
        # print(json.dumps(v, indent=2))
        new_procedures.append(v)

    ret = db.add_procedures(new_procedures)
    return ret

add_new_procedure("single", procedure_single_input_parser, procedure_single_output_parser)
add_new_procedure("optimization", procedure_optimization_input_parser, procedure_optimization_output_parser)
