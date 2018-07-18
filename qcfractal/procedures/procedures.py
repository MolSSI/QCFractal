"""
A base for all procedures involved in on-node computation.
"""

import qcengine

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

add_new_procedure("single", procedure_single_input_parser, procedure_single_output_parser)

def procedure_optimization():

    # qc_schema_input = {
    #     "schema_name": "qc_schema_input",
    #     "schema_version": 1,
    #     "molecule": {
    #         "geometry": [
    #             0.0,  0.0, -0.6,
    #             0.0,  0.0,  0.6,
    #         ],
    #         "symbols": ["H", "H"],
    #         "connectivity": [[0, 1, 1]]
    #     },
    #     "driver": "gradient",
    #     "model": {
    #         "method": "HF",
    #         "basis": "sto-3g"
    #     },
    #     "keywords": {},
    # }
    #     json_data = {
    #     "schema_name": "qc_schema_optimization_input",
    #     "schema_version": 1,
    #     "keywords": {
    #         "coordsys": "tric",
    #         "maxiter": 100,
    #         "program": "psi4"
    #     },
    #     "input_specification": qc_schema_input
    # }

    json_data = {
        "meta": {
            "procedure": "single",
            "options": "default",
            "program": "geometric",
            "qc_schema": {
                "driver": "energy",
                "method": "HF",
                "basis": "sto-3g",
                "options": "default",
            },
        },
        "data": [mol_ret["data"]["hydrogen"]],
    }

    # }
