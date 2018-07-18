"""
A base for all procedures involved in on-node computation.
"""

_input_parsers = {}
_output_parsers = {}

def add_new_procedures(name, creator, unpacker):

    _input_parsers[name] = creator
    _output_parsers[name] = unpacker
    return True


def procedure_qce():

    # compute = {
    #     "meta": {
    #         "procedure": "single",
    #         "driver": "energy",
    #         "method": "HF",
    #         "basis": "sto-3g",
    #         "options": "default",
    #         "program": "psi4",
    #     },
    #     "data": [mol_ret["data"]["hydrogen"]],
    # }

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
                "options": "default,
            },
        },
        "data": [mol_ret["data"]["hydrogen"]],
    }

    # }
