"""
Assists in grabbing the requisite schema
"""

import copy
import json

import jsonschema

from .definitions_schema import get_definition
from .molecule_schema import molecule_schema
from .options_schema import options_schema

__all__ = ["get_schema", "get_table_indices", "get_schema_keys", "validate", "get_hash_fields", "format_result_indices"]

_schemas = {}

# Add in molecule
for req in molecule_schema["required_definitions"]:
    molecule_schema["definitions"][req] = get_definition(req)

_schemas["molecule"] = molecule_schema
_schemas["options"] = options_schema

# Load molecule schema

# Collection and hash indices
_table_indices = {

    "collection": ("collection", "name"),
    "procedure": ("procedure", "program"),
    "service_queue": ("status", "hash_index", "status", "tag"),

    "molecule": ("molecule_hash", "molecular_formula"),
    "result": ("molecule_id", "program", "driver", "method", "basis", "options"),
    "options": ("program", "name"),

    "task_queue": ("status", "hash_index", "tag"),
}  # yapf: disable


def get_hash_fields(name):
    if name not in _schemas:
        raise KeyError("Schema name {} not found.".format(name))
    return copy.deepcopy(_schemas[name]["hash_fields"])


def get_table_indices(name):
    if name not in _table_indices:
        raise KeyError("Indices for {} not found.".format(name))
    return _table_indices[name]


def format_result_indices(data, program=None):
    if program is None:
        program = data["program"]
    return program, data["molecule_id"], data["driver"], data["method"], data["basis"], data["options"]


def get_schema(name):
    if name not in _schemas:
        raise KeyError("Schema name {} not found.".format(name))
    return copy.deepcopy(_schemas)


def get_schema_keys(name):
    if name not in _schemas:
        raise KeyError("Schema name {} not found.".format(name))
    return _schemas[name]["properties"].keys()


def validate(data, schema_name, return_errors=False):
    if schema_name not in _schemas:
        raise KeyError("Schema name {} not found.".format(schema_name))

    error_gen = jsonschema.Draft4Validator(_schemas[schema_name]).iter_errors(data)
    errors = [x for x in error_gen]
    if len(errors):
        if return_errors:
            return errors
        else:
            error_msg = "Error validating schema '{}'!\n".format(schema_name)
            error_msg += "Data: \n" + json.dumps(data, indent=2)
            error_msg += "\n\nJSON Schema errors as follow:\n"
            error_msg += "\r".join(x.message for x in errors)
            error_msg += "\n"

            raise ValueError(error_msg)
    else:
        return True
