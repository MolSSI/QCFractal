"""
Assists in grabbing the requisite schema
"""

import copy

from .definitions_schema import get_definition
from .molecule_schema import molecule_schema
from .options_schema import options_schema

__all__ = [
    "get_schema", "get_table_indices", "get_schema_keys", "get_hash_fields", "format_result_indices"
]

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

    "molecule": ("molecule_hash", "molecular_formula"),
    "result": ("molecule", "program", "driver", "method", "basis", "options"),  # ** Renamed molecule_id
    "options": ("program", "name"),

    # "task_queue": ("status", "tag", "hash_index"),
    "task_queue": ("status", "tag", "base_result"),  # updated
    "service_queue": ("status", "tag", "hash_index"),
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
    return program, data["molecule"], data["driver"], data["method"], data["basis"], data["options"]


def get_schema(name):
    if name not in _schemas:
        raise KeyError("Schema name {} not found.".format(name))
    return copy.deepcopy(_schemas)


def get_schema_keys(name):
    if name not in _schemas:
        raise KeyError("Schema name {} not found.".format(name))
    return _schemas[name]["properties"].keys()
