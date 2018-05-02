"""
Assists in grabbing the requisite schema
"""

import copy
import glob
import os
import jsonschema
import json

from .definitions_schema import get_definition
from .molecule_schema import molecule_schema
from .options_schema import options_schema

__all__ = ["get_schema", "validate", "get_hash_fields"]

_schemas = {}

# Add in molecule
for req in molecule_schema["requied_definitions"]:
    molecule_schema["definitions"][req] = get_definition(req)

_schemas["molecule"] = molecule_schema
_schemas["options"] = options_schema

# Load molecule schema


def get_hash_fields(name):
    if name not in _schemas:
        raise KeyError("Schema name %s not found." % name)
    return copy.deepcopy(_schemas[name]["hash_fields"])


def get_schema(name):
    if name not in _schemas:
        raise KeyError("Schema name %s not found." % name)
    return copy.deepcopy(_schemas)


def validate(data, schema_name, return_errors=False):
    if schema_name not in _schemas:
        raise KeyError("Schema name %s not found." % name)

    error_gen = jsonschema.Draft4Validator(_schemas[schema_name]).iter_errors(data)
    errors = [x for x in error_gen]
    if len(errors):
        if return_errors:
            return errors
        else:
            error_msg = "Error validating schema '%s'!\n" % schema_name
            error_msg += "Data: \n" + json.dumps(data, indent=2)
            error_msg += "\n\nJSON Schema errors as follow:\n"
            error_msg += "\r".join(x.message for x in errors)
            error_msg += "\n"

            raise ValueError(error_msg)
    else:
        return True
