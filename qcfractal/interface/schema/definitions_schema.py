"""
All json-schema definitions for the project
"""

import copy

__all__ = ["get_definition"]

_definitions = {
    "provenance": {
        "properties": {
            "creator": {
                "description": "The name of the person or program who created this object.",
                "type": "string"
            },
            "version": {
                "description": "The version of the program which created this object, blank otherwise.",
                "type": "string"
            },
            "routine": {
                "description": "The routine of the program which created this object, blank otherwise.",
                "type": "string"
            }
        },
        "required": ["creator"],
        "description": "A short provenance of the object.",
        "additionalProperties": True
    }
}


def get_definition(definition):
    if definition not in _definitions:
        raise KeyError("Definition '{}' not present.".format(definition))

    return copy.deepcopy(_definitions[definition])
