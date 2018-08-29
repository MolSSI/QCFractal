"""
The json-schema for the Options definition
"""

options_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "properties": {
        "name": {
            "description": "The name of the options definition.",
            "type": "string",
        },
        "program": {
            "description": "The name of the program these options are associated with.",
            "type": "string",
        },
    },
    "required": ["program", "name"],
    "description": "The physical cartesian representation of the molecular system",
    "definitions": {},
    "additionalProperties": True,
}
