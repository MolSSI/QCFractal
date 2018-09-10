"""
The schema for various database specifications
"""

database_schema = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "properties": {
        "program": {
            "type": "string",
            "description": "The QC program to use"
        },
        "options": {
            "type": "string",
            "description": "The name of the options set to use."
        },
        "model": {
            "type": "object",
            "description": "The physical model to apply to the system",
            "properties": {
                "method": {
                    "type": "string",
                },
                "basis": {
                    "type": "string"
                }
            }
        },
        "molecule_hashes": {
            "type": "array",
            "items": {
                "type": "string",
                "description": "The required compute hashes."
            }
        }
    }
}
