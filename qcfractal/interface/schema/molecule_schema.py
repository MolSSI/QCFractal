"""
The json-schema for the Molecule definition
"""

molecule_schema = {
    "$schema":
    "http://json-schema.org/draft-04/schema#",
    "properties": {
        "symbols": {
            "description": "The atom symbol for each atom in the molecule.",
            "type": "array",
            "items": {
                "type": "string"
            }
        },
        "geometry": {
            "description": "The 3N XYZ coordinates of the atoms involved.",
            "type": "array",
            "items": {
                "type": "number"
            }
        },
        "masses": {
            "description": "The masses of the atoms in the molecule, canonical weights assumed if not given.",
            "type": "array",
            "items": {
                "type": "number"
            }
        },
        "name": {
            "description": "The name of the molecule.",
            "type": "string"
        },
        "comment": {
            "description": "Any additional comment one would attach to the molecule.",
            "type": "string"
        },
        "charge": {
            "description": "The overall charge of the molecule.",
            "type": "number",
            "default": 0.0
        },
        "multiplicity": {
            "description": "The overall multiplicity of the molecule.",
            "type": "number",
            "multipleOf": 1.0,
            "default": 1
        },
        "real": {
            "description": "A list describing if the atoms are real or ghost.",
            "type": "array",
            "items": {
                "type": "boolean"
            }
        },
        "fragments": {
            "description": "A list of indices (0-indexed) for molecular fragments within the topology.",
            "type": "array",
            "items": {
                "type": "array",
                "items": {
                    "type": "number",
                    "multipleOf": 1.0
                }
            }
        },
        "fragment_charges": {
            "description": "A list of charges associated with the fragments tuple.",
            "type": "array",
            "items": {
                "type": "number"
            }
        },
        "fragment_multiplicities": {
            "description": "A list of multiplicites associated with each fragment.",
            "type": "array",
            "items": {
                "type": "number",
                "multipleOf": 1.0
            }
        },
        "fix_com": {
            "description": "Whether to adjust to the molecule to the COM or not.",
            "type": "boolean",
            "default": False
        },
        "fix_orientation": {
            "description": "Whether to rotate the molecule to a standard orientation or not.",
            "type": "boolean",
            "default": False
        },
        "provenance": {
            "type": "object",
            "$ref": "#/definitions/provenance"
        }
    },
    "required": ["symbols", "geometry"],
    "description":
    "The physical cartesian representation of the molecular system",
    "definitions": {},
    "additionalProperties":
    False,

    # Custom components
    "hash_fields": [
        "symbols", "masses", "charge", "multiplicity", "real", "geometry", "fragments", "fragment_charges",
        "fragment_multiplicities"
    ],
    "requied_definitions": ["provenance"]
}
