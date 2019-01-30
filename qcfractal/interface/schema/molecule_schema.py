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
        "identifiers": {
            "description": "Canonical chemical identifiers.",
            "type": "object",
            "properties": {
                "molecule_hash": {
                    "description": "A unique hash for molecules specific to QCFractal.",
                    "type": "string"
                },
                "molecular_formula": {
                    "description": "A string giving the symbol and symbol count for the molecule.",
                    "type": "string"
                },
                "smiles": {
                    "description": "Simplified Molecular Input Line Entry System line notation.",
                    "type": "string"
                },
                "inchi": {
                    "description": "IUPAC International Chemical Identifier line notation.",
                    "type": "string"
                },
                "inchikey": {
                    "description": "A SHA1 hash of the inchi description.",
                    "type": "string"
                },
            }
        },
        "comment": {
            "description": "Any additional comment one would attach to the molecule.",
            "type": "string"
        },
        "molecular_charge": {
            "description": "The overall charge of the molecule.",
            "type": "number",
            "default": 0.0
        },
        "molecular_multiplicity": {
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
        "connectivity": {
            "description":
            "(nat, ) list describing bonds within a molecule. Each element is a (atom1, atom2, order) tuple.",
            "type":
            "array",
            "items": {
                "type": "array",
                "minItems": 3,
                "maxItems": 3,
                "items": {
                    "type": "number",
                    "minimum": 0,
                }
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
        "symbols", "masses", "molecular_charge", "molecular_multiplicity", "real", "geometry", "fragments", "fragment_charges",
        "fragment_multiplicities", "connectivity"
    ],
    "required_definitions": ["provenance"]
}
