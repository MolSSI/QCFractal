"""
Contains a number of utility functions for storage sockets
"""

import json

# Constants
_get_metadata = json.dumps({"errors": [], "n_found": 0, "success": False, "missing": [],
                            "error_description": False})

_add_metadata = json.dumps({"errors": [], "n_inserted": 0, "success": False, "duplicates": [],
                            "error_description": False, "validation_errors": []})


def translate_molecule_index(index):
    if index in ["_id", "ids"]:
        return "id"
    elif index == "hash":
        return "molecule_hash"
    elif index in ["id", "molecule_hash"]:
        return index
    elif index == "molecular_formula":
        return index
    else:
        raise KeyError("Molecule Index '{}' not understood".format(index))


def translate_generic_index(index):
    if index in ["_id", "id", "ids"]:
        return "_id"
    elif index in ["key"]:
        return "key"
    else:
        raise KeyError("Generic Index '{}' not understood".format(index))


def get_metadata_template():
    """
    Returns a copy of the metadata for database getters
    """
    return json.loads(_get_metadata)


def add_metadata_template():
    """
    Returns a copy of the metadata for database save/updates
    """
    return json.loads(_add_metadata)
