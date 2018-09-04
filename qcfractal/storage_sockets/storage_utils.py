"""
Contains a number of utility functions for storage sockets
"""

import json

# Constants
_get_metadata = json.dumps({"errors": [], "n_found": 0, "success": False, "error_description": False, "missing": []})


def translate_molecule_index(index):
    if index in ["id", "ids"]:
        return "_id"
    elif index == "hash":
        return "molecule_hash"
    elif index in ["_id", "molecule_hash"]:
        return index
    elif index == "molecular_formula":
        return index
    else:
        raise KeyError("Molecule Index '{}' not understood".format(index))


def translate_generic_index(index):
    if index in ["id", "ids"]:
        return "_id"
    elif index in ["key"]:
        return "key"
    else:
        raise KeyError("Generic Index '{}' not understood".format(index))


def get_metadata():
    """
    Returns a copy of the metadata for database getters
    """
    return json.loads(_get_metadata)


def mixed_molecule_get(socket, data):
    """
    Creates a mixed molecule getter so both molecule_id's and/or molecules can be supplied.

    """

    meta = get_metadata()

    dict_mols = {}
    id_mols = {}
    for idx, mol in data.items():
        if isinstance(mol, str):
            id_mols[idx] = mol
        elif isinstance(mol, dict):
            dict_mols[idx] = mol
        else:
            meta["errors"].append((idx, "Data type not understood"))

    ret_mols = {}

    # Add all new molecules
    id_mols.update(socket.add_molecules(dict_mols)["data"])

    # Get molecules by index and translate back to dict
    tmp = socket.get_molecules(list(id_mols.values()), index="id")
    id_mols_list = tmp["data"]
    meta["errors"].append(tmp["meta"]["errors"])

    inv_id_mols = {v: k for k, v in id_mols.items()}

    for mol in id_mols_list:
        ret_mols[inv_id_mols[mol["id"]]] = mol

    meta["success"] = True
    meta["n_found"] = len(ret_mols)
    meta["missing"] = list(data.keys() - ret_mols.keys())
    return {"meta": meta, "data": ret_mols}
