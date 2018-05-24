"""
Contains a number of utility functions for database sockets
"""

def translate_molecule_index(index):
    if index in ["id", "ids"]:
        return "_id"
    elif index == "hash":
        return "molecule_hash"
    elif index in ["_id", "molecule_hash"]:
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
