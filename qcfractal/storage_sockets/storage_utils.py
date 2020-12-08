"""
Contains a number of utility functions for storage sockets.
"""

import json
from typing import Union, List, Sequence

# Constants
_get_metadata = json.dumps({"errors": [], "n_found": 0, "success": False, "missing": [], "error_description": False})

_add_metadata = json.dumps(
    {
        "errors": [],
        "n_inserted": 0,
        "success": False,
        "duplicates": [],
        "error_description": False,
        "validation_errors": [],
    }
)


def get_metadata_template():
    """
    Returns a copy of the metadata for database getters.
    """
    return json.loads(_get_metadata)


def add_metadata_template():
    """
    Returns a copy of the metadata for database save/updates.
    """
    return json.loads(_add_metadata)


def to_pydantic_models(orms: Sequence[Union["Base", None]]) -> List[Union["ProtoModel", None]]:
    return [x.to_pydantic_model() if x is not None else None for x in orms]


def find_indices(list1, list2):
    """
    For each element in list1, find the index of the element in list2

    If, for a value in list1, there are multiple occurances in list2, that index will be duplicated in the
    returned list.

    The resulting list will always be sorted
    """
    ret = []
    for idx, v in enumerate(list1):
        ret.extend(i for i, x in enumerate(list2) if x == v)
    return sorted(ret)
