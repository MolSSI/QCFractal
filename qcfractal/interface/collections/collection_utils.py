"""
A set of utility functions to help the Collections
"""

import math
from typing import Any, Dict, List

__registered_collections = {}


def nCr(n: int, r: int) -> int:
    """
    Compute the binomial coefficient n! / (k! * (n-k)!)

    Parameters
    ----------
    n : int
        Number of samples
    r : int
        Denominator

    Returns
    -------
    ret : int
        Value
    """
    return math.factorial(n) / math.factorial(r) / math.factorial(n - r)


def register_collection(collection: "Collection") -> None:
    """Registers a collection for use by the factory.

    Parameters
    ----------
    collection : Collection
        The Collection class to be registered

    """

    class_name = collection.__name__.lower()
    # if class_name in __registered_collections:
    #     raise KeyError("Collection type '{}' already registered".format(class_name))
    __registered_collections[class_name] = collection


def collection_factory(data: Dict[str, Any], client: "FractalClient" = None) -> "Collection":
    """Creates a new Collection class from a JSON blob.

    Parameters
    ----------
    data : Dict[str, Any]
        The JSON blob to create a new class from.
    client : FractalClient, optional
        A FractalClient connected to a server

    Returns
    -------
    Collection
        A ODM of the data.

    """
    if "collection" not in data:
        raise KeyError("Attempted to create Collection from JSON, but no `collection` field found.")

    if data["collection"].lower() not in __registered_collections:
        raise KeyError("Attempted to create Collection of unknown type '{}'.".format(data["collection"]))

    return __registered_collections[data["collection"].lower()].from_json(data, client=client)


def collections_name_map() -> Dict[str, str]:
    """
    Returns a map of internal name to external Collection name.

    Returns
    -------
    Dict[str, str]
        Map of {'internal': 'user fiendly name'}
    """
    return {k: v.__name__ for k, v in __registered_collections.items()}


def list_known_collections() -> List[str]:
    """
    Returns the case sensitive list of Collection names.

    Returns
    -------
    List[str]
        A list of registered collections
    """
    return list(collection_name_map.values())


def composition_planner(program=None, method=None, basis=None, driver=None, keywords=None):
    """
    Plans out a given query into multiple pieces
    """

    base = {"program": program, "method": method, "basis": basis, "driver": driver, "keywords": keywords}

    if ("-d3" in method.lower()) and ("dftd3" != program.lower()) and ("hessian" != driver.lower()):
        dftd3keys = {"program": "dftd3", "method": method, "basis": None, "driver": driver, "keywords": None}
        base["method"] = method.lower().split("-d3")[0]

        return [dftd3keys, base]

    else:
        return [base]
