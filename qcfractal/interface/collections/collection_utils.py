"""
A set of utility functions to help the Collections
"""

import math

__registered_collections = {}


def nCr(n, r):
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


def register_collection(collection):
    """Registers a collection for the factory

    Parameters
    ----------
    collection : Collection
        The Collection class to be registered
    """

    class_name = collection.__name__.lower()
    if class_name in __registered_collections:
        raise KeyError("Collection type '{}' already registered".format(class_name))
    __registered_collections[class_name] = collection


def collection_factory(data, client=None):
    """Creates a new Collection class from a JSON blob.

    Parameters
    ----------
    data : dict
        The JSON blob to create a new class from.
    client : client.FractalClient
        A Portal client to connected to a server

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
