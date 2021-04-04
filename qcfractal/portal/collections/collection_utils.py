"""
A set of utility functions to help the Collections
"""

import math
from typing import Any, Dict, List

__registered_collections = {}


def register_collection(collection: "Collection") -> None:
    """Registers a collection for use by the factory.

    Parameters
    ----------
    collection : Collection
        The Collection class to be registered

    """

    class_name = collection.__name__.lower()
    __registered_collections[class_name] = collection


def collection_factory(data: Dict[str, Any], client: "PortalClient" = None) -> "Collection":
    """Returns a Collection object from data deserialized from JSON.

    Parameters
    ----------
    data : Dict[str, Any]
        The JSON blob to create a new class from.
    client : PortalClient, optional
        A PortalClient connected to a server.

    Returns
    -------
    Collection
        An object representation of the data.

    """
    if "collection" not in data:
        raise KeyError("Attempted to create Collection from data, but no `collection` field found.")

    if data["collection"].lower() not in __registered_collections:
        raise KeyError("Attempted to create Collection of unknown type '{}'.".format(data["collection"]))

    return __registered_collections[data["collection"].lower()].from_dict(data, client=client)


def collections_name_map() -> Dict[str, str]:
    """
    Returns a map of internal name to external Collection name.

    Returns
    -------
    Dict[str, str]
        Map of {'internal': 'user fiendly name'}
    """
    return {k: v.__name__ for k, v in __registered_collections.items()}
