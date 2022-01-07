from typing import Any, Dict, Optional


__registered_records = {}


def register_record(record: "RecordBase") -> None:
    """Registers a record class for use by the factory.

    Parameters
    ----------
    record : Record
        The Record class to be registered

    """

    record_type = record._type
    __registered_records[record_type] = record


def record_factory(data: Dict[str, Any], client: Optional["PortalClient"] = None) -> "RecordBase":
    """Returns a Record object from data deserialized from JSON.

    Parameters
    ----------
    data : Dict[str, Any]
        The JSON-serializable dict to create a new class from.
    client : PortalClient, optional
        A PortalClient connected to a server.

    Returns
    -------
    Record
        An object representation of the data.

    """
    if "procedure" not in data:
        raise KeyError("Attempted to create Record from data, but no `procedure` field found.")

    if data["procedure"].lower() not in __registered_records:
        raise KeyError("Attempted to create Record of unknown type '{}'.".format(data["procedure"]))

    # TODO: return here after fixing `from_json`, `to_json` to be less ambiguous
    return __registered_records[data["procedure"].lower()].from_dict(data, client=client)


def record_name_map() -> Dict[str, str]:
    """
    Returns a map of internal name to external Record name.

    Returns
    -------
    Dict[str, str]
        Map of {'internal': 'user fiendly name'}
    """
    return {k: v.__name__ for k, v in __registered_records.items()}
