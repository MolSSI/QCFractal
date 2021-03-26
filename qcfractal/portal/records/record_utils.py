from typing import Any, Dict, Optional


__registered_records = {}


def register_record(record: "RecordBase") -> None:
    """Registers a record class for use by the factory.

    Parameters
    ----------
    record : Record
        The Record class to be registered

    """

    class_name = record.__name__.lower()
    __registered_records[class_name] = record


def record_factory(data: Dict[str, Any], client: "PortalClient" = None) -> "RecordBase":
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
    return __registered_records[data["procedure"].lower()].from_json(data, client=client)


def record_name_map() -> Dict[str, str]:
    """
    Returns a map of internal name to external Record name.

    Returns
    -------
    Dict[str, str]
        Map of {'internal': 'user fiendly name'}
    """
    return {k: v.__name__ for k, v in __registered_records.items()}


#def build_procedure(
#    data: Dict[str, Any], procedure: Optional[str] = None, client: Optional["FractalClient"] = None
#) -> "BaseRecord":
#    """
#    Constructs a Service ORM from incoming JSON data.
#
#    Parameters
#    ----------
#    data : Dict[str, Any]
#        A JSON representation of the procedure.
#    procedure : Optional[str], optional
#        The name of the procedure. If blank the procedure name is pulled from the `data["procedure"]` field.
#    client : Optional['FractalClient'], optional
#        A FractalClient connected to a server.
#
#    Returns
#    -------
#    ret : BaseRecord
#        Returns an interface object of the appropriate procedure.
#
#    Examples
#    --------
#
#    # A partial example of torsiondrive metadata
#    >>> data = {
#        "procedure": "torsiondrive",
#        "initial_molecule": "5b7f1fd57b87872d2c5d0a6c",
#        "state": "RUNNING",
#        "id": "5b7f1fd57b87872d2c5d0a6d",
#        ....
#    }
#
#    >>> build_orm(data)
#    TorsionDriveRecord(id='5b7f1fd57b87872d2c5d0a6c', state='RUNNING', molecule_id='5b7f1fd57b87872d2c5d0a6c', molecule_name='HOOH')
#    """
#
#    if ("procedure" not in data) and (procedure is None):
#        raise KeyError("There is not a procedure tag and procedure is none. Unable to determine procedure type")
#
#    # import json
#    # print(json.dumps(data, indent=2))
#    if data["procedure"].lower() == "single":
#        return ResultRecord(**data, client=client)
#    elif data["procedure"].lower() == "torsiondrive":
#        return TorsionDriveRecord(**data, client=client)
#    elif data["procedure"].lower() == "gridoptimization":
#        return GridOptimizationRecord(**data, client=client)
#    elif data["procedure"].lower() == "optimization":
#        return OptimizationRecord(**data, client=client)
#    else:
#        raise KeyError("Service names {} not recognized.".format(data["procedure"]))
