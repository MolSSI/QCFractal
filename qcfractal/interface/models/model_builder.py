from typing import Any, Dict, Optional

from .gridoptimization import GridOptimizationRecord
from .records import OptimizationRecord, ResultRecord
from .torsiondrive import TorsionDriveRecord


def build_procedure(
    data: Dict[str, Any], procedure: Optional[str] = None, client: Optional["FractalClient"] = None
) -> "BaseRecord":
    """
    Constructs a Service ORM from incoming JSON data.

    Parameters
    ----------
    data : Dict[str, Any]
        A JSON representation of the procedure.
    procedure : Optional[str], optional
        The name of the procedure. If blank the procedure name is pulled from the `data["procedure"]` field.
    client : Optional['FractalClient'], optional
        A FractalClient connected to a server.

    Returns
    -------
    ret : BaseRecord
        Returns an interface object of the appropriate procedure.

    Examples
    --------

    # A partial example of torsiondrive metadata
    >>> data = {
        "procedure": "torsiondrive",
        "initial_molecule": "5b7f1fd57b87872d2c5d0a6c",
        "state": "RUNNING",
        "id": "5b7f1fd57b87872d2c5d0a6d",
        ....
    }

    >>> build_orm(data)
    TorsionDriveRecord(id='5b7f1fd57b87872d2c5d0a6c', state='RUNNING', molecule_id='5b7f1fd57b87872d2c5d0a6c', molecule_name='HOOH')
    """

    if ("procedure" not in data) and (procedure is None):
        raise KeyError("There is not a procedure tag and procedure is none. Unable to determine procedure type")

    # import json
    # print(json.dumps(data, indent=2))
    if data["procedure"].lower() == "single":
        return ResultRecord(**data, client=client)
    elif data["procedure"].lower() == "torsiondrive":
        return TorsionDriveRecord(**data, client=client)
    elif data["procedure"].lower() == "gridoptimization":
        return GridOptimizationRecord(**data, client=client)
    elif data["procedure"].lower() == "optimization":
        return OptimizationRecord(**data, client=client)
    else:
        raise KeyError("Service names {} not recognized.".format(data["procedure"]))
