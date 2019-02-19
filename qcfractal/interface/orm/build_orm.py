"""
Constructs ORMs from raw JSON
"""

from ..models.gridoptimization import GridOptimization
from ..models.torsiondrive import TorsionDrive
from .optimization_orm import OptimizationORM


def build_orm(data, procedure=None, client=None):
    """
    Constructs a Service ORM from incoming JSON data.

    Parameters
    ----------
    data : dict
        A JSON representation of the procedure.
    procedure : None, optional
        The name of the procedure. If blank the procedure name is pulled from the `data["procedure"]` field.
    client : FractalClient, optional
        A activate server connection.

    Returns
    -------
    ret : a ORM-like object
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
    TorsionDrive(id='5b7f1fd57b87872d2c5d0a6c', state='RUNNING', molecule_id='5b7f1fd57b87872d2c5d0a6c', molecule_name='HOOH')
    """

    if ("procedure" not in data) and (procedure is None):
        raise KeyError("There is not a procedure tag and procedure is none. Unable to determine procedure type")

    # import json
    # print(json.dumps(data, indent=2))
    if data["procedure"].lower() == "torsiondrive":
        return TorsionDrive(**data, client=client)
    elif data["procedure"].lower() == "gridoptimization":
        return GridOptimization(**data, client=client)
    elif data["procedure"].lower() == "optimization":
        return OptimizationORM.from_json(data, client=client)
    else:
        raise KeyError("Service names {} not recognized.".format(data["procedure"]))
