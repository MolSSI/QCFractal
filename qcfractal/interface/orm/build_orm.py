"""
Constructs ORMs from raw JSON
"""

from .torsiondrive_orm import TorsionDriveORM


def build_orm(data, service=None):
    """
    Constructs a Service ORM from incoming JSON data.

    Parameters
    ----------
    data : dict
        A JSON representation of the service.
    service : None, optional
        The name of the service. If blank the service name is pulled from the `data["service"]` field.

    Returns
    -------
    ret : a ORM-like object
        Returns an interface object of the appropriate service.

    Examples
    --------

    # A partial example of torsiondrive metadata
    >>> data = {
        "service": "torsiondrive",
        "initial_molecule": "5b7f1fd57b87872d2c5d0a6c",
        "state": "RUNNING",
        "id": "5b7f1fd57b87872d2c5d0a6d",
        ....
    }

    >>> build_orm(data)
    TorsionDrive(id='5b7f1fd57b87872d2c5d0a6c', state='RUNNING', molecule_id='5b7f1fd57b87872d2c5d0a6c', molecule_name='HOOH')
    """

    if ("service" not in data) and (service is None):
        raise KeyError("There is not a service tag and service is none. Unable to determine service type")

    if data["service"].lower() == "torsiondrive":
        return TorsionDriveORM.from_json(data)
    else:
        raise KeyError("Service names {} not recognized.".format(data["service"]))
