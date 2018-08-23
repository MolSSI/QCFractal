"""
Constructs ORMs from raw JSON
"""

from .crank_orm import CrankORM

def build_orm(data, service=None):
    """
    Constructs a Service ORM from incoming JSON data.
    This data should have a "service" label.
    """

    if ("service" not in data) and (service is None):
        raise KeyError("There is not a service tag and service is none. Unable to determine service type")

    if data["service"].lower() == "crank":
        return CrankORM.from_json(data)
    else:
        raise KeyError("Service names {} not recognized.".format(data["service"]))
