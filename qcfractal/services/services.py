"""
Manipulates available services.
"""

from .gridoptimization_service import GridOptimizationService
from .torsiondrive_service import TorsionDriveService

__all__ = ["initialize_service", "construct_service"]


def _service_chooser(name):
    """
    Choose the correct service
    """
    name = name.lower()
    if name == "torsiondrive":
        return TorsionDriveService
    elif name == "gridoptimization":
        return GridOptimizationService
    else:
        raise KeyError("Name {} not recognized.".format(name.title()))


def initialize_service(storage_socket, service_input):
    """Initializes a service from a API call

    Parameters
    ----------
    name : str
        Description
    storage_socket : StorageSocket
        A StorageSocket to the currently active database
    meta : dict
        A JSON blob with the required keys for the service.
    molecule : dict
        A JSON Molecule object

    Returns
    -------
    Service
        Returns an instantiated service

    """
    name = service_input.procedure
    return _service_chooser(name).initialize_from_api(storage_socket, service_input)


def construct_service(storage_socket, data):
    """Initializes a service from a JSON blob

    Parameters
    ----------
    storage_socket : StorageSocket
        A StorageSocket to the currently active database
    data : dict
        The associated JSON blob with the service

    Returns
    -------
    Service
        Returns an instantiated service

    """
    name = data["service"]
    return _service_chooser(name)(**data, storage_socket=storage_socket)
