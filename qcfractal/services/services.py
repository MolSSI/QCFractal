"""
Manipulates available services.
"""

from .gridoptimization_service import GridOptimizationService
from .torsiondrive_service import TorsionDriveService

__all__ = ["initialize_service", "construct_service"]


def _service_chooser(name):
    """
    Choose the correct service.
    """
    name = name.lower()
    if name == "torsiondrive":
        return TorsionDriveService
    elif name == "gridoptimization":
        return GridOptimizationService
    else:
        raise KeyError("Name {} not recognized.".format(name.title()))


def initialize_service(storage_socket, logger, service_input, tag=None, priority=None):
    """Initializes a service from a API call.

    Parameters
    ----------
    storage_socket : StorageSocket
        A StorageSocket to the currently active database
    logger
        A logger for use by the service
    service_input
        The service to be initialized.
    tag : Optional
        Optional tag to user with the service. Defaults to None
    priority :
        The priority of the service.

    Returns
    -------
    Service
        Returns an instantiated service

    """
    name = service_input.procedure
    return _service_chooser(name).initialize_from_api(storage_socket, logger, service_input, tag=tag, priority=priority)


def construct_service(storage_socket, logger, data):
    """Initializes a service from a JSON blob.

    Parameters
    ----------
    storage_socket : StorageSocket
        A StorageSocket to the currently active database
    logger
        A logger for use by the service
    data : dict
        The associated JSON blob with the service

    Returns
    -------
    Service
        Returns an instantiated service

    """
    name = data["service"]
    return _service_chooser(name)(**data, storage_socket=storage_socket, logger=logger)
