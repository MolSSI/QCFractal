"""
Manipulates available services.
"""

from .torsiondrive_service import TorsionDriveService

__all__ = ["initializer", "build"]


def initializer(name, storage_socket, meta, molecule):
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
    name = name.lower()
    if name == "torsiondrive":
        return TorsionDriveService.initialize_from_api(storage_socket, meta, molecule)
    else:
        raise KeyError("Name {} not recognized.".format(name.title()))


def build(name, storage_socket, data):
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
    name = name.lower()
    if name == "torsiondrive":
        return TorsionDriveService(storage_socket, data)
    else:
        raise KeyError("Name {} not recognized.".format(name.title()))
