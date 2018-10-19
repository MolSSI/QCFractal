"""
A factory for Database sockets
"""


def storage_socket_factory(uri, project_name, logger=None, **kwargs):
    """
    Factory for generating storage sockets. Spins up a given storage layer on request given common inputs.

    Right now only supports MongoDB

    Parameters
    ----------
    uri : string
        A URI to given database such as ("mongodb://localhost:27107", )
    project_name : string
        Name of the project
    logger : logging.Logger, Optional, Default: None
        Specific logger to report to
    **kwargs
        Additional keyword arguments to pass to the storage constructor

    Returns
    -------

    """

    if uri.startswith("mongodb"):
        from . import mongo_socket
        return mongo_socket.MongoSocket(uri, logger=logger, **kwargs)
    else:
        raise Exception
