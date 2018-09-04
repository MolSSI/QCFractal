"""
A factory for Database sockets
"""


def storage_socket_factory(url,
                           port,
                           project_name,
                           username=None,
                           password=None,
                           storage_type="mongo",
                           logger=None,
                           **kwargs):
    """
    Factory for generating storage sockets. Spins up a given storage layer on request given common inputs.

    Right now only supports MongoDB

    Parameters
    ----------
    url : string
        URL-resolvable string to fetch the server from
    port : int
        URL-resolvable port at the ``url`` to resolve
    project_name : string
        Name of the project
    username : string, Optional. Default: None
        Username to access the storage, if required
    password : string, Optional. Default: None
        Username to access the storage, if required
    storage_type : string, Default: "mongo"
        What type of storage socket to spin up. Required
        Valid options:
            "mongo" : Mongo DB Socket
    logger : logging.Logger, Optional, Default: None
        Specific logger to report to
    **kwargs
        Additional keyword arguments to pass to the storage constructor

    Returns
    -------

    """

    if storage_type == "mongo":
        from . import mongo_socket
        return mongo_socket.MongoSocket(
            url, port, project=project_name, username=username, password=password, logger=logger, **kwargs)
    else:
        raise Exception
