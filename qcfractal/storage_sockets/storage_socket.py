"""
A factory for Database sockets
"""


def storage_socket_factory(uri, project_name, logger=None, db_type='mongoengine', **kwargs):
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
    db_type : string, Optional, Default: 'pymongo'
        socket type, 'pymongo' or 'mongoengine'
    **kwargs
        Additional keyword arguments to pass to the storage constructor

    Returns
    -------

    """

    if db_type == "mongoengine":
        from . import mongoengine_socket
        return mongoengine_socket.MongoengineSocket(uri, project=project_name, logger=logger, **kwargs)
    else:
        raise KeyError("DBType {} not understood".format(db_type))
