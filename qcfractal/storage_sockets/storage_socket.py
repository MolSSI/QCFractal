"""
A factory for Database sockets
"""


def storage_socket_factory(uri, project_name, logger=None, db_type=None, **kwargs):
    """
    Factory for generating storage sockets. Spins up a given storage layer on request given common inputs.

    Right now only supports MongoDB.

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

    if db_type is None:
        # try to find db_type from uri
        if uri.startswith('postgresql') or uri.startswith('sqlite'):
            db_type = 'sqlalchemy'
        elif uri.startswith('mongodb'):
            db_type = 'mongoengine'
        else:
            raise TypeError('Unknown DB type, uri: {}'.format(uri))

    if db_type == "mongoengine":
        from . import mongoengine_socket
        return mongoengine_socket.MongoengineSocket(uri, project=project_name, logger=logger, **kwargs)
    elif db_type == "sqlalchemy":
        from . import sqlalchemy_socket
        return sqlalchemy_socket.SQLAlchemySocket(uri, project=project_name, logger=logger, **kwargs)
    else:
        raise KeyError("DBType {} not understood".format(db_type))
