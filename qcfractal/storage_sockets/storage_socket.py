"""
A factory for Database sockets
"""


def storage_socket_factory(uri, project_name="", logger=None, db_type=None, **kwargs):
    """
    Factory for generating storage sockets. Spins up a given storage layer on request given common inputs.

    Right now only supports MongoDB.

    Parameters
    ----------
    uri : string
        A URI to given database such as ("postgresql://localhost:5432", )
    project_name : string
        Name of the project
    logger : logging.Logger, Optional, Default: None
        Specific logger to report to
    db_type : string, Optional, Default: 'sqlalchemy'
        socket type, 'sqlalchemy'
    **kwargs
        Additional keyword arguments to pass to the storage constructor

    Returns
    -------

    """

    if db_type is None:
        # try to find db_type from uri
        if uri.startswith("postgresql"):
            db_type = "sqlalchemy"
        else:
            raise TypeError("Unknown DB type, uri: {}".format(uri))

    if db_type == "sqlalchemy":
        from . import sqlalchemy_socket

        return sqlalchemy_socket.SQLAlchemySocket(uri, project=project_name, logger=logger, **kwargs)
    else:
        raise KeyError("DBType {} not understood".format(db_type))
