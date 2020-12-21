"""
A factory for Database sockets
"""
from . import sqlalchemy_socket


def storage_socket_factory(uri, project_name="", logger=None, **kwargs):
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
    **kwargs
        Additional keyword arguments to pass to the storage constructor

    Returns
    -------

    """

    return sqlalchemy_socket.SQLAlchemySocket(uri, project=project_name, logger=logger, **kwargs)
