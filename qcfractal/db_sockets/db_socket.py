"""
A factory for Database sockets
"""

def db_socket_factory(url, port, project_name, username=None, password=None, db_type="mongo", logger=None, **kwargs):

    if db_type == "mongo":
        from . import mongo_socket
        return mongo_socket.MongoSocket(url, port, project=project_name, username=username, password=password, logger=logger, **kwargs)
    else:
        raise Exception
