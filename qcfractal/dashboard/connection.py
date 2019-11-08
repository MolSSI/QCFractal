from flask import current_app, g

from ..storage_sockets import storage_socket_factory


def get_socket():
    if "socket" not in g:
        config = current_app.config["FRACTAL_CONFIG"]
        g.socket = storage_socket_factory(config.database_uri(safe=False), max_limit=50000)

    return g.socket
