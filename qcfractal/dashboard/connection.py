from flask import current_app, g

from ..storage_sockets import storage_socket_factory

def get_socket():
    if 'socket' not in g:
        g.socket = storage_socket_factory(current_app.config['CONNECTION'])

    return g.socket
