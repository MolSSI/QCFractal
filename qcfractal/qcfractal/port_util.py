"""
Utility functions for handling network ports
"""

import socket
from typing import Optional


def find_open_port(host: str = "localhost", starting_port: Optional[int] = None) -> int:
    """
    Finds an open port that we can bind to
    """

    if starting_port is None:
        sock = socket.socket()
        sock.bind((host, 0))
        _, port = sock.getsockname()
    else:
        port = starting_port
        while is_port_inuse(host, port):
            port += 1

    return port


def is_port_inuse(host: str, port: int) -> bool:
    """
    Determine if an ip/port is being used or not

    Parameters
    ----------
    host
        The host name or IP address to check
    port
        The port on the host to check

    Returns
    -------
    bool
        True if the port is currently in use, False otherwise
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        try:
            s.connect((host, int(port)))
            s.shutdown(socket.SHUT_RDWR)
            return True
        except ConnectionRefusedError:
            return False
