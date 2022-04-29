"""
Utility functions for handling network ports
"""

import socket


def find_open_port() -> int:
    """
    Finds an open port that we can bind to
    """

    sock = socket.socket()
    sock.bind(("localhost", 0))
    _, port = sock.getsockname()
    return port


def is_port_inuse(ip: str, port: int) -> bool:
    """
    Determine if an ip/port is being used or not

    Parameters
    ----------
    ip: str
        The host IP address to check
    port: int
        The port on the IP address to check

    Returns
    -------
    bool
        True if the port is currently in use, False otherwise
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        try:
            s.connect((ip, int(port)))
            s.shutdown(socket.SHUT_RDWR)
            return True
        except ConnectionRefusedError:
            return False
