"""
Utility functions for handling network ports
"""

import socket


def find_port() -> int:
    sock = socket.socket()
    sock.bind(("localhost", 0))
    host, port = sock.getsockname()
    return port


def is_port_open(ip: str, port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        try:
            s.connect((ip, int(port)))
            s.shutdown(socket.SHUT_RDWR)
            return True
        except ConnectionRefusedError:
            return False
