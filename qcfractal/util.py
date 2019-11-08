"""
Utility functions for QCFractal.
"""

import socket


def find_port() -> int:
    sock = socket.socket()
    sock.bind(("", 0))
    host, port = sock.getsockname()
    return port


def is_port_open(ip: str, port: int) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1)
    try:
        s.connect((ip, int(port)))
        s.shutdown(socket.SHUT_RDWR)
        return True
    except ConnectionRefusedError:
        return False
    finally:
        s.close()
