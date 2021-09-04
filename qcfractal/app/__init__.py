from __future__ import annotations

from .config import config

# Forward these to the other flask_app file
from .flask_app import storage_socket, view_handler, jwt  # ,cors
from .flask_app import create_qcfractal_flask_app
