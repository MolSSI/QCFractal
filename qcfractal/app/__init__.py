from __future__ import annotations

import os

from .config import config
from ..config import read_configuration

# Forward these to the other flask_app file
from .flask_app import storage_socket, api_logger, view_handler, jwt #,cors
from .flask_app import create_qcfractal_flask_app


def create_app(config_name: str ="default"):
    '''
    Default create_app for running a standalone flask server

    This loads qcfractal configuration based on an environment variable
    '''

    # Load QCFractal settings from files.
    # The QCF_CONFIG_PATHS environment variable is a semicolon-separated list of paths
    # to read for configuration (in order, with later paths taking priority)
    config_paths = os.environ.get('QCF_CONFIG_PATHS', None)
    if config_paths is None:
        raise RuntimeError("QCFractal configuration paths must be set using the QCF_CONFIG_PATHS environment variable")

    config_paths = [x.strip() for x in config_paths.split(';')]

    extra_config = {'flask': {'config_name': config_name}}
    qcfractal_config = read_configuration(config_paths.split(';'), extra_config=extra_config)

    return create_qcfractal_flask_app(qcfractal_config)