from flask import Flask
from .config import config
import yaml
import atexit

# from flask_cors import CORS
from flask_jwt_extended import JWTManager
import logging

from ..config import FractalConfig
from ..periodics import QCFractalPeriodics

from qcfractal.storage_sockets.sqlalchemy_socket import SQLAlchemySocket
from qcfractal.storage_sockets import ViewHandler, API_AccessLogger


# The root level logger
logging.basicConfig(format='[%(asctime)s] %(levelname)8s: %(name)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S %Z')


storage_socket = SQLAlchemySocket()
api_logger = API_AccessLogger()
view_handler = ViewHandler()

jwt = JWTManager()
# cors = CORS()


def create_app(config_name="default"):
    logging.getLogger().setLevel('DEBUG')

    logger = logging.getLogger('qcfractal_flask')
    logger.info(f"Creating flask app with config {config_name}")

    app = Flask(__name__)

    # Load the defaults for the Flask configuration
    app.config.from_object(config[config_name])

    # Probably not needed?
    # config[config_name].init_app(app)

    # Load QCFractal settings
    qcf_cfg_path = '/home/ben/programming/qca/servers/sql_refactor/qcfractal_config.yaml'
    with open(qcf_cfg_path, 'r') as f_cfg:
        qcf_cfg = yaml.safe_load(f_cfg)
    qcf_cfg = FractalConfig(**qcf_cfg)

    # Store for later use
    app.config["QCFRACTAL_CONFIG"] = qcf_cfg

    # cors.init_app(app)
    jwt.init_app(app)

    # Initialize the database socket, API logger, and view handler
    storage_socket.init_app(qcf_cfg)
    api_logger.init_app(qcf_cfg)
    view_handler.init_app(qcf_cfg)

    # TODO FIXME
    app.config.JWT_ENABLED = False
    app.config.ALLOW_READ = True

    #logger.debug("Adding blueprints..")

    # Start periodic services
    # TODO FIXME
    start_periodics = True

    if start_periodics:
        logger.info("Starting periodics")
        periodics = QCFractalPeriodics(storage_socket, qcf_cfg)
        periodics.start()

    atexit.register(periodics.stop)

    # The main application entry
    from .routes import main

    app.register_blueprint(main)

    return app
