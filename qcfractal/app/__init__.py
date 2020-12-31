from flask import Flask
from .config import config
# from flask_cors import CORS
from flask_jwt_extended import JWTManager
import logging

logger = logging.getLogger(__name__)

jwt = JWTManager()
# cors = CORS()


def create_app(config_name='default', **objects):
    logger.info(f"Creating flask app with config {config_name}")

    app = Flask(__name__)

    app.config.from_object(config[config_name])
    config[config_name].init_app(app)

#     cors.init_app(app)
    jwt.init_app(app)

    ### Add Fractal config, like storage, logger, etc
    app.config.objects = objects  # TODO: check if needed


    app.config.storage = objects["storage"]
    app.config.logger = objects["logger"]
    app.config.api_logger = objects["api_logger"]
    app.config.view_handler = objects["view_handler"]
    app.config.public_information = objects["public_information"]
    app.config.JWT_ENABLED = objects["JWT_ENABLED"]
    app.config.ALLOW_READ = objects['ALLOW_READ']


    # TODO: not tested
    if app.config['SSL_REDIRECT']:
        from flask_sslify import SSLify
        sslify = SSLify(app)


    logger.debug("Adding blueprints..")

    # The main application entry
    from .routes import main
    app.register_blueprint(main)

    return app
