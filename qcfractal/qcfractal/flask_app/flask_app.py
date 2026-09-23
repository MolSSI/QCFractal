from __future__ import annotations

import logging
import queue
from typing import TYPE_CHECKING

from flask import Flask, current_app
from flask_cors import CORS
from flask_jwt_extended import JWTManager
from werkzeug.local import LocalProxy
from werkzeug.routing import IntegerConverter

from .csrf import CSRF_HEADER
from .rate_limit import FlaskLoginRateLimiter
from .user_verify import FlaskUserVerifier, FlaskTokenVerifier
from .flask_session import QCFFlaskSessionInterface
from .flask_socket import FlaskStorageSocket
from ..db_socket import SQLAlchemySocket

if TYPE_CHECKING:
    from ..config import FractalConfig
    from typing import Optional

app_storage_sockets = FlaskStorageSocket()


def _get_storage_socket() -> SQLAlchemySocket:
    return app_storage_sockets.get_socket(current_app._get_current_object())


storage_socket = LocalProxy(_get_storage_socket)

jwt = JWTManager()

# Flask extensions holding per-application state. The objects themselves are module-level
# singletons, but the state they act on belongs to whichever app is handling the current request,
# so two apps in one process never share login counters
login_rate_limiter = FlaskLoginRateLimiter()
user_verifier = FlaskUserVerifier()
token_verifier = FlaskTokenVerifier()


# Some routes allow for negative integers (ie, list index)
# See https://github.com/pallets/flask/issues/2643
class SignedIntConverter(IntegerConverter):
    regex = r"-?\d+"


def create_flask_app(qcfractal_config: FractalConfig, finished_queue: Optional[queue.Queue] = None):
    app = Flask(__name__)

    app.url_map.converters["signed_int"] = SignedIntConverter

    app.logger = logging.getLogger("fractal_flask_app")
    app.logger.info(f"Creating flask app")

    # Read in and store the qcfractal configuration for later use
    app.config["QCFRACTAL_CONFIG"] = qcfractal_config

    # Configure the flask app

    # Some defaults (but can be overridden)
    # must be set to false to avoid restarting
    app.config["DEBUG"] = False

    # Never propagate exceptions. This uses the default error pages
    # which are HTML, but we are using json...
    app.config["PROPAGATE_EXCEPTIONS"] = False

    app.config["SECRET_KEY"] = qcfractal_config.api.secret_key
    app.config["JWT_SECRET_KEY"] = qcfractal_config.api.jwt_secret_key
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = qcfractal_config.api.jwt_access_token_expires
    app.config["JWT_REFRESH_TOKEN_EXPIRES"] = qcfractal_config.api.jwt_refresh_token_expires
    app.config["JWT_TOKEN_LOCATION"] = ["headers"]
    app.config["SESSION_COOKIE_NAME"] = qcfractal_config.api.user_session_cookie_name

    # Where we store user-uploaded files for processing
    app.config["UPLOAD_FOLDER"] = qcfractal_config.upload_directory

    # Any additional configuration
    if qcfractal_config.api.extra_flask_options:
        app.config.update(**qcfractal_config.api.extra_flask_options)

    # The session lifetime must agree with the periodic cleanup of expired sessions, which uses
    # the configuration value, so it is not overridable through extra_flask_options
    app.config["PERMANENT_SESSION_LIFETIME"] = qcfractal_config.api.user_session_max_age

    jwt.init_app(app)

    if qcfractal_config.cors.enabled:
        app.config["CORS_ORIGINS"] = qcfractal_config.cors.origins
        app.config["CORS_SUPPORTS_CREDENTIALS"] = qcfractal_config.cors.supports_credentials

        # Content-Type is needed for JSON bodies, and the CSRF header for all cookie-authenticated
        # requests that change state. Always allow those, in addition to whatever is configured
        allow_headers = list(qcfractal_config.cors.headers)
        for h in ("Content-Type", CSRF_HEADER):
            if h.lower() not in {x.lower() for x in allow_headers}:
                allow_headers.append(h)
        app.config["CORS_ALLOW_HEADERS"] = allow_headers

        if qcfractal_config.cors.methods:
            app.config["CORS_METHODS"] = qcfractal_config.cors.methods

        CORS(app)

    # Initialize the database socket, API logger, and view handler
    app_storage_sockets.init_app(app, finished_queue=finished_queue)

    # Login rate limiting state belongs to this app, not to the process
    login_rate_limiter.init_app(app)
    user_verifier.init_app(app)
    token_verifier.init_app(app)

    # Initialize the session interface after the storage socket
    app.session_interface = QCFFlaskSessionInterface(app)

    # Registers the various error and before/after request handlers
    from . import handlers

    # Register all the routes in the other files.
    # Must be done before registering the blueprint
    from .api_v1 import routes
    from .auth_v1 import routes
    from .compute_v1 import routes
    from ..components import register_all

    from .home_v1 import home_v1
    from .api_v1.blueprint import api_v1
    from .auth_v1.blueprint import auth_v1
    from .compute_v1.blueprint import compute_v1

    app.register_blueprint(home_v1)
    app.register_blueprint(api_v1)
    app.register_blueprint(auth_v1)
    app.register_blueprint(compute_v1)

    # Check through all registered routes to ensure they have permission checks
    for endpoint, view in app.view_functions.items():
        if endpoint == "static":
            continue
        if not hasattr(view, "_has_permission_check"):
            raise RuntimeError(f"Route {endpoint} does not have permission check")

    # A copy of the URL map excluding the homepage blueprint's catch-all route, used by
    # handlers.handle_method_not_allowed to tell a genuine 405 apart from an unknown route
    # (see that function for why the catch-all route makes this necessary)
    from werkzeug.routing import Map

    app.config["ROUTES_WITHOUT_HOMEPAGE_MAP"] = Map(
        [rule.empty() for rule in app.url_map.iter_rules() if rule.endpoint != "home.homepage"]
    )

    return app
