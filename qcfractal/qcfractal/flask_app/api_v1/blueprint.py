from flask import Blueprint

from qcfractal.flask_app.load_user import load_logged_in_user

api_v1 = Blueprint("api", __name__, url_prefix="/api/v1")


@api_v1.before_request
def load_user_info():
    load_logged_in_user()
