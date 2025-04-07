from flask import Blueprint

from qcfractal.flask_app.load_user import load_logged_in_user

compute_v1 = Blueprint("compute", __name__, url_prefix="/compute/v1")


@compute_v1.before_request
def load_user_info():
    load_logged_in_user()
