from flask import Blueprint

compute_v1 = Blueprint("compute", __name__, url_prefix="/compute/v1")
