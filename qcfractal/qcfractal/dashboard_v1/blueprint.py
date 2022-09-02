from flask import Blueprint

dashboard_v1 = Blueprint(
    "dashboard", __name__, url_prefix="/dashboard/v1", static_folder="static", template_folder="templates"
)
