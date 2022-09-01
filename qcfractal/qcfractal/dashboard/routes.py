from qcfractal.flask_app import dashboard


@dashboard.route("/dashboard", methods=["GET"])
def dashboard_home():
    return "<html><h1>Hello</h1></html>", 200
