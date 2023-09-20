from qcfractal.flask_app.dashboard_v1.blueprint import dashboard_v1


@dashboard_v1.route("/home", methods=["GET"])
def dashboard_home():
    return "<html><h1>Hi!</h1></html>", 200

    # return render_template("index.html",
    #                       posts={
    #                           'error_log': [],
    #                           'server_information': [],
    #                           'users_set': [],
    #                       })
