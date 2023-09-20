from typing import Optional

from flask import Blueprint, current_app, redirect, abort, send_from_directory

home_v1 = Blueprint("home", __name__)


@home_v1.route("/", methods=["GET"])
@home_v1.route("/<path:file_path>", methods=["GET"])
def homepage(file_path: Optional[str] = None):
    # If the root is accessed, serve the static homepage site or do a redirect
    # If a specific file is accessed, only try to serve it from the directory

    homepage_redirect_url = current_app.config["QCFRACTAL_CONFIG"].homepage_redirect_url
    homepage_dir = current_app.config["QCFRACTAL_CONFIG"].homepage_directory

    if homepage_dir is not None:
        if file_path is None:
            file_path = "index.html"

        print("SENDING", homepage_dir, file_path)
        return send_from_directory(homepage_dir, file_path)
    if homepage_redirect_url is not None and file_path is None:
        # This should probably only be a temporary redirect (code 302).
        # This would make it easier to change the redirect in the settings, since
        # browsers shouldn't cache it
        return redirect(homepage_redirect_url, 302)

    # No homepage dir/redirect specified
    return abort(404)
