import os
from typing import Any

from flask import Blueprint, current_app, redirect, abort, send_from_directory, request, Response
from werkzeug.exceptions import NotFound

from qcfractal.flask_app.decorators import no_permission_required

home_v1 = Blueprint("home", __name__)

# Top-level path segments that belong to the server (the api, auth, and compute blueprints).
# Every server route must be under one of these, and the homepage (such as the web portal) must not
# use them. This is checked when the flask app is created
RESERVED_PATH_PREFIXES = ("api", "auth", "compute")

# Placeholder in the homepage's index.html that is replaced with the origin of the site (scheme + host)
_site_origin_placeholder = "__SITE_ORIGIN__"


def _send_index(homepage_dir: str) -> Response:
    # The index can reference the site origin, and must not be cached (so that clients
    # pick up new versions of the hashed assets it references)
    index_path = os.path.join(homepage_dir, "index.html")
    if not os.path.isfile(index_path):
        abort(404)

    with open(index_path, "r") as f:
        index_html = f.read()

    index_html = index_html.replace(_site_origin_placeholder, request.host_url.rstrip("/"))
    response = Response(index_html, mimetype="text/html")
    response.headers["Cache-Control"] = "no-cache"
    return response


@home_v1.route("/", methods=["GET"])
@home_v1.route("/<path:file_path>", methods=["GET"])
@no_permission_required()
def homepage(file_path: str | None = None) -> Any:
    # If the root is accessed, serve the static homepage site or do a redirect
    # If a specific file is accessed, only try to serve it from the directory

    # Unknown paths under the server's prefixes are never part of the homepage
    if file_path is not None and file_path.split("/", 1)[0] in RESERVED_PATH_PREFIXES:
        abort(404)

    homepage_redirect_url = current_app.config["QCFRACTAL_CONFIG"].homepage_redirect_url
    homepage_dir = current_app.config["QCFRACTAL_CONFIG"].homepage_directory

    if homepage_dir is not None:
        if file_path is None or file_path == "index.html":
            return _send_index(homepage_dir)

        try:
            response = send_from_directory(homepage_dir, file_path)
        except NotFound:
            # The homepage may be a single-page app with client-side routing, in which case
            # browsers navigating to one of its routes (rather than requesting a file) get the index.
            # Browsers explicitly ask for html when navigating, but not when loading scripts, images, etc.
            # (which accept */*)
            if "text/html" in request.headers.get("Accept", ""):
                return _send_index(homepage_dir)
            raise

        # Files under assets/ have a content hash in their name, so never change
        if file_path.startswith("assets/"):
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response

    if homepage_redirect_url is not None and file_path is None:
        # This should probably only be a temporary redirect (code 302).
        # This would make it easier to change the redirect in the settings, since
        # browsers shouldn't cache it
        return redirect(homepage_redirect_url, 302)

    # No homepage dir/redirect specified
    return abort(404)
