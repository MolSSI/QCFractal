import shutil
import tempfile
from functools import wraps
from typing import Callable, Optional, Iterable

from flask import current_app

try:
    import pydantic.v1 as pydantic
except ImportError:
    import pydantic
from flask import request, g, Response
from werkzeug.exceptions import BadRequest

from qcfractal.flask_app import storage_socket
from qcportal.serialization import deserialize, serialize


def wrap_global_route(
    requested_resource,
    requested_action,
    require_security: bool = False,
    allowed_file_extensions: Optional[Iterable[str]] = None,
) -> Callable:
    """
    Decorator that wraps a Flask route function, providing useful functionality

    This wrapper handles several things:

        1. Checks the JWT for permission to access this route (with the requested action)
           OR
           Checks the session cookie (for browser-based authentication)
        2. Parses the request body and URL params, and converts them to the appropriate model (see below)
        3. Serializes the response returned from the wrapped function into the appropriate
           type (taken from the accepted mimetypes)

    The data packaged with the request may be json, msgpack, or maybe others in the future.
    This is deserialized and converted to the types needed by the wrapped function. These
    types are read from the type annotations on the wrapped function.

    There are two function parameters that are inspected - `url_params` for the URL parameters,
    and `body_data` for data included in the request body. The type annotations for these
    parameters are read, and then pydantic is used to convert the deserialized request body/params
    into the appropriate type, after which they are passed to the function.

    If `allowed_file_extensions` is not None, then this route will also accept file uploads. The wrapped
    function will be passed the file objects as the `files` parameter. If this parameter does not exist,
    an exception will be raised.

    Parameters
    ----------
    requested_resource
        The name of the major resource
    requested_action
        Type of action that this route handles (read, write, etc)
    allowed_file_extensions
        If not None, files are allowed to be uploaded with this route. The files must have these extensions.
    require_security
        If true, route is only accessible if security is enabled on the server.
    """

    def decorate(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            storage_socket.auth.assert_global_permission(g.role, requested_resource, requested_action, require_security)

            ##################################################################
            # If we got here, then the user is allowed access to this endpoint
            # Continue with parsing their request
            ##################################################################

            content_type = request.headers.get("Content-Type")

            # Find an appropriate return type (from the "Accept" header)
            # Flask helpfully parses this for us
            # By default, use plain json
            possible_types = ["text/html", "application/msgpack", "application/json"]
            accept_type = request.accept_mimetypes.best_match(possible_types, "application/json")

            # If text/html is first, then this is probably a browser. Send json, as most browsers
            # will accept that
            if accept_type == "text/html":
                accept_type = "application/json"

            # get the type annotations for body_model and url_params_model
            # from the wrapped function
            annotations = fn.__annotations__

            body_model = annotations.get("body_data", None)
            url_params_model = annotations.get("url_params", None)

            # If files are allowed, check for the files parameter in the function
            if allowed_file_extensions is not None:
                if "files" not in annotations:
                    raise BadRequest('Route allows uploads, but function doesn\'t have the "files" argument')

            # 1. The body is stored in request.data
            #    However, if it's not there, and we have files, it will be in request.files
            #    (since you cannot send body data and files in the same request)
            if "body_data" in request.files:
                body_data = request.files["body_data"].read()
                content_type = request.files["body_data"].content_type
            else:
                body_data = request.data

            if body_model is not None:
                if content_type is None:
                    raise BadRequest("No Content-Type specified")

                if not body_data:
                    raise BadRequest("Expected body, but it is empty")

                try:
                    deserialized_data = deserialize(body_data, content_type)
                    kwargs["body_data"] = pydantic.parse_obj_as(body_model, deserialized_data)
                except Exception as e:
                    raise BadRequest("Invalid body: " + str(e))

            # 2. Query parameters are in request.args
            if url_params_model is not None:
                try:
                    kwargs["url_params"] = url_params_model(**request.args.to_dict(False))
                except Exception as e:
                    raise BadRequest("Invalid request arguments: " + str(e))

            # 3. File uploads
            temp_dir = None
            if allowed_file_extensions is not None:
                kwargs["files"] = []

                if "files" in request.files:
                    temp_dir = tempfile.mkdtemp(dir=current_app.config["UPLOAD_FOLDER"])

                    req_files = request.files.getlist("files")
                    for f in req_files:
                        if f.filename.split(".")[-1] not in allowed_file_extensions:
                            raise BadRequest(
                                f"Invalid file extension on file: {f.filename}. Allowed extensions: {allowed_file_extensions}"
                            )

                        with tempfile.NamedTemporaryFile("wb", dir=temp_dir, delete=False) as temp_file:
                            f.save(temp_file)
                            kwargs["files"].append((f.filename, temp_file.name))
                else:
                    kwargs["files"] = []

            # Now call the function, and validate the output
            try:
                ret = fn(*args, **kwargs)
            finally:
                if temp_dir is not None:
                    shutil.rmtree(temp_dir)

            # Serialize the output it it's not a normal flask response
            if isinstance(ret, Response):
                return ret

            serialized = serialize(ret, accept_type)
            return Response(serialized, content_type=accept_type)

        return wrapper

    return decorate
