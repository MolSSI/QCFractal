from functools import wraps
from typing import Callable

try:
    import pydantic.v1 as pydantic
except ImportError:
    import pydantic
from flask import request, Response
from werkzeug.exceptions import BadRequest

from qcfractal.flask_app.helpers import assert_is_authorized
from qcportal.serialization import deserialize, serialize


def wrap_route(
    requested_action,
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

    Parameters
    ----------
    requested_action
        The overall type of action that this route handles (read, write, etc)
    """

    def decorate(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            assert_is_authorized(requested_action)

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

            # 1. The body is stored in request.data
            if body_model is not None:
                if content_type is None:
                    raise BadRequest("No Content-Type specified")

                if not request.data:
                    raise BadRequest("Expected body, but it is empty")

                try:
                    deserialized_data = deserialize(request.data, content_type)
                    kwargs["body_data"] = pydantic.parse_obj_as(body_model, deserialized_data)
                except Exception as e:
                    raise BadRequest("Invalid body: " + str(e))

            # 2. Query parameters are in request.args
            if url_params_model is not None:
                try:
                    kwargs["url_params"] = url_params_model(**request.args.to_dict(False))
                except Exception as e:
                    raise BadRequest("Invalid request arguments: " + str(e))

            # Now call the function, and validate the output
            ret = fn(*args, **kwargs)

            # Serialize the output it it's not a normal flask response
            if isinstance(ret, Response):
                return ret

            serialized = serialize(ret, accept_type)
            return Response(serialized, content_type=accept_type)

        return wrapper

    return decorate
