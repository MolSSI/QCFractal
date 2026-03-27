import tempfile
from functools import wraps
from typing import Callable, Iterable, Optional

import shutil
from flask import g, request, current_app, Response
from werkzeug.exceptions import BadRequest

from qcfractal.components.auth import AuthorizedEnum
from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.flask_app import storage_socket
from qcportal.exceptions import AuthorizationFailure
from qcportal.serialization import deserialize, serialize


def _get_file_extension(filename, allowed_extensions = None):
    """Return a normalized extension (including leading period) for ``filename``.

    Matching is case-insensitive and supports multi-part extensions such as
    ``tar.gz`` when included in ``allowed_extensions``.

    Parameters
    ----------
    filename
        Original user-provided file name.
    allowed_extensions
        Iterable of allowed extensions *without* a leading period. If None, all extensions are allowed.

    Returns
    -------
    str
        The matched extension, including the leading period.

    Raises
    ------
    BadRequest
        If the filename does not end in one of the allowed extensions.
    """
    filename = filename.lower()

    if allowed_extensions is None:
        # Return the actual extension of the file, including the period
        # This is largely for tmp files, so don't care about compound extensions like .tar.gz
        if "." in filename:
            return filename[filename.rfind("."):]
        else:
            raise BadRequest("No file extension found in filename. Uploaded files must have an extension, such as .gz")

    # Otherwise, check the allowed extensions
    with_period = [f".{ext}" for ext in allowed_extensions]

    for ext in with_period:
        if filename.endswith(ext):
            return ext

    raise BadRequest(f"Invalid file extension on file: {filename}. Allowed extensions: {allowed_extensions}")


def check_permissions(
        requested_resource,
        requested_action,
        require_security: bool = False,
) -> Callable:
    """
    Route decorator that enforces global authorization for a resource/action pair.

    This uses user/group/role data already loaded into ``flask.g`` by request
    handlers and asks the auth socket if the current identity is allowed.

    The result is stored in ``g.permission_level``:
    - ``AuthorizedEnum.Allow``: route may proceed immediately
    - ``AuthorizedEnum.Conditional``: route must perform additional checks and
      set ``g.permission_level = AuthorizedEnum.Allow`` before returning

    If the route exits while the permission level is still conditional, this
    decorator raises ``RuntimeError``. This catches missing route-level checks.

    This decorator also sets ``_has_permission_check = True`` on the wrapped
    function so app startup can verify every route has explicit permission
    handling (either this decorator or ``@no_permission_required``).

    Parameters
    ----------
    requested_resource
        Logical resource name (for example ``"records"`` or ``"groups"``).
    requested_action
        Action name (for example ``"read"``, ``"add"``, ``"modify"``).
    require_security
        If ``True``, authorization fails when server security is disabled.
    """

    def decorate(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):

            allowed = storage_socket.auth.check_global_permission(
                g.role, requested_resource, requested_action, require_security
            )

            if allowed not in (AuthorizedEnum.Allow, AuthorizedEnum.Conditional):
                raise AuthorizationFailure(
                    f"Role '{g.role}' is not authorized to use action '{requested_action}' "
                    " on resource '{requested_resource}'"
                )

            # Store whether this is allowed or conditionally allowed (additional checks required)
            g.permission_level = allowed

            r = fn(*args, **kwargs)

            # Check that the permission level has been changed to allow
            # If not, then either something catastrophic happened or the value was never used.
            # Both of these would be logic/developer bugs
            if g.permission_level != AuthorizedEnum.Allow:
                raise RuntimeError("Permission level was not set to allow after function call. Developer error?")

            return r

        wrapper._has_permission_check = True
        return wrapper

    return decorate

def no_permission_required() -> Callable:
    """
    Mark a route as intentionally not requiring permission checks.

    This is intended for endpoints such as health checks or login/session APIs.
    It does not modify request context or perform authentication by itself.

    The main purpose is to set ``_has_permission_check = True`` on the wrapped
    function so startup validation accepts the route as explicitly reviewed.
    """

    def decorate(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        wrapper._has_permission_check = True
        return wrapper
    return decorate


def allow_uploads(allowed_file_extensions: Optional[Iterable[str]]) -> Callable:
    """
    Route decorator that stages uploaded files in a temporary directory.

    Files are read from ``request.files["files"]`` and passed to the route as a
    ``files`` keyword argument containing ``[(original_filename, temp_path), ...]``.
    Temporary files are always deleted after the route returns (or raises).

    The wrapped route function must define a ``files`` parameter in its
    signature (type annotation recommended and used as a guard here).

    Notes
    -----
    For multipart requests that include structured payload + files:
    - send structured payload in multipart part ``body_data``
    - apply ``@allow_uploads(...)`` *outside* ``@serialization()`` so
      ``serialization`` can deserialize ``body_data`` from multipart form data

    Parameters
    ----------
    allowed_file_extensions
        Allowed extensions without leading periods. Comparison is
        case-insensitive.

    Raises
    ------
    BadRequest
        If the route signature does not accept ``files`` or if an uploaded file
        has a disallowed extension.
    """

    def decorate(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            annotations = fn.__annotations__
            # If files are allowed, check for the files parameter in the function
            if "files" not in annotations:
                raise BadRequest('Route allows uploads, but function doesn\'t have the "files" argument')

            temp_dir = None
            files = []
            if "files" in request.files:
                temp_dir = tempfile.mkdtemp(dir=current_app.config["UPLOAD_FOLDER"])

                req_files = request.files.getlist("files")
                for f in req_files:

                    # Will raise on invalid file extension
                    ext = _get_file_extension(f.filename, allowed_file_extensions)

                    with tempfile.NamedTemporaryFile("wb", dir=temp_dir, suffix=ext, delete=False) as temp_file:
                        f.save(temp_file)
                        files.append((f.filename, temp_file.name))

            try:
                return fn(*args, **kwargs, files=files)
            finally:
                if temp_dir is not None:
                    shutil.rmtree(temp_dir)

        return wrapper

    return decorate


def handle_session(
) -> Callable:
    """
    Route decorator that injects a managed SQLAlchemy session.

    The wrapped route must include one of these annotated parameters:
    - ``session: SQLAlchemySocket`` for read/write transactions
    - ``ro_session: SQLAlchemySocket`` for read-only transactions

    A session scope is created for each request and passed as a keyword
    argument. If neither annotated parameter exists, a ``RuntimeError`` is
    raised.
    """
    def decorate(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            annotations = fn.__annotations__

            # If the function contains a "session" or "ro_session" argument, supply that
            if annotations.get("session", None) == SQLAlchemySocket:
                with storage_socket.session_scope() as session:
                    return fn(*args, **kwargs, session=session)
            elif annotations.get("ro_session", None) == SQLAlchemySocket:
                with storage_socket.session_scope(read_only=True) as session:
                    return fn(*args, **kwargs, ro_session=session)
            else:
                raise RuntimeError("Function does not have a session or ro_session argument, or it is not typed")

        return wrapper

    return decorate


def serialization(
) -> Callable:
    """
    Route decorator that deserializes request data and serializes return values.

    Request handling
    ----------------
    This decorator inspects wrapped-function annotations and injects arguments:
    - ``body_data`` annotation: body is deserialized to that model/type
    - ``url_params`` annotation: query args are parsed into that model/type

    Supported body locations:
    - raw request body (``request.data``)
    - multipart part named ``body_data`` (used with file uploads)

    Content negotiation
    -------------------
    Request body is decoded from the request ``Content-Type``.
    Response format is chosen from ``Accept`` best match among:
    - ``application/msgpack``
    - ``application/json`` (default and browser fallback)

    Return handling
    ---------------
    - If the route returns a Flask ``Response``, it is passed through unchanged.
    - Otherwise the return value is serialized and wrapped in a ``Response``.

    Error behavior
    --------------
    Invalid content type, empty required body, model validation failures, and
    invalid query parameters raise ``BadRequest``.
    """

    def decorate(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
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
                    kwargs["body_data"] = deserialize(body_data, content_type, body_model)
                except Exception as e:
                    raise BadRequest("Invalid body: " + str(e))

            # 2. Query parameters are in request.args
            if url_params_model is not None:
                try:
                    kwargs["url_params"] = url_params_model(**request.args.to_dict(False))
                except Exception as e:
                    raise BadRequest("Invalid request arguments: " + str(e))

            ret = fn(*args, **kwargs)

            # Serialize the output if it's not a normal flask response
            if isinstance(ret, Response):
                return ret

            serialized = serialize(ret, accept_type)
            return Response(serialized, content_type=accept_type)

        return wrapper

    return decorate
