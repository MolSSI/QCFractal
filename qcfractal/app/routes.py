"""
Routes handlers for Flask
"""
from __future__ import annotations

import time

from .. import __version__ as qcfractal_version
from qcelemental.util import deserialize, serialize
from qcelemental.models import FailedOperation
from ..storage_sockets.storage_utils import add_metadata_template
from ..interface.models import rest_model, build_procedure, PriorityEnum, TaskStatusEnum, RecordStatusEnum, UserInfo
from ..procedures import check_procedure_available, get_procedure_parser
from ..services import initialize_service
from ..exceptions import UserReportableError, AuthenticationFailure

from ..interface.models.rest_models import (
    AccessLogGETBody,
    AccessLogGETResponse,
    AccessSummaryGETBody,
    AccessSummaryGETResponse,
    InternalErrorLogGETBody,
    InternalErrorLogGETResponse,
    KeywordGETBody,
    KeywordGETResponse,
    KeywordPOSTBody,
    KeywordPOSTResponse,
    ResponseGETMeta,
    ResponsePOSTMeta,
    QueueManagerGETBody,
    QueueManagerGETResponse,
    QueueManagerPOSTBody,
    QueueManagerPOSTResponse,
    TaskQueuePOSTBody,
    TaskQueuePOSTResponse,
    MoleculeGETBody,
    MoleculeGETResponse,
    MoleculePOSTBody,
    MoleculePOSTResponse,
    KVStoreGETBody,
    KVStoreGETResponse,
    ManagerInfoGETBody,
    ManagerInfoGETResponse,
    WavefunctionStoreGETBody,
    WavefunctionStoreGETResponse,
)

from flask import jsonify, request, g
import traceback
import json
from flask_jwt_extended import (
    create_access_token,
    get_jwt,
    jwt_required,
    create_refresh_token,
    get_jwt_identity,
    verify_jwt_in_request,
)
from urllib.parse import urlparse
from ..policyuniverse import Policy
from flask import Blueprint, current_app, Response
from functools import wraps
from werkzeug.exceptions import BadRequest, HTTPException, NotFound, Forbidden, Unauthorized, InternalServerError

from . import api_logger, storage_socket, view_handler

from typing import TYPE_CHECKING, Sequence

if TYPE_CHECKING:
    from ..interface.models.query_meta import QueryMetadata, InsertMetadata, DeleteMetadata
    from typing import Union, TypeVar, List

    _T = TypeVar("_T")


main = Blueprint("main", __name__)


_valid_encodings = {
    "application/json": "json",
    "application/json-ext": "json-ext",
    "application/msgpack-ext": "msgpack-ext",
}

# TODO: not implemented yet
_logging_param_counts = {"id"}
_read_permissions = {}


def make_list(obj: Union[_T, Sequence[_T]]) -> List[_T]:
    """
    Returns a list containing obj if obj is not a list or sequence type object
    """

    # Be careful. strings are sequences
    if isinstance(obj, str):
        return [obj]
    if not isinstance(obj, Sequence):
        return [obj]
    return list(obj)


def convert_get_response_metadata(meta: QueryMetadata, missing: List) -> ResponseGETMeta:
    """
    Converts the new QueryMetadata format to the old ResponseGETMeta format
    """

    error_description = meta.error_description
    if error_description is None:
        error_description = False

    return ResponseGETMeta(
        errors=meta.errors,
        success=meta.success,
        error_description=error_description,
        missing=missing,
        n_found=meta.n_found,
    )


def convert_post_response_metadata(meta: InsertMetadata, duplicates: List) -> ResponsePOSTMeta:
    """
    Converts the new InsertMetadata format to the old ResponsePOSTMeta format
    """

    error_description = meta.error_description
    if error_description is None:
        error_description = False

    return ResponsePOSTMeta(
        errors=meta.errors,
        success=meta.success,
        error_description=error_description,
        n_inserted=meta.n_inserted,
        duplicates=duplicates,
        validation_errors=[],
    )


def check_access(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        """
        Call the route (fn) if allowed to access the url using the given
        permissions in the JWT token in the request headers

        1- If no security (JWT_ENABLED=False), always allow
        2- If JWT_ENABLED:
            if read allowed (allow_read=True), use the default read permissions
            otherwise, check against the logged-in user permissions
            from the headers' JWT token
        """

        # current_app.logger.debug(f"JWT_ENABLED: {current_app.config['JWT_ENABLED']}")
        # current_app.logger.debug(f"ALLOW_UNAUTHENTICATED_READ: {current_app.config['ALLOW_UNAUTHENTICATED_READ']}")
        # current_app.logger.debug(f"SECRET_KEY: {current_app.secret_key}")
        # current_app.logger.debug(f"SECRET_KEY: {current_app.config['SECRET_KEY']}")
        # current_app.logger.debug(f"JWT_SECRET_KEY: {current_app.config['JWT_SECRET_KEY']}")
        # current_app.logger.debug(f"JWT_ACCESS_TOKEN_EXPIRES: {current_app.config['JWT_ACCESS_TOKEN_EXPIRES']}")
        # current_app.logger.debug(f"JWT_REFRESH_TOKEN_EXPIRES: {current_app.config['JWT_REFRESH_TOKEN_EXPIRES']}")

        # if no auth required, always allowed
        if not current_app.config["JWT_ENABLED"]:
            return fn(*args, **kwargs)

        # load read permissions from DB if not read
        global _read_permissions
        if not _read_permissions:
            _read_permissions = storage_socket.role.get("read").permissions

        # if read is allowed without login, use read_permissions
        # otherwise, check logged-in permissions
        if current_app.config["ALLOW_UNAUTHENTICATED_READ"]:
            # don't raise exception if no JWT is found
            verify_jwt_in_request(optional=True)
        else:
            # read JWT token from request headers
            verify_jwt_in_request(optional=False)

        claims = get_jwt()
        permissions = claims.get("permissions", {})

        try:
            # host_url = request.host_url
            identity = get_jwt_identity() or "anonymous"
            resource = urlparse(request.url).path.split("/")[1]
            context = {
                "Principal": identity,
                "Action": request.method,
                "Resource": resource
                # "IpAddress": request.remote_addr,
                # "AccessTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            }
            current_app.logger.info(f"Permissions: {permissions}")
            current_app.logger.info(f"Context: {context}")
            policy = Policy(permissions)
            if not policy.evaluate(context):
                if not Policy(_read_permissions).evaluate(context):
                    return Forbidden(f"User {identity} is not authorized to access '{resource}' resource.")

            # Store the user in the global app/request context
            g.user = identity

        except Exception as e:
            current_app.logger.info("Error in evaluating JWT permissions: \n" + str(e))
            return BadRequest("Error in evaluating JWT permissions")

        return fn(*args, **kwargs)

    return wrapper


def parse_bodymodel(model):
    """Parse request body using pydantic models"""

    try:
        return model(**request.data)
    except Exception as e:
        current_app.logger.error("Invalid request body:\n" + str(e))
        raise BadRequest("Invalid body: " + str(e))


class SerializedResponse(Response):
    """Serialize pydantic response using the given encoding and pass it
    as a flask response object"""

    def __init__(self, response, **kwargs):

        # TODO: support other content types? We would need to check the Accept header
        content_type = "application/msgpack-ext"
        encoding = _valid_encodings[content_type]
        response = serialize(response, encoding)
        super(SerializedResponse, self).__init__(response, content_type=content_type, **kwargs)


@main.before_request
def before_request_func():
    ###############################################################
    # Deserialize the various encodings we support (like msgpack) #
    ###############################################################

    # Store timing information in the request/app context
    # g here refers to flask.g
    g.request_start = time.time()

    # default to "application/json"
    content_type = request.headers.get("Content-Type", "application/json")
    encoding = _valid_encodings.get(content_type, None)

    if encoding is None:
        raise BadRequest(f"Did not understand 'Content-Type {content_type}")

    try:
        # Check to see if we have a json that is encoded as bytes rather than a string
        if (encoding == "json") and isinstance(request.data, bytes):
            blob = request.data.decode()
        else:
            blob = request.data

        if blob:
            request.data = deserialize(blob, encoding)
        else:
            request.data = None
    except Exception as e:
        raise BadRequest(f"Could not deserialize body. {e}")


@main.after_request
def after_request_func(response: SerializedResponse):

    # Determine the time the request took
    # g here refers to flask.g
    request_duration = time.time() - g.request_start

    exclude_uris = ["/task_queue", "/service_queue", "/queue_manager"]

    # No associated data, so skip all of this
    # (maybe caused by not using portal or not using the REST API correctly?)
    if request.data is None:
        return response

    log_access = current_app.config["QCFRACTAL_CONFIG"].log_access
    if log_access and request.method == "GET" and request.path not in exclude_uris:
        extra_params = request.data.copy()
        if _logging_param_counts:
            for key in _logging_param_counts:
                if "data" in extra_params and extra_params["data"].get(key, None):
                    extra_params["data"][key] = len(extra_params["data"][key])

        if "data" in extra_params:
            extra_params["data"] = {k: v for k, v in extra_params["data"].items() if v is not None}

        extra_params = json.dumps(extra_params)

        log = api_logger.get_api_access_log(request=request, extra_params=extra_params)

        log["request_duration"] = request_duration
        log["user"] = g.user if "user" in g else None

        if isinstance(response.response, (bytes, str)):
            log["response_bytes"] = len(response.response)

        storage_socket.server_log.save_access(log)

    return response


@main.errorhandler(InternalServerError)
def handle_internal_error(error):
    # For otherwise unhandled errors
    # Do not report the details to the user. Instead, log it,
    # and send the user the error id

    # Obtain the original exception that caused the error
    # original = getattr(error, "original_exception", None)

    # Copy the headers to a dict, and remove the JWT stuff
    headers = dict(request.headers.items())
    headers.pop("Authorization", None)

    user = g.user if "user" in g else None
    error_log = {
        "error_text": traceback.format_exc(),
        "user": user,
        "request_path": request.full_path,
        "request_headers": str(headers),
        "request_body": str(request.data)[:8192],
    }

    # Log it to the internal error table
    err_id = storage_socket.server_log.save_error(error_log)

    msg = error.description + f"  **Refer to internal error id {err_id} when asking your admin**"
    return jsonify(msg=msg), error.code


@main.errorhandler(HTTPException)
def handle_http_exception(error):
    # This handles many errors, such as NotFound, Unauthorized, etc
    # These are all reportable to the user
    return jsonify(msg=str(error)), error.code


@main.errorhandler(UserReportableError)
def handle_userreport_error(error):
    # This handles any errors that are reportable to the user
    return jsonify(msg=str(error)), 400


@main.errorhandler(AuthenticationFailure)
def handle_auth_error(error):
    # This handles Authentication errors (invalid user, password, etc)
    return jsonify(msg=str(error)), 401


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#                            Routes
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


@main.route("/register", methods=["POST"])
def register():
    if request.is_json:
        username = request.json["username"]
        password = request.json["password"]
        fullname = request.json["fullname"]
        email = request.json["email"]
        organization = request.json["organization"]
    else:
        username = request.form["username"]
        password = request.form["password"]
        fullname = request.form["fullname"]
        email = request.form["email"]
        organization = request.form["organization"]

    role = "read"
    try:
        user_info = UserInfo(
            username=username,
            enabled=True,
            fullname=fullname,
            email=email,
            organization=organization,
            role=role,
        )
    except Exception as e:
        return jsonify(msg=f"Invalid user information: {str(e)}"), 500

    # add returns the password. Raises exception on error
    # Exceptions should be handled property by the flask errorhandlers
    pw = storage_socket.user.add(user_info, password=password)
    if password is None or len(password) == 0:
        return jsonify(msg="New user created!"), 201
    else:
        return jsonify(msg="New user created! Password is '{pw}'"), 201


@main.route("/login", methods=["POST"])
def login():
    if request.is_json:
        username = request.json["username"]
        password = request.json["password"]
    else:
        username = request.form["username"]
        password = request.form["password"]

    # Raises exceptions on error
    # Also raises AuthenticationFailure if the user is invalid or the password is incorrect
    # This should be handled properly by the flask errorhandlers
    permissions = storage_socket.user.verify(username, password)

    access_token = create_access_token(identity=username, additional_claims={"permissions": permissions})
    # expires_delta=datetime.timedelta(days=3))
    refresh_token = create_refresh_token(identity=username)
    return jsonify(msg="Login succeeded!", access_token=access_token, refresh_token=refresh_token), 200


@main.route("/information", methods=["GET"])
def get_information():
    qcf_cfg = current_app.config["QCFRACTAL_CONFIG"]

    # TODO FOR RELEASE - change lower and upper version limits?
    db_data = storage_socket.server_log.get_latest_stats()
    public_info = {
        "name": qcf_cfg.name,
        "manager_heartbeat_frequency": qcf_cfg.heartbeat_frequency,
        "version": qcfractal_version,
        "query_limits": qcf_cfg.response_limits.dict(),
        "client_lower_version_limit": qcfractal_version,
        "client_upper_version_limit": qcfractal_version,
        "collection": db_data.get("collection_count", 0),
        "molecule": db_data.get("molecule_count", 0),
        "result": db_data.get("result_count", 0),
        "kvstore": db_data.get("kvstore_count", 0),
        "last_update": db_data.get("timestamp", None),
    }

    return SerializedResponse(public_info)


@main.route("/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh():
    username = get_jwt_identity()
    permissions = storage_socket.user.get_permissions(username)
    ret = {"access_token": create_access_token(identity=username, additional_claims={"permissions": permissions})}
    return jsonify(ret), 200


@main.route("/fresh-login", methods=["POST"])
def fresh_login():
    if request.is_json:
        username = request.json["username"]
        password = request.json["password"]
    else:
        username = request.form["username"]
        password = request.form["password"]

    # Raises exceptions on error
    # Also raises AuthenticationFailure if the user is invalid or the password is incorrect
    # This should be handled properly by the flask errorhandlers
    permissions = storage_socket.user.verify(username, password)

    access_token = create_access_token(
        identity=username, additionalclaims={"permissions": permissions.dict()}, fresh=True
    )
    return jsonify(msg="Fresh login succeeded!", access_token=access_token), 200


@main.route("/molecule", methods=["GET"])
@check_access
def get_molecule():
    body = parse_bodymodel(MoleculeGETBody)

    # TODO - should only accept lists/sequences/iterables
    data_dict = body.data.dict()
    molecule_hash = data_dict.pop("molecule_hash", None)
    molecular_formula = data_dict.pop("molecular_formula", None)

    if molecule_hash is not None:
        molecule_hash = make_list(molecule_hash)
    if molecular_formula is not None:
        molecular_formula = make_list(molecular_formula)

    meta, molecules = storage_socket.molecule.query(
        **data_dict, **body.meta.dict(), molecular_formula=molecular_formula, molecule_hash=molecule_hash
    )

    # Convert the new metadata format to the old format
    meta_old = convert_get_response_metadata(meta, missing=[])

    response = MoleculeGETResponse(meta=meta_old, data=molecules)

    return SerializedResponse(response)


@main.route("/molecule", methods=["POST"])
@check_access
def post_molecule():
    """
    Request:
        "meta" - Overall options to the Molecule pull request
            - No current options
        "data" - A dictionary of {key : molecule JSON} requests

    Returns:
        "meta" - Metadata associated with the query
            - "errors" - A list of errors in (index, error_id) format.
            - "n_inserted" - The number of molecule inserted.
            - "success" - If the query was successful or not.
            - "error_description" - A string based description of the error or False
            - "duplicates" - A list of keys that were already inserted.
        "data" - A dictionary of {key : id} results
    """

    body = parse_bodymodel(MoleculePOSTBody)

    meta, ret = storage_socket.molecule.add(body.data)

    # Convert new metadata format to old
    duplicate_ids = [ret[i] for i in meta.existing_idx]
    meta_old = convert_post_response_metadata(meta, duplicate_ids)

    response = MoleculePOSTResponse(meta=meta_old, data=ret)
    return SerializedResponse(response)


@main.route("/kvstore", methods=["GET"])
@check_access
def get_kvstore():
    """
    Request:
        "data" - A list of key requests
    Returns:
        "meta" - Metadata associated with the query
            - "errors" - A list of errors in (index, error_id) format.
            - "n_found" - The number of molecule found.
            - "success" - If the query was successful or not.
            - "error_description" - A string based description of the error or False
            - "missing" - A list of keys that were not found.
        "data" - A dictionary of {key : value} dictionary of the results
    """

    body = parse_bodymodel(KVStoreGETBody)
    ret = storage_socket.output_store.get(body.data.id, missing_ok=True)

    # REST API currently expects a dict {id: KVStore dict}
    # But socket returns a list of KVStore dict
    ret_dict = {x["id"]: x for x in ret if x is not None}

    missing_id = [x for x, y in zip(body.data.id, ret) if y is None]

    # Transform to the old metadata format
    meta = ResponseGETMeta(n_found=len(ret), missing=missing_id, errors=[], error_description=False, success=True)
    response = KVStoreGETResponse(meta=meta, data=ret_dict)

    return SerializedResponse(response)


@main.route("/keyword", methods=["GET"])
@check_access
def get_keyword():
    body = parse_bodymodel(KeywordGETBody)

    ret = storage_socket.keywords.get(body.data.id, missing_ok=True)
    missing_id = [x for x, y in zip(body.data.id, ret) if y is None]
    meta = ResponseGETMeta(n_found=len(ret), missing=missing_id, errors=[], error_description=False, success=True)
    response = KeywordGETResponse(meta=meta, data=ret)

    return SerializedResponse(response)


@main.route("/keyword", methods=["POST"])
@check_access
def post_keyword():

    body = parse_bodymodel(KeywordPOSTBody)
    meta, ret = storage_socket.keywords.add(body.data)

    # Convert new metadata format to old
    duplicate_ids = [ret[i] for i in meta.existing_idx]
    meta_old = convert_post_response_metadata(meta, duplicate_ids)

    response = MoleculePOSTResponse(meta=meta_old, data=ret)
    return SerializedResponse(response)


@main.route("/collection", methods=["GET"])
@main.route("/collection/<int:collection_id>", methods=["GET"])
@main.route("/collection/<int:collection_id>/<string:view_function>", methods=["GET"])
@check_access
def get_collection(collection_id: int = None, view_function: str = None):
    # List collections

    view_function_vals = ("value", "entry", "list", "molecule")
    if view_function is not None and view_function not in view_function_vals:
        raise NotFound(f"URL Not Found. view_function must be in : {view_function_vals}")

    if (collection_id is None) and (view_function is None):
        body_model, response_model = rest_model("collection", "get")
        body = parse_bodymodel(body_model)

        cols = storage_socket.get_collections(**body.data.dict(), include=body.meta.include, exclude=body.meta.exclude)
        response = response_model(**cols)

    # Get specific collection
    elif (collection_id is not None) and (view_function is None):
        body_model, response_model = rest_model("collection", "get")

        body = parse_bodymodel(body_model)
        cols = storage_socket.get_collections(
            **body.data.dict(), col_id=int(collection_id), include=body.meta.include, exclude=body.meta.exclude
        )
        response = response_model(**cols)

    # View-backed function on collection
    elif (collection_id is not None) and (view_function is not None):
        body_model, response_model = rest_model(f"collection/{collection_id}/{view_function}", "get")
        body = parse_bodymodel(body_model)
        if view_handler.enabled is False:
            meta = {
                "success": False,
                "error_description": "Server does not support collection views.",
                "errors": [],
                "msgpacked_cols": [],
            }
            response = response_model(meta=meta, data=None)
            return SerializedResponse(response)

        result = view_handler.handle_request(collection_id, view_function, body.data.dict())
        response = response_model(**result)

    # Unreachable?
    else:
        body_model, response_model = rest_model("collection", "get")
        meta = add_metadata_template()
        meta["success"] = False
        meta["error_description"] = "GET request for view with no collection ID not understood."
        response = response_model(meta=meta, data=None)

    return SerializedResponse(response)


@main.route("/collection", methods=["POST"])
@main.route("/collection/<int:collection_id>", methods=["POST"])
@main.route("/collection/<int:collection_id>/<string:view_function>", methods=["POST"])
@check_access
def post_collection(collection_id: int = None, view_function: str = None):

    view_function_vals = ("value", "entry", "list", "molecule")
    if view_function is not None and view_function not in view_function_vals:
        raise NotFound(f"URL Not Found. view_function must be in : {view_function_vals}")

    body_model, response_model = rest_model("collection", "post")
    body = parse_bodymodel(body_model)

    # POST requests not supported for anything other than "/collection"
    if collection_id is not None or view_function is not None:
        meta = add_metadata_template()
        meta["success"] = False
        meta["error_description"] = "POST requests not supported for sub-resources of /collection"
        response = response_model(meta=meta, data=None)

        return SerializedResponse(response)

    ret = storage_socket.add_collection(body.data.dict(), overwrite=body.meta.overwrite)
    response = response_model(**ret)

    return SerializedResponse(response)


@main.route("/collection", methods=["DELETE"])
@main.route("/collection/<int:collection_id>", methods=["DELETE"])
@check_access
def delete_collection(collection_id: int, view_function: str):
    body_model, response_model = rest_model(f"collection/{collection_id}", "delete")
    ret = storage_socket.del_collection(col_id=collection_id)
    if ret == 0:
        return jsonify(msg="Collection does not exist."), 404
    else:
        response = response_model(meta={"success": True, "errors": [], "error_description": False})

    return SerializedResponse(response)


@main.route("/result", methods=["GET"])
@check_access
def get_result():

    body_model, response_model = rest_model("result", "get")
    body = parse_bodymodel(body_model)

    ret = storage_socket.get_results(**{**body.data.dict(), **body.meta.dict()})
    response = response_model(**ret)

    current_app.logger.info("GET: Results - {} pulls.".format(len(response.data)))

    return SerializedResponse(response)


@main.route("/wavefunctionstore", methods=["GET"])
@check_access
def get_wave_function():

    # NOTE - this only supports one wavefunction at a time
    body = parse_bodymodel(WavefunctionStoreGETBody)

    ret = storage_socket.wavefunction.get([body.data.id], include=body.meta.include)
    meta_nfound = len(ret)
    if len(ret) > 0:
        meta_missing = []
        ret = ret[0]
    else:
        meta_missing = [body.data.id]

    meta = ResponseGETMeta(errors=[], success=True, error_description=False, missing=meta_missing, n_found=meta_nfound)

    response = WavefunctionStoreGETResponse(meta=meta, data=ret)

    return SerializedResponse(response)


@main.route("/procedure", methods=["GET"])
@main.route("/procedure/<string:query_type>", methods=["GET"])
@check_access
def get_procedure(query_type: str = "get"):
    body_model, response_model = rest_model("procedure", query_type)
    body = parse_bodymodel(body_model)

    # try:
    if query_type == "get":
        ret = storage_socket.get_procedures(**{**body.data.dict(), **body.meta.dict()})
    else:  # all other queries, like 'best_opt_results'
        ret = storage_socket.custom_query("procedure", query_type, **{**body.data.dict(), **body.meta.dict()})
    # except KeyError as e:
    #     return jsonify(msg=str(e)), 500

    response = response_model(**ret)

    return SerializedResponse(response)


@main.route("/optimization/<string:query_type>", methods=["GET"])
@check_access
def get_optimization(query_type: str):
    body_model, response_model = rest_model(f"optimization/{query_type}", "get")
    body = parse_bodymodel(body_model)

    # try:
    if query_type == "get":
        ret = storage_socket.get_procedures(**{**body.data.dict(), **body.meta.dict()})
    else:  # all other queries, like 'best_opt_results'
        ret = storage_socket.custom_query("optimization", query_type, **{**body.data.dict(), **body.meta.dict()})
    # except KeyError as e:
    #     return jsonify(msg=str(e)), 500

    response = response_model(**ret)

    return SerializedResponse(response)


@main.route("/task_queue", methods=["GET"])
@check_access
def get_task_queue():
    body_model, response_model = rest_model("task_queue", "get")
    body = parse_bodymodel(body_model)

    tasks = storage_socket.get_queue(**{**body.data.dict(), **body.meta.dict()})
    response = response_model(**tasks)

    return SerializedResponse(response)


@main.route("/task_queue", methods=["POST"])
@check_access
def post_task_queue():
    body = parse_bodymodel(TaskQueuePOSTBody)

    # Format and submit tasks
    if not check_procedure_available(body.meta.procedure):
        return jsonify(msg="Unknown procedure {}.".format(body.meta.procedure)), 400

    procedure_parser = get_procedure_parser(body.meta.procedure, storage_socket)

    # Verify the procedure
    verify = procedure_parser.verify_input(body)
    if verify is not True:
        return jsonify(msg=verify), 400

    payload = procedure_parser.submit_tasks(body)
    response = TaskQueuePOSTResponse(**payload)

    return SerializedResponse(response)


@main.route("/task_queue", methods=["PUT"])
@check_access
def put_task_queue():
    """Modifies tasks in the task queue"""

    body_model, response_model = rest_model("task_queue", "put")
    body = parse_bodymodel(body_model)

    if (body.data.id is None) and (body.data.base_result is None):
        return jsonify(msg="Id or ResultId must be specified."), 400

    if body.meta.operation == "restart":
        d = body.data.dict()
        d.pop("new_tag", None)
        d.pop("new_priority", None)
        tasks_updated = storage_socket.queue_reset_status(**d, reset_error=True)
        data = {"n_updated": tasks_updated}
    elif body.meta.operation == "regenerate":
        tasks_updated = 0
        result_data = storage_socket.get_procedures(id=body.data.base_result)["data"]

        new_tag = body.data.new_tag
        if body.data.new_priority is None:
            new_priority = PriorityEnum.NORMAL
        else:
            new_priority = PriorityEnum(int(body.data.new_priority))

        for r in result_data:
            model = build_procedure(r)

            # Only regenerate the task if the base record is not complete
            # This will not do anything if the task already exists
            if model.status != RecordStatusEnum.complete:
                procedure_parser = get_procedure_parser(model.procedure, storage_socket)

                task_info = procedure_parser.create_tasks([model], tag=new_tag, priority=new_priority)
                n_inserted = task_info["meta"]["n_inserted"]
                tasks_updated += n_inserted

                # If we inserted a new task, then also reset base result statuses
                # (ie, if it was running, then it obviously isn't since we made a new task)
                if n_inserted > 0:
                    storage_socket.reset_base_result_status(id=body.data.base_result)

            data = {"n_updated": tasks_updated}
    elif body.meta.operation == "modify":
        tasks_updated = storage_socket.queue_modify_tasks(
            id=body.data.id,
            base_result=body.data.base_result,
            new_tag=body.data.new_tag,
            new_priority=body.data.new_priority,
        )
        data = {"n_updated": tasks_updated}
    else:
        return jsonify(msg=f"Operation '{body.meta.operation}' is not valid."), 400

    response = response_model(data=data, meta={"errors": [], "success": True, "error_description": False})

    current_app.logger.info(f"PUT: TaskQueue - Operation: {body.meta.operation} - {tasks_updated}.")

    return SerializedResponse(response)


@main.route("/service_queue", methods=["GET"])
@check_access
def get_service_queue():
    body_model, response_model = rest_model("service_queue", "get")
    body = parse_bodymodel(body_model)

    ret = storage_socket.get_services(**{**body.data.dict(), **body.meta.dict()})
    response = response_model(**ret)

    return SerializedResponse(response)


@main.route("/service_queue", methods=["POST"])
@check_access
def post_service_queue():
    """Posts new services to the service queue."""

    body_model, response_model = rest_model("service_queue", "post")
    body = parse_bodymodel(body_model)

    new_services = []
    for service_input in body.data:
        # Add all the molecules specified (or check that specified IDs exist)
        mol_list = make_list(service_input.initial_molecule)

        meta, molecule_ids = storage_socket.molecule.add_mixed(mol_list)
        if not meta.success:
            err_msg = "Error adding initial molecules:\n" + meta.error_string
            raise RuntimeError(err_msg)

        molecules = storage_socket.molecule.get(molecule_ids)
        if not isinstance(service_input.initial_molecule, list):
            molecules = molecules[0]

        # Update the input and build a service object
        service_input = service_input.copy(update={"initial_molecule": molecules})
        new_services.append(
            initialize_service(
                storage_socket,
                service_input,
                tag=body.meta.tag,
                priority=body.meta.priority,
            )
        )

    ret = storage_socket.add_services(new_services)
    ret["data"] = {"ids": ret["data"], "existing": ret["meta"]["duplicates"]}
    ret["data"]["submitted"] = list(set(ret["data"]["ids"]) - set(ret["meta"]["duplicates"]))
    response = response_model(**ret)

    return SerializedResponse(response)


@main.route("/service_queue", methods=["PUT"])
@check_access
def put_service_queue():
    """Modifies services in the service queue"""

    body_model, response_model = rest_model("service_queue", "put")
    body = parse_bodymodel(body_model)

    if (body.data.id is None) and (body.data.procedure_id is None):
        return jsonify(msg="Id or ProcedureId must be specified."), 400

    if body.meta.operation == "restart":
        updates = storage_socket.update_service_status("running", **body.data.dict())
        data = {"n_updated": updates}
    else:
        return jsonify(msg="Operation '{operation}' is not valid."), 400

    response = response_model(data=data, meta={"errors": [], "success": True, "error_description": False})

    return SerializedResponse(response)


def _get_name_from_metadata(meta):
    """
    Form the canonical name string.
    """
    ret = meta.cluster + "-" + meta.hostname + "-" + meta.uuid
    return ret


def _insert_complete_tasks(storage_socket, body: QueueManagerPOSTBody):

    results = body.data
    meta = body.meta
    task_ids = list(int(x) for x in results.keys())

    manager_name = _get_name_from_metadata(meta)
    current_app.logger.info("QueueManager: Received completed tasks from {}.".format(manager_name))
    current_app.logger.info("              Task ids: " + " ".join(str(x) for x in task_ids))

    # Pivot data so that we group all results in categories
    queue = storage_socket.get_queue(id=task_ids)["data"]
    queue = {v.id: v for v in queue}

    task_success = 0
    task_failures = 0
    task_totals = len(results.items())

    failure_parser = get_procedure_parser("failed_operation", storage_socket)

    for task_id, result in results.items():
        existing_task_data = queue.get(task_id, None)

        # Does the task exist?
        if existing_task_data is None:
            current_app.logger.warning(f"Task id {task_id} does not exist in the task queue.")
            task_failures += 1
            continue

        try:
            #################################################################
            # Perform some checks for consistency
            #################################################################
            # Information passed to handle_completed_output for the various output parsers
            base_result_id = int(existing_task_data.base_result)

            # Is the task in the running state
            # If so, do not attempt to modify the task queue. Just move on
            if existing_task_data.status != TaskStatusEnum.running:
                current_app.logger.warning(f"Task id {task_id} is not in the running state.")
                task_failures += 1

            # Was the manager that sent the data the one that was assigned?
            # If so, do not attempt to modify the task queue. Just move on
            elif existing_task_data.manager != manager_name:
                current_app.logger.warning(
                    f"Task id {task_id} belongs to {existing_task_data.manager}, not this manager"
                )
                task_failures += 1

            # Failed task returning FailedOperation
            elif result.success is False and isinstance(result, FailedOperation):
                failure_parser.handle_completed_output(task_id, base_result_id, manager_name, result)
                task_failures += 1

            elif result.success is not True:
                # QCEngine should always return either FailedOperation, or some result with success == True
                current_app.logger.warning(f"Task id {task_id} returned success != True, but is not a FailedOperation")
                task_failures += 1

            # Manager returned a full, successful result
            else:
                parser = get_procedure_parser(queue[task_id].parser, storage_socket)
                parser.handle_completed_output(task_id, base_result_id, manager_name, result)
                task_success += 1

        except Exception:
            msg = "Internal FractalServer Error:\n" + traceback.format_exc()
            error = {"error_type": "internal_fractal_error", "error_message": msg}
            failed_op = FailedOperation(error=error, success=False)

            base_result_id = int(existing_task_data.base_result)

            failure_parser.handle_completed_output(task_id, base_result_id, manager_name, failed_op)
            current_app.logger.error("update: ERROR\n{}".format(msg))
            task_failures += 1

    current_app.logger.info(
        "QueueManager: Found {} complete tasks ({} successful, {} failed).".format(
            task_totals, task_success, task_failures
        )
    )

    return task_success, task_failures


@main.route("/queue_manager", methods=["GET"])
@check_access
def get_queue_manager():
    """Pulls new tasks from the task queue"""

    body = parse_bodymodel(QueueManagerGETBody)

    # Figure out metadata and kwargs
    name = _get_name_from_metadata(body.meta)

    # Grab new tasks and write out
    new_tasks = storage_socket.queue_get_next(
        name, body.meta.programs, body.meta.procedures, limit=body.data.limit, tag=body.meta.tag
    )
    response = QueueManagerGETResponse(
        **{
            "meta": {
                "n_found": len(new_tasks),
                "success": True,
                "errors": [],
                "error_description": "",
                "missing": [],
            },
            "data": new_tasks,
        }
    )
    # Update manager logs
    storage_socket.manager.update(name, submitted=len(new_tasks), **body.meta.dict())

    return SerializedResponse(response)


@main.route("/queue_manager", methods=["POST"])
@check_access
def post_queue_manager():
    """Posts complete tasks to the task queue"""

    body = parse_bodymodel(QueueManagerPOSTBody)

    success, error = _insert_complete_tasks(storage_socket, body)

    completed = success + error

    response = QueueManagerPOSTResponse(
        **{
            "meta": {
                "n_inserted": completed,
                "duplicates": [],
                "validation_errors": [],
                "success": True,
                "errors": [],
                "error_description": "",
            },
            "data": True,
        }
    )

    # Update manager logs
    name = _get_name_from_metadata(body.meta)
    storage_socket.manager.update(name, completed=completed, failures=error)

    return SerializedResponse(response)


@main.route("/queue_manager", methods=["PUT"])
@check_access
def put_queue_manager():
    """
    Various manager manipulation operations
    """

    ret = True

    body_model, response_model = rest_model("queue_manager", "put")
    body = parse_bodymodel(body_model)

    name = _get_name_from_metadata(body.meta)
    op = body.data.operation
    if op == "startup":
        storage_socket.manager.update(
            name, status="ACTIVE", configuration=body.data.configuration, **body.meta.dict(), log=True
        )
        # current_app.logger.info("QueueManager: New active manager {} detected.".format(name))

    elif op == "shutdown":
        nshutdown = storage_socket.queue_reset_status(manager=name, reset_running=True)
        storage_socket.manager.update(name, returned=nshutdown, status="INACTIVE", **body.meta.dict(), log=True)

        # current_app.logger.info("QueueManager: Shutdown of manager {} detected, recycling {} incomplete tasks.".format(name, nshutdown))

        ret = {"nshutdown": nshutdown}

    elif op == "heartbeat":
        storage_socket.manager.update(name, status="ACTIVE", **body.meta.dict(), log=True)
        # current_app.logger.debug("QueueManager: Heartbeat of manager {} detected.".format(name))

    else:
        msg = "Operation '{}' not understood.".format(op)
        return jsonify(msg=msg), 400

    response = response_model(**{"meta": {}, "data": ret})

    return SerializedResponse(response)


@main.route("/manager", methods=["GET"])
@check_access
def get_manager():
    """Gets manager information from the task queue"""

    body = parse_bodymodel(ManagerInfoGETBody)
    meta, managers = storage_socket.manager.query(**{**body.data.dict(), **body.meta.dict()})
    meta_old = convert_get_response_metadata(meta, missing=[])
    response = ManagerInfoGETResponse(meta=meta_old, data=managers)
    return SerializedResponse(response)


@main.route("/access/log", methods=["GET"])
@check_access
def get_access_log():
    """
    Queries access logs
    """

    body = parse_bodymodel(AccessLogGETBody)
    meta, logs = storage_socket.server_log.query_access_logs(**{**body.data.dict(), **body.meta.dict()})
    response = AccessLogGETResponse(meta=convert_get_response_metadata(meta, missing=[]), data=logs)
    return SerializedResponse(response)


@main.route("/access/summary", methods=["GET"])
@check_access
def get_access_summary():
    """
    Queries access logs
    """

    body = parse_bodymodel(AccessSummaryGETBody)
    summary = storage_socket.server_log.query_access_summary(**{**body.data.dict()})
    response = AccessSummaryGETResponse(data=summary)
    return SerializedResponse(response)


@main.route("/error", methods=["GET"])
@check_access
def get_internal_error_log():
    """
    Queries internal error logs
    """

    body = parse_bodymodel(InternalErrorLogGETBody)
    meta, logs = storage_socket.server_log.query_error_logs(**{**body.data.dict(), **body.meta.dict()})
    response = InternalErrorLogGETResponse(meta=convert_get_response_metadata(meta, missing=[]), data=logs)
    return SerializedResponse(response)


@main.route("/role", methods=["GET"])
@check_access
def get_roles():
    roles = storage_socket.role.list()
    # TODO - SerializedResponse?
    r = [x.dict() for x in roles]
    return jsonify(roles), 200


@main.route("/role/<string:rolename>", methods=["GET"])
@check_access
def get_role(rolename: str):

    role = storage_socket.role.get(rolename)
    # TODO - SerializedResponse?
    return jsonify(role.dict()), 200


@main.route("/role/<string:rolename>", methods=["POST"])
@check_access
def add_role():
    rolename = request.json["rolename"]
    permissions = request.json["permissions"]

    try:
        storage_socket.role.add(rolename, permissions)
        return jsonify({"msg": "New role created!"}), 201
    except Exception as e:
        current_app.logger.warning(f"Error creating role {rolename}: {str(e)}")
        return jsonify({"msg": "Error creating role"}), 400


@main.route("/role", methods=["PUT"])
@check_access
def update_role():
    rolename = request.json["rolename"]
    permissions = request.json["permissions"]

    try:
        storage_socket.role.update(rolename, permissions)
        return jsonify({"msg": "Role was updated!"}), 200
    except Exception as e:
        current_app.logger.warning(f"Error updating role {rolename}: {str(e)}")
        return jsonify({"msg": "Failed to update role"}), 400


@main.route("/role", methods=["DELETE"])
@check_access
def delete_role():
    rolename = request.json["rolename"]

    try:
        storage_socket.role.delete(rolename)
        return jsonify({"msg": "Role was deleted!"}), 200
    except Exception as e:
        current_app.logger.warning(f"Error deleting role {rolename}: {str(e)}")
        return jsonify({"msg": "Failed to delete role!"}), 400
