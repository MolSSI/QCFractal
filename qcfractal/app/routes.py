"""
Routes handlers for Flask
"""

from qcelemental.util import deserialize, serialize
from ..storage_sockets.storage_utils import add_metadata_template
from ..interface.models.rest_models import rest_model
# from ..interface.models.task_models import PriorityEnum, TaskStatusEnum
# from ..interface.models.records import RecordStatusEnum
# from ..interface.models.model_builder import build_procedure
from ..procedures import check_procedure_available, get_procedure_parser
from ..services import initialize_service
from flask import jsonify, request, make_response
import traceback
import collections
from flask_jwt_extended import (
    fresh_jwt_required,
    create_access_token,
    get_jwt_claims,
    jwt_refresh_token_required,
    create_refresh_token,
    get_jwt_identity,
    verify_jwt_in_request,
)
from urllib.parse import urlparse
from ..policyuniverse import Policy
from flask import Blueprint, current_app, session, Response
from . import jwt
import logging
import json
from functools import wraps

from werkzeug.exceptions import HTTPException, BadRequest, NotFound, \
    Forbidden, Unauthorized


logger = logging.getLogger(__name__)

main = Blueprint('main', __name__)


_valid_encodings = {
    "application/json": "json",
    "application/json-ext": "json-ext",
    "application/msgpack-ext": "msgpack-ext",
}

#TODO: not implemented yet
_logging_param_counts = {'id'}


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

        logger.debug(f'JWT_ENABLED: {current_app.config.JWT_ENABLED}')
        logger.debug(f'ALLOW_READ: {current_app.config.ALLOW_READ}')

        # if no auth required, always allowed
        if not current_app.config.JWT_ENABLED:
            return fn(*args, **kwargs)

        # if read is allowed without login, load read permissions from DB
        # otherwise, check logged-in permissions
        if current_app.config.ALLOW_READ:
            _, permissions = current_app.config.storage.get_role('read')
            permissions = permissions['permissions']
        else:
            # read JWT token from request headers
            verify_jwt_in_request()
            claims = get_jwt_claims()
            permissions = claims.get('permissions', None)

        try:
            # host_url = request.host_url
            identity = get_jwt_identity()
            resource = urlparse(request.url).path.split("/")[1]
            context = {
                "Principal": identity,
                "Action": request.method,
                "Resource": resource
                # "IpAddress": request.remote_addr,
                # "AccessTime": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            }
            logger.info(f'Permissions: {permissions}')
            logger.info(f'Context: {context}')
            policy = Policy(permissions)
            if policy.evaluate(context):
                return fn(*args, **kwargs)
            else:
                return Forbidden(f"User {identity} is not authorized to access '{resource}' resource.")

        except Exception as e:
            logger.info("Error in evaluating JWT permissions: \n" + str(e) )
            # logger.info(f"Permissions: {permissions}")
            return BadRequest("Error in evaluating JWT permissions")

    return wrapper


def parse_bodymodel(model):
    """Parse request body using pydantic models"""

    try:
        return model(**request.data)
    except Exception as e:
        logger.error("Invalid request body:\n" + str(e))
        raise BadRequest("Invalid body: " + str(e))


class PydanticResponse(Response):
    """Serialize pydantic response using the given encoding and pass it
    as a flask response object"""

    def __init__(self, response, **kwargs):

        if not isinstance(response, (str, bytes)):
            response = serialize(response, session['encoding'])

        return super(PydanticResponse, self).__init__(response, **kwargs)


@main.before_request
def before_request_func():

    # session['content_type'] = "Not Provided"
    try:
        # default to "application/json"
        session['content_type'] = request.headers.get("Content-Type", "application/json")
        session['encoding'] = _valid_encodings[session['content_type']]
    except KeyError:
        raise BadRequest(f"Did not understand 'Content-Type': {session['content_type']}")

    # TODO: check if needed in Flask
    try:
        if (session['encoding'] == "json") and isinstance(request.data, bytes):
            blob = request.data.decode()
        else:
            blob = request.data

        if blob: #TODO:
            request.data = deserialize(blob, session['encoding'])
        else:
            request.data = None
    except:
        raise BadRequest("Could not deserialize body.")


@main.after_request
def after_request_func(response):

     # Always reply in the format sent

    response.headers['Content-Type'] = session['content_type']

    exclude_uris = ["/task_queue", "/service_queue", "/queue_manager"]

    # No associated data, so skip all of this
    # (maybe caused by not using portal or not using the REST API correctly?)
    if request.data is None:
        return

    if current_app.config.api_logger and request.method == "GET" and request.path not in exclude_uris:

        extra_params = request.data.copy()
        if _logging_param_counts:
            for key in _logging_param_counts:
                if extra_params["data"].get(key, None):
                    extra_params["data"][key] = len(extra_params["data"][key])

        if "data" in extra_params:
            extra_params["data"] = {k: v for k, v in extra_params["data"].items() if v is not None}

        extra_params = json.dumps(extra_params)

        log = current_app.config.api_logger.get_api_access_log(request=request, extra_params=extra_params)
        current_app.config.storage.save_access(log)

        # logger.info('Done saving API access to the database')

    return response

@main.errorhandler(Exception)
def handle_python_errors(error):
    response = jsonify(str(error))
    response.status_code = 400
    return response

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#                            Routes
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

@main.route('/register', methods=['POST'])
def register():
    if request.is_json:
        username = request.json['username']
        password = request.json['password']
    else:
        username = request.form['username']
        password = request.form['password']

    success = current_app.config.storage.add_user(username, password=password, rolename="user")
    if success:
        return jsonify({'message': 'New user created!'}), 201
    else:
        logger.info("\n>>> Failed to add user. Perhaps the username is already taken?")
        return jsonify({'message': 'Failed to add user.'}), 500


@main.route('/login', methods=['POST'])
def login():
    if request.is_json:
        username = request.json['username']
        password = request.json['password']
    else:
        username = request.form['username']
        password = request.form['password']

    success, error_message, permissions = current_app.config.storage.verify_user(username, password)
    if success:
        access_token = create_access_token(identity=username, user_claims={"permissions": permissions})
        refresh_token = create_refresh_token(identity=username)
        return jsonify(message="Login succeeded!", access_token=access_token,
                       refresh_token=refresh_token), 200
    else:
        return Unauthorized(error_message)


@main.route('/information', methods=['GET'])
def get_information():

    return PydanticResponse(current_app.config.public_information)


@main.route('/refresh', methods=['POST'])
@jwt_refresh_token_required
def refresh():
    username = get_jwt_identity()
    ret = {
        'access_token': create_access_token(identity=username)
    }
    return jsonify(ret), 200


@main.route('/fresh-login', methods=['POST'])
def fresh_login():
    if request.is_json:
        username = request.json['username']
        password = request.json['password']
    else:
        username = request.form['username']
        password = request.form['password']

    success, error_message, permissions = current_app.config.storage.verify_user(username, password)
    if success:
        access_token = create_access_token(identity=username, user_claims={"permissions": permissions}, fresh=True)
        return jsonify(message="Fresh login succeeded!", access_token=access_token), 200
    else:
        return Unauthorized(error_message)


@main.route('/molecule', methods=['GET'])
@check_access
def get_molecule():
    """
    Request:
        "meta" - Overall options to the Molecule pull request
            - "index" - What kind of index used to find the data ("id", "molecule_hash", "molecular_formula")
        "data" - A dictionary of {key : index} requests

    Returns:
        "meta" - Metadata associated with the query
            - "errors" - A list of errors in (index, error_id) format.
            - "n_found" - The number of molecule found.
            - "success" - If the query was successful or not.
            - "error_description" - A string based description of the error or False
            - "missing" - A list of keys that were not found.
        "data" - A dictionary of {key : molecule JSON} results
    """

    body_model, response_model = rest_model("molecule", "get")
    body = parse_bodymodel(body_model)
    molecules = current_app.config.storage.get_molecules(**{**body.data.dict(), **body.meta.dict()})
    response = response_model(**molecules)

    return PydanticResponse(response)


@main.route('/molecule', methods=['POST'])
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

    body_model, response_model = rest_model("molecule", "post")
    body = parse_bodymodel(body_model)

    ret = current_app.config.storage.add_molecules(body.data)
    response = response_model(**ret)

    return PydanticResponse(response)


@main.route('/kvstore', methods=['GET'])
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

    body_model, response_model = rest_model("kvstore", "get")
    body = parse_bodymodel(body_model)

    ret = current_app.config.storage.get_kvstore(body.data.id)
    response = response_model(**ret)

    return PydanticResponse(response)


@main.route('/keyword', methods=['GET'])
@check_access
def get_keyword():
    body_model, response_model = rest_model("keyword", "get")
    body = parse_bodymodel(body_model)

    ret = current_app.config.storage.get_keywords(**{**body.data.dict(), **body.meta.dict()}, with_ids=False)
    response = response_model(**ret)

    current_app.config.logger.info("GET: Keywords - {} pulls.".format(len(response.data)))
    return PydanticResponse(response)


@main.route('/keyword', methods=['POST'])
@check_access
def post_keyword():

    body_model, response_model = rest_model("keyword", "post")
    body = parse_bodymodel(body_model)

    ret = current_app.config.storage.add_keywords(body.data)
    response = response_model(**ret)

    current_app.config.logger.info("POST: Keywords - {} inserted.".format(response.meta.n_inserted))
    return PydanticResponse(response)


@main.route('/collection', methods=['GET'])
@main.route('/collection/<int:collection_id>', methods=['GET'])
@main.route('/collection/<int:collection_id>/<string:view_function>', methods=['GET'])
@check_access
def get_collection(collection_id: int=None, view_function: str=None):
    # List collections

    view_function_vals = ('value', 'entry', 'list', 'molecule')
    if view_function is not None and view_function not in view_function_vals:
        raise NotFound(f"URL Not Found. view_function must be in : {view_function_vals}")

    if (collection_id is None) and (view_function is None):
        body_model, response_model = rest_model("collection", "get")
        body = parse_bodymodel(body_model)

        cols = current_app.config.storage.get_collections(
            **body.data.dict(), include=body.meta.include, exclude=body.meta.exclude
        )
        response = response_model(**cols)

    # Get specific collection
    elif (collection_id is not None) and (view_function is None):
        body_model, response_model = rest_model("collection", "get")

        body = parse_bodymodel(body_model)
        cols = current_app.config.storage.get_collections(
            **body.data.dict(), col_id=int(collection_id), include=body.meta.include, exclude=body.meta.exclude
        )
        response = response_model(**cols)

    # View-backed function on collection
    elif (collection_id is not None) and (view_function is not None):
        body_model, response_model = rest_model(f"collection/{collection_id}/{view_function}", "get")
        body = parse_bodymodel(body_model)
        if current_app.config.view_handler is None:
            meta = {
                "success": False,
                "error_description": "Server does not support collection views.",
                "errors": [],
                "msgpacked_cols": [],
            }
            response = response_model(meta=meta, data=None)
            return PydanticResponse(response)

        result = current_app.config.view_handler.handle_request(collection_id, view_function, body.data.dict())
        response = response_model(**result)

    # Unreachable?
    else:
        body_model, response_model = rest_model("collection", "get")
        meta = add_metadata_template()
        meta["success"] = False
        meta["error_description"] = "GET request for view with no collection ID not understood."
        response = response_model(meta=meta, data=None)

    return PydanticResponse(response)


@main.route('/collection', methods=['POST'])
@main.route('/collection/<int:collection_id>', methods=['POST'])
@main.route('/collection/<int:collection_id>/<string:view_function>', methods=['POST'])
@check_access
def post_collection(collection_id: int=None, view_function: str=None):

    view_function_vals = ('value', 'entry', 'list', 'molecule')
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

        return PydanticResponse(response)

    ret = current_app.config.storage.add_collection(body.data.dict(), overwrite=body.meta.overwrite)
    response = response_model(**ret)

    return PydanticResponse(response)


@main.route('/collection', methods=['DELETE'])
@main.route('/collection/<int:collection_id>', methods=['DELETE'])
@check_access
def delete_collection(collection_id: int, view_function: str):
    body_model, response_model = rest_model(f"collection/{collection_id}", "delete")
    ret = current_app.config.storage.del_collection(col_id=collection_id)
    if ret == 0:
        return jsonify(message="Collection does not exist."), 404
    else:
        response = response_model(meta={"success": True, "errors": [], "error_description": False})

    return PydanticResponse(response)


@main.route('/result', methods=['GET'])
@check_access
def get_result():

    body_model, response_model = rest_model("result", "get")
    body = parse_bodymodel(body_model)

    ret = current_app.config.storage.get_results(**{**body.data.dict(), **body.meta.dict()})
    response = response_model(**ret)

    logger.info("GET: Results - {} pulls.".format(len(response.data)))

    return PydanticResponse(response)


@main.route('/wavefunctionstore', methods=['GET'])
@check_access
def get_wave_function():

    body_model, response_model = rest_model("wavefunctionstore", "get")
    body = parse_bodymodel(body_model)

    ret = current_app.config.storage.get_wavefunction_store(body.data.id, include=body.meta.include)
    if len(ret["data"]):
        ret["data"] = ret["data"][0]
    response = response_model(**ret)

    return PydanticResponse(response)


@main.route('/procedure', methods=['GET'])
@main.route('/procedure/<string:query_type>', methods=['GET'])
@check_access
def get_procedure(query_type: str = 'get'):
    body_model, response_model = rest_model("procedure", query_type)
    body = parse_bodymodel(body_model)

    try:
        if query_type == "get":
            ret = current_app.config.storage.get_procedures(**{**body.data.dict(), **body.meta.dict()})
        else:  # all other queries, like 'best_opt_results'
            ret = current_app.config.storage.custom_query("procedure", query_type, **{**body.data.dict(), **body.meta.dict()})
    except KeyError as e:
        return jsonify(message=KeyError), 500

    response = response_model(**ret)

    return PydanticResponse(response)


@main.route('/optimization/<string:query_type>', methods=['GET'])
@check_access
def get_optimization(query_type: str):
    body_model, response_model = rest_model(f"optimization/{query_type}", "get")
    body = parse_bodymodel(body_model)

    try:
        if query_type == "get":
            ret = current_app.config.storage.get_procedures(**{**body.data.dict(), **body.meta.dict()})
        else:  # all other queries, like 'best_opt_results'
            ret = current_app.config.storage.custom_query("optimization", query_type, **{**body.data.dict(), **body.meta.dict()})
    except KeyError as e:
        return jsonify(message=KeyError), 500

    response = response_model(**ret)

    return PydanticResponse(response)


@main.route('/task_queue', methods=['GET'])
@check_access
def get_task_queue():
    body_model, response_model = rest_model("task_queue", "get")
    body = parse_bodymodel(body_model)

    tasks = current_app.config.storage.get_queue(**{**body.data.dict(), **body.meta.dict()})
    response = response_model(**tasks)

    return PydanticResponse(response)


@main.route('/task_queue', methods=['POST'])
@check_access
def post_task_queue():
    body_model, response_model = rest_model("task_queue", "post")
    body = parse_bodymodel(body_model)

    # Format and submit tasks
    if not check_procedure_available(body.meta.procedure):
        return jsonify(message="Unknown procedure {}.".format(body.meta.procedure)), 500

    procedure_parser = get_procedure_parser(body.meta.procedure,
                                            current_app.config.storage,
                                            current_app.config.logger)

    # Verify the procedure
    verify = procedure_parser.verify_input(body)
    if verify is not True:
        return jsonify(message="Verify error"), 400

    payload = procedure_parser.submit_tasks(body)
    response = response_model(**payload)

    return PydanticResponse(response)


@main.route('/task_queue', methods=['PUT'])
@check_access
def put_task_queue():
    body_model, response_model = rest_model("task_queue", "put")
    body = parse_bodymodel(body_model)

    if (body.data.id is None) and (body.data.base_result is None):
        return jsonify(message="Id or ResultId must be specified."), 400
    if body.meta.operation == "restart":
        tasks_updated = current_app.config.storage.queue_reset_status(**body.data.dict(), reset_error=True)
        data = {"n_updated": tasks_updated}
    else:
        return jsonify(message="Operation '{operation}' is not valid."), 400

    response = response_model(data=data, meta={"errors": [], "success": True, "error_description": False})

    return PydanticResponse(response)


@main.route('/service_queue', methods=['GET'])
@check_access
def get_service_queue():
    body_model, response_model = rest_model("service_queue", "get")
    body = parse_bodymodel(body_model)

    ret = current_app.config.storage.get_services(**{**body.data.dict(), **body.meta.dict()})
    response = response_model(**ret)

    return PydanticResponse(response)


@main.route('/service_queue', methods=['POST'])
@check_access
def post_service_queue():
    """Posts new services to the service queue."""

    body_model, response_model = rest_model("service_queue", "post")
    body = parse_bodymodel(body_model)

    new_services = []
    for service_input in body.data:
        # Get molecules with ids
        if isinstance(service_input.initial_molecule, list):
            molecules = current_app.config.storage.get_add_molecules_mixed(service_input.initial_molecule)["data"]
            if len(molecules) != len(service_input.initial_molecule):
                return jsonify(message=KeyError), 500
        else:
            molecules = current_app.config.storage.get_add_molecules_mixed([service_input.initial_molecule])["data"][0]

        # Update the input and build a service object
        service_input = service_input.copy(update={"initial_molecule": molecules})
        new_services.append(
            initialize_service(
                current_app.config.storage, current_app.config.logger, service_input,
                tag=body.meta.tag, priority=body.meta.priority
            )
        )

    ret = current_app.config.storage.add_services(new_services)
    ret["data"] = {"ids": ret["data"], "existing": ret["meta"]["duplicates"]}
    ret["data"]["submitted"] = list(set(ret["data"]["ids"]) - set(ret["meta"]["duplicates"]))
    response = response_model(**ret)

    return PydanticResponse(response)


@main.route('/service_queue', methods=['PUT'])
@check_access
def put_service_queue():
    """Modifies services in the service queue"""

    body_model, response_model = rest_model("service_queue", "put")
    body = parse_bodymodel(body_model)

    if (body.data.id is None) and (body.data.procedure_id is None):
        return jsonify(message="Id or ProcedureId must be specified."), 400

    if body.meta.operation == "restart":
        updates = current_app.config.storage.update_service_status("running", **body.data.dict())
        data = {"n_updated": updates}
    else:
        return jsonify(message="Operation '{operation}' is not valid."), 400

    response = response_model(data=data, meta={"errors": [], "success": True, "error_description": False})

    return PydanticResponse(response)


def _get_name_from_metadata(meta):
    """
    Form the canonical name string.
    """
    ret = meta.cluster + "-" + meta.hostname + "-" + meta.uuid
    return ret


def insert_complete_tasks(storage_socket, results, logger):
    # Pivot data so that we group all results in categories
    new_results = collections.defaultdict(list)

    queue = storage_socket.get_queue(id=list(results.keys()))["data"]
    queue = {v.id: v for v in queue}

    error_data = []

    task_success = 0
    task_failures = 0
    task_totals = len(results.items())
    for key, result in results.items():
        try:
            # Successful task
            if result["success"] is False:
                if "error" not in result:
                    error = {"error_type": "not_supplied", "error_message": "No error message found on task."}
                else:
                    error = result["error"]

                logger.warning(
                    "Computation key {key} did not complete successfully:\n"
                    "error_type: {error_type}\nerror_message: {error_message}".format(key=str(key), **error)
                )

                error_data.append((key, error))
                task_failures += 1

            # Failed task
            elif key not in queue:
                logger.warning(f"Computation key {key} completed successfully, but not found in queue.")
                error_data.append((key, "Internal Error: Queue key not found."))
                task_failures += 1

            # Success!
            else:
                parser = queue[key].parser
                new_results[parser].append(
                    {"result": result, "task_id": key, "base_result": queue[key].base_result}
                )
                task_success += 1

        except Exception:
            msg = "Internal FractalServer Error:\n" + traceback.format_exc()
            logger.warning("update: ERROR\n{}".format(msg))
            error_data.append((key, msg))
            task_failures += 1

    if task_totals:
        logger.info(
            "QueueManager: Found {} complete tasks ({} successful, {} failed).".format(
                task_totals, task_success, task_failures
            )
        )

    # Run output parsers
    completed = []
    for k, v in new_results.items():
        procedure_parser = get_procedure_parser(k, storage_socket, logger)
        com, err, hks = procedure_parser.parse_output(v)
        completed.extend(com)
        error_data.extend(err)

    # Handle complete tasks
    storage_socket.queue_mark_complete(completed)
    storage_socket.queue_mark_error(error_data)
    return len(completed), len(error_data)


@main.route('/queue_manager', methods=['GET'])
@check_access
def get_queue_manager():
        """Pulls new tasks from the task queue"""

        body_model, response_model = rest_model("queue_manager", "get")
        body = parse_bodymodel(body_model)

        # Figure out metadata and kwargs
        name = _get_name_from_metadata(body.meta)

        # Grab new tasks and write out
        new_tasks = current_app.config.storage.queue_get_next(
            name, body.meta.programs, body.meta.procedures, limit=body.data.limit, tag=body.meta.tag
        )
        response = response_model(
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
        current_app.config.storage.manager_update(name, submitted=len(new_tasks), **body.meta.dict())

        return PydanticResponse(response)


@main.route('/queue_manager', methods=['POST'])
@check_access
def post_queue_manager():
    """Posts complete tasks to the task queue"""

    body_model, response_model = rest_model("queue_manager", "post")
    body = parse_bodymodel(body_model)

    name = _get_name_from_metadata(body.meta)
    # logger.info("QueueManager: Received completed task packet from {}.".format(name))
    success, error = insert_complete_tasks(current_app.config.storage, body.data,
                                           current_app.config.logger)

    completed = success + error

    response = response_model(
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

    return PydanticResponse(response)


@main.route('/queue_manager', methods=['PUT'])
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
        current_app.config.storage.manager_update(
            name, status="ACTIVE", configuration=body.data.configuration, **body.meta.dict(), log=True
        )
        # logger.info("QueueManager: New active manager {} detected.".format(name))

    elif op == "shutdown":
        nshutdown = current_app.config.storage.queue_reset_status(manager=name, reset_running=True)
        current_app.config.storage.manager_update(name, returned=nshutdown, status="INACTIVE", **body.meta.dict(), log=True)

        # logger.info("QueueManager: Shutdown of manager {} detected, recycling {} incomplete tasks.".format(name, nshutdown))

        ret = {"nshutdown": nshutdown}

    elif op == "heartbeat":
        current_app.config.storage.manager_update(name, status="ACTIVE", **body.meta.dict(), log=True)
        # logger.debug("QueueManager: Heartbeat of manager {} detected.".format(name))

    else:
        msg = "Operation '{}' not understood.".format(op)
        return jsonify(message=msg), 400

    response = response_model(**{"meta": {}, "data": ret})

    return PydanticResponse(response)


@main.route('/manager', methods=['GET'])
@check_access
def get_manager():
    """Gets manager information from the task queue"""

    body_model, response_model = rest_model("manager", "get")
    body = parse_bodymodel(body_model)

    # logger.info("GET: ComputeManagerHandler")
    managers = current_app.config.storage.get_managers(**{**body.data.dict(), **body.meta.dict()})

    # remove passwords?
    # TODO: Are passwords stored anywhere else? Other kinds of passwords?
    for m in managers["data"]:
        if "configuration" in m and isinstance(m["configuration"], dict) and "server" in m["configuration"]:
            m["configuration"]["server"].pop("password", None)

    response = response_model(**managers)

    return PydanticResponse(response)


@main.route('/role', methods=['GET'])
@check_access
def get_roles():
    roles = current_app.config.storage.get_roles()
    return jsonify(roles), 200


@main.route('/role/<string:rolename>', methods=['GET'])
@check_access
def get_role(rolename: str):

    success, role = current_app.config.storage.get_role(rolename)
    return jsonify(role), 200


@main.route('/role/<string:rolename>', methods=['POST'])
@check_access
def add_role():
    rolename = request.json['rolename']
    permissions = request.json['permissions']

    success, error_message = current_app.config.storage.add_role(rolename, permissions)
    if success:
        return jsonify({'message': 'New role created!'}), 201
    else:
        return jsonify({'message': error_message}), 400


@main.route('/role', methods=['PUT'])
@check_access
def update_role():
    rolename = request.json['rolename']
    permissions = request.json['permissions']

    success = current_app.config.storage.update_role(rolename, permissions)
    if success:
        return jsonify({'message': 'Role was updated!'}), 200
    else:
        return jsonify({'message': 'Failed to update role'}), 400


@main.route('/role', methods=['DELETE'])
@check_access
def delete_role():
    rolename = request.json['rolename']

    success = current_app.config.storage.delete_role(rolename)
    if success:
        return jsonify({'message': 'Role was deleted!.'}), 200
    else:
        return jsonify({'message': 'Filed to delete role!.'}), 400
