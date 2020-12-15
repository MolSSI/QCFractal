"""
Web handlers for the FractalServer.
"""

from qcelemental.util import serialize
from .interface.models.rest_models import rest_model
from .storage_sockets.storage_utils import add_metadata_template
from flask import jsonify, request
from flask_jwt_extended import (
    jwt_required,
    fresh_jwt_required,
    create_access_token,
    get_jwt_claims,
    get_current_user,
    jwt_refresh_token_required,
    create_refresh_token,
    get_jwt_identity
)

_valid_encodings = {
    "application/json": "json",
    "application/json-ext": "json-ext",
    "application/msgpack-ext": "msgpack-ext",
}

class APIHandler():
    def register(self):
        if request.is_json:
            email = request.json['email']
            password = request.json['password']
        else:
            email = request.form['email']
            password = request.form['password']

        success = self.storage.add_user(email, password=password, rolename="user")
        if success:
            return jsonify({'message': 'New user created!'}), 201
        else:
            print("\n>>> Failed to add user. Perhaps the username is already taken?")
            return jsonify({'message': 'Failed to add user.'}), 500

    def login(self):
        if request.is_json:
            email = request.json['email']
            password = request.json['password']
        else:
            email = request.form['email']
            password = request.form['password']

        success, error_message, permissions = self.storage.verify_user(email, password)
        if success:
            access_token = create_access_token(identity=email, user_claims={"permissions": permissions})
            refresh_token = create_refresh_token(identity=email)
            return jsonify(message="Login succeeded!", access_token=access_token,
                           refresh_token=refresh_token), 200
        else:
            return jsonify(message=error_message), 401

            @self.server.route('/')
            def home_func():
                return '<h1>Success</h1>'

            # Then, you make it an object member manually:
            self.home = home_func

    def parse_bodymodel(self, model):

        try:
            return model(**self.data)
        # TODO: refactor
        except Exception as e:
            # return "Invalid REST", 400
            raise Exception(e)

    def get_information(self):
        current_user = get_current_user()
        public_information = {
            "name": "self.name",
            "heartbeat_frequency": "self.heartbeat_frequency",
            "version": "version",
            "query_limit": "self.storage.get_limit(1.0e9)",
            "client_lower_version_limit": "0.12.1",
            "client_upper_version_limit": "0.13.99",
        }
        return jsonify(public_information)

    @jwt_refresh_token_required
    def refresh(self):
        email = get_jwt_identity()
        ret = {
            'access_token': create_access_token(identity=email)
        }
        return jsonify(ret), 200

    def fresh_login(self):
        if request.is_json:
            email = request.json['email']
            password = request.json['password']
        else:
            email = request.form['email']
            password = request.form['password']

        success, error_message, permissions = self.storage.verify_user(email, password)
        if success:
            access_token = create_access_token(identity=email, user_claims={"permissions": permissions}, fresh=True)
            return jsonify(message="Fresh login succeeded!", access_token=access_token), 200
        else:
            return jsonify(message=error_message), 401

    @jwt_required
    def get_molecule(self):
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
        content_type = request.headers.get("Content-Type", "application/json")
        encoding = _valid_encodings[content_type]

        body_model, response_model = rest_model("molecule", "get")
        body = self.parse_bodymodel(body_model)
        molecules = self.storage.get_molecules(**{**body.data.dict(), **body.meta.dict()})
        ret = response_model(**molecules)

        if not isinstance(ret, (str, bytes)):
            data = serialize(ret, encoding)

        return data

    @jwt_required
    def post_molecule(self):
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
        content_type = request.headers.get("Content-Type", "application/json")
        encoding = _valid_encodings[content_type]

        body_model, response_model = rest_model("molecule", "post")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.add_molecules(body.data)
        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_kvstore(self):
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
        content_type = request.headers.get("Content-Type", "application/json")
        encoding = _valid_encodings[content_type]

        body_model, response_model = rest_model("kvstore", "get")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.get_kvstore(body.data.id)
        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_collection(self, collection_id: int, view_function: str):
        # List collections
        if (collection_id is None) and (view_function is None):
            body_model, response_model = rest_model("collection", "get")
            body = self.parse_bodymodel(body_model)

            cols = self.storage.get_collections(
                **body.data.dict(), include=body.meta.include, exclude=body.meta.exclude
            )
            response = response_model(**cols)

        # Get specific collection
        elif (collection_id is not None) and (view_function is None):
            body_model, response_model = rest_model("collection", "get")

            body = self.parse_bodymodel(body_model)
            cols = self.storage.get_collections(
                **body.data.dict(), col_id=int(collection_id), include=body.meta.include, exclude=body.meta.exclude
            )
            response = response_model(**cols)

        # View-backed function on collection
        elif (collection_id is not None) and (view_function is not None):
            body_model, response_model = rest_model(f"collection/{collection_id}/{view_function}", "get")
            body = self.parse_bodymodel(body_model)
            if view_handler is None:
                meta = {
                    "success": False,
                    "error_description": "Server does not support collection views.",
                    "errors": [],
                    "msgpacked_cols": [],
                }
                response = response_model(meta=meta, data=None)
                if not isinstance(response, (str, bytes)):
                    data = serialize(response, encoding)

                return data

            result = view_handler.handle_request(collection_id, view_function, body.data.dict())
            response = response_model(**result)

        # Unreachable?
        else:
            body_model, response_model = rest_model("collection", "get")
            meta = add_metadata_template()
            meta["success"] = False
            meta["error_description"] = "GET request for view with no collection ID not understood."
            response = response_model(meta=meta, data=None)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    @jwt_required
    def post_collection(self, collection_id: int, view_function: str):
        body_model, response_model = rest_model("collection", "post")
        body = self.parse_bodymodel(body_model)

        # POST requests not supported for anything other than "/collection"
        if collection_id is not None or view_function is not None:
            meta = add_metadata_template()
            meta["success"] = False
            meta["error_description"] = "POST requests not supported for sub-resources of /collection"
            response = response_model(meta=meta, data=None)
            if not isinstance(response, (str, bytes)):
                data = serialize(response, encoding)

            return data

        ret = self.storage.add_collection(body.data.dict(), overwrite=body.meta.overwrite)
        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    @jwt_required
    def delete_collection(self, collection_id: int, view_function: str):
        body_model, response_model = rest_model(f"collection/{collection_id}", "delete")
        ret = self.storage.del_collection(col_id=collection_id)
        if ret == 0:
            return jsonify(message="Collection does not exist."), 404
        else:
            response = response_model(meta={"success": True, "errors": [], "error_description": False})

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_result(self, query_type: str):
        content_type = request.headers.get("Content-Type", "application/json")
        encoding = _valid_encodings[content_type]

        body_model, response_model = rest_model("procedure", query_type)
        body = self.parse_bodymodel(body_model)

        try:
            if query_type == "get":
                ret = self.storage.get_procedures(**{**body.data.dict(), **body.meta.dict()})
            else:  # all other queries, like 'best_opt_results'
                ret = self.storage.custom_query("procedure", query_type, **{**body.data.dict(), **body.meta.dict()})
        except KeyError as e:
            return jsonify(message=KeyError), 500

        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_wave_function(self):
        content_type = request.headers.get("Content-Type", "application/json")
        encoding = _valid_encodings[content_type]

        body_model, response_model = rest_model("wavefunctionstore", "get")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.get_wavefunction_store(body.data.id, include=body.meta.include)
        if len(ret["data"]):
            ret["data"] = ret["data"][0]
        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_procedure(self, query_type: str):
        body_model, response_model = rest_model("procedure", query_type)
        body = self.parse_bodymodel(body_model)

        try:
            if query_type == "get":
                ret = self.storage.get_procedures(**{**body.data.dict(), **body.meta.dict()})
            else:  # all other queries, like 'best_opt_results'
                ret = self.storage.custom_query("procedure", query_type, **{**body.data.dict(), **body.meta.dict()})
        except KeyError as e:
            return jsonify(message=KeyError), 500

        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_optimization(self, query_type: str):
        body_model, response_model = rest_model(f"optimization/{query_type}", "get")
        body = self.parse_bodymodel(body_model)

        try:
            if query_type == "get":
                ret = self.storage.get_procedures(**{**body.data.dict(), **body.meta.dict()})
            else:  # all other queries, like 'best_opt_results'
                ret = self.storage.custom_query("optimization", query_type, **{**body.data.dict(), **body.meta.dict()})
        except KeyError as e:
            return jsonify(message=KeyError), 500

        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_task_queue(self):
        body_model, response_model = rest_model("task_queue", "get")
        body = self.parse_bodymodel(body_model)

        tasks = self.storage.get_queue(**{**body.data.dict(), **body.meta.dict()})
        response = response_model(**tasks)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    @jwt_required
    def post_task_queue(self):
        body_model, response_model = rest_model("task_queue", "post")
        body = self.parse_bodymodel(body_model)

        # Format and submit tasks
        if not check_procedure_available(body.meta.procedure):
            return jsonify(message="Unknown procedure {}.".format(body.meta.procedure)), 500

        procedure_parser = get_procedure_parser(body.meta.procedure, storage, logger)

        # Verify the procedure
        verify = procedure_parser.verify_input(body)
        if verify is not True:
            return jsonify(message="Verify error"), 400

        payload = procedure_parser.submit_tasks(body)
        response = response_model(**payload)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    @jwt_required
    def put_task_queue(self):
        body_model, response_model = rest_model("task_queue", "put")
        body = self.parse_bodymodel(body_model)

        if (body.data.id is None) and (body.data.base_result is None):
            return jsonify(message="Id or ResultId must be specified."), 400
        if body.meta.operation == "restart":
            tasks_updated = self.storage.queue_reset_status(**body.data.dict(), reset_error=True)
            data = {"n_updated": tasks_updated}
        else:
            return jsonify(message="Operation '{operation}' is not valid."), 400

        response = response_model(data=data, meta={"errors": [], "success": True, "error_description": False})

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_service_queue(self):
        body_model, response_model = rest_model("service_queue", "get")
        body = self.parse_bodymodel(body_model)

        ret = self.storage.get_services(**{**body.data.dict(), **body.meta.dict()})
        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    @jwt_required
    def post_service_queue(self):
        """Posts new services to the service queue."""

        body_model, response_model = rest_model("service_queue", "post")
        body = self.parse_bodymodel(body_model)

        new_services = []
        for service_input in body.data:
            # Get molecules with ids
            if isinstance(service_input.initial_molecule, list):
                molecules = self.storage.get_add_molecules_mixed(service_input.initial_molecule)["data"]
                if len(molecules) != len(service_input.initial_molecule):
                    return jsonify(message=KeyError), 500
            else:
                molecules = self.storage.get_add_molecules_mixed([service_input.initial_molecule])["data"][0]

            # Update the input and build a service object
            service_input = service_input.copy(update={"initial_molecule": molecules})
            new_services.append(
                initialize_service(
                    storage, logger, service_input, tag=body.meta.tag, priority=body.meta.priority
                )
            )

        ret = self.storage.add_services(new_services)
        ret["data"] = {"ids": ret["data"], "existing": ret["meta"]["duplicates"]}
        ret["data"]["submitted"] = list(set(ret["data"]["ids"]) - set(ret["meta"]["duplicates"]))
        response = response_model(**ret)

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    @jwt_required
    def put_service_queue(self):
        """Modifies services in the service queue"""

        body_model, response_model = rest_model("service_queue", "put")
        body = self.parse_bodymodel(body_model)

        if (body.data.id is None) and (body.data.procedure_id is None):
            return jsonify(message="Id or ProcedureId must be specified."), 400

        if body.meta.operation == "restart":
            updates = self.storage.update_service_status("running", **body.data.dict())
            data = {"n_updated": updates}
        else:
            return jsonify(message="Operation '{operation}' is not valid."), 400

        response = response_model(data=data, meta={"errors": [], "success": True, "error_description": False})

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def _get_name_from_metadata(meta):
        """
        Form the canonical name string.
        """
        ret = meta.cluster + "-" + meta.hostname + "-" + meta.uuid
        return ret

    def insert_complete_tasks(storage_socket, results, logger):
        # Pivot data so that we group all results in categories
        new_results = collections.defaultdict(list)

        queue = self.storage_socket.get_queue(id=list(results.keys()))["data"]
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

        def get_queue_manager(sefl):
            """Pulls new tasks from the task queue"""

            body_model, response_model = rest_model("queue_manager", "get")
            body = self.parse_bodymodel(body_model)

            # Figure out metadata and kwargs
            name = _get_name_from_metadata(body.meta)

            # Grab new tasks and write out
            new_tasks = self.storage.queue_get_next(
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
            storage.manager_update(name, submitted=len(new_tasks), **body.meta.dict())
            if not isinstance(response, (str, bytes)):
                data = serialize(response, encoding)

            return data

    @jwt_required
    def post_queue_manager(self):
        """Posts complete tasks to the task queue"""

        body_model, response_model = rest_model("queue_manager", "post")
        body = self.parse_bodymodel(body_model)

        name = _get_name_from_metadata(body.meta)
        # logger.info("QueueManager: Received completed task packet from {}.".format(name))
        success, error = insert_complete_tasks(storage, body.data, logger)

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

        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def put_queue_manager(self):
        """
        Various manager manipulation operations
        """

        ret = True

        body_model, response_model = rest_model("queue_manager", "put")
        body = self.parse_bodymodel(body_model)

        name = _get_name_from_metadata(body.meta)
        op = body.data.operation
        if op == "startup":
            storage.manager_update(
                name, status="ACTIVE", configuration=body.data.configuration, **body.meta.dict(), log=True
            )
            # logger.info("QueueManager: New active manager {} detected.".format(name))

        elif op == "shutdown":
            nshutdown = self.storage.queue_reset_status(manager=name, reset_running=True)
            storage.manager_update(name, returned=nshutdown, status="INACTIVE", **body.meta.dict(), log=True)

            # logger.info("QueueManager: Shutdown of manager {} detected, recycling {} incomplete tasks.".format(name, nshutdown))

            ret = {"nshutdown": nshutdown}

        elif op == "heartbeat":
            storage.manager_update(name, status="ACTIVE", **body.meta.dict(), log=True)
            # logger.debug("QueueManager: Heartbeat of manager {} detected.".format(name))

        else:
            msg = "Operation '{}' not understood.".format(op)
            return jsonify(message=msg), 400

        response = response_model(**{"meta": {}, "data": ret})
        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    def get_manager(self):
        """Gets manager information from the task queue"""

        body_model, response_model = rest_model("manager", "get")
        body = self.parse_bodymodel(body_model)

        # logger.info("GET: ComputeManagerHandler")
        managers = self.storage.get_managers(**{**body.data.dict(), **body.meta.dict()})

        # remove passwords?
        # TODO: Are passwords stored anywhere else? Other kinds of passwords?
        for m in managers["data"]:
            if "configuration" in m and isinstance(m["configuration"], dict) and "server" in m["configuration"]:
                m["configuration"]["server"].pop("password", None)

        response = response_model(**managers)
        if not isinstance(response, (str, bytes)):
            data = serialize(response, encoding)

        return data

    @jwt_required
    def get_roles(self):
        roles = self.storage.get_roles()
        return jsonify(roles), 200

    @jwt_required
    def get_role(self, rolename: str):

        success, role = self.storage.get_role(rolename)
        return jsonify(role), 200

    @jwt_required
    def create_role(self):
        rolename = request.json['rolename']
        permissions = request.json['permissions']

        success, error_message = self.storage.create_role(rolename, permissions)
        if success:
            return jsonify({'message': 'New role created!'}), 201
        else:
            return jsonify({'message': error_message}), 400

    @fresh_jwt_required
    def update_role(self):
        rolename = request.json['rolename']
        permissions = request.json['permissions']

        success = self.storage.update_role(rolename, permissions)
        if success:
            return jsonify({'message': 'Role was updated!'}), 200
        else:
            return jsonify({'message': 'Failed to update role'}), 400

    @fresh_jwt_required
    def delete_role(self):
        rolename = request.json['rolename']

        success = self.storage.delete_role(rolename)
        if success:
            return jsonify({'message': 'Role was deleted!.'}), 200
        else:
            return jsonify({'message': 'Filed to delete role!.'}), 400
