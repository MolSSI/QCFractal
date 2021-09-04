from flask import jsonify
from werkzeug.exceptions import NotFound

from qcfractal.app import storage_socket, view_handler
from qcfractal.app.new_routes.helpers import parse_bodymodel, SerializedResponse
from qcfractal.app.new_routes.main import main
from qcfractal.app.new_routes.permissions import check_access
from qcfractal.interface.models import rest_model
from qcfractal.storage_sockets.storage_utils import add_metadata_template


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

        cols = storage_socket.collection.get(**body.data.dict(), include=body.meta.include, exclude=body.meta.exclude)
        response = response_model(**cols)

    # Get specific collection
    elif (collection_id is not None) and (view_function is None):
        body_model, response_model = rest_model("collection", "get")

        body = parse_bodymodel(body_model)
        cols = storage_socket.collection.get(
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

    ret = storage_socket.collection.add(body.data.dict(), overwrite=body.meta.overwrite)
    response = response_model(**ret)

    return SerializedResponse(response)


@main.route("/collection", methods=["DELETE"])
@main.route("/collection/<int:collection_id>", methods=["DELETE"])
@check_access
def delete_collection(collection_id: int, view_function: str):
    body_model, response_model = rest_model(f"collection/{collection_id}", "delete")
    ret = storage_socket.collection.delete(col_id=collection_id)
    if ret == 0:
        return jsonify(msg="Collection does not exist."), 404
    else:
        response = response_model(meta={"success": True, "errors": [], "error_description": False})

    return SerializedResponse(response)
