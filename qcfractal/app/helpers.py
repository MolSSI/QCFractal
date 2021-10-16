from __future__ import annotations

from typing import List, Optional, Callable

from flask import request, current_app, Response
from qcelemental.util import serialize
from werkzeug.exceptions import BadRequest

from qcfractal.portal.metadata_models import QueryMetadata, InsertMetadata, DeleteMetadata
from qcfractal.interface.models.rest_models import ResponseGETMeta, ResponsePOSTMeta

_valid_encodings = {
    "application/json": "json",
    "application/json-ext": "json-ext",
    "application/msgpack-ext": "msgpack-ext",
}


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
        content_type = "application/json"
        encoding = _valid_encodings[content_type]
        response = serialize(response, encoding)
        super(SerializedResponse, self).__init__(response, content_type=content_type, **kwargs)


def get_helper(id: Optional[int], id_args: Optional[List[int]], missing_ok: bool, func: Callable):
    """
    A general helper for calling a get_* function of a component

    All these functions share the same signature and have the same behavior, so we can
    handle that in a common function
    """

    # If an empty list was specified in the query params, it won't be sent
    # and the id member of the args will be None
    if id is None and id_args is None:
        return []

    # If an id was specified in the url (keyword/1234) then use that
    # Otherwise, grab from the query parameters
    if id is not None:
        return func([id], missing_ok=missing_ok)[0]
    else:
        return func(id_args, missing_ok=missing_ok)


def delete_helper(id: Optional[int], id_args: Optional[List[int]], func: Callable) -> DeleteMetadata:
    """
    A general helper for calling a delete_* function of a component

    All these functions share the same signature and have the same behavior, so we can
    handle that in a common function
    """

    # If an id was specified in the url (keyword/1234) then use that
    # Otherwise, grab from the query parameters
    if id is not None:
        return func([id])
    else:
        return func(id_args)
