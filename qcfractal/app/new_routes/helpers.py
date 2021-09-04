from typing import List

from flask import request, current_app, Response
from qcelemental.util import serialize
from werkzeug.exceptions import BadRequest

from qcfractal.interface.models import QueryMetadata, InsertMetadata
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
        encoding = _valid_encodings[content_type]
        response = serialize(response, encoding)
        super(SerializedResponse, self).__init__(response, content_type=content_type, **kwargs)
