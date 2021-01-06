from routes import main
from flask import jsonify, json
from werkzeug.exceptions import HTTPException, BadRequest


# TODO: not called
@main.errorhandler(BadRequest)
# @main.errorhandler(KeyError)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


# @main.errorhandler(HTTPException)
# # @main.errorhandler(BadRequest)
# def handle_exception(e):
#     """Return JSON instead of HTML for HTTP errors."""
#     # start with the correct headers and status code from the error
#     response = e.get_response()
#
#     code = e.code
#     if not code:
#         if isinstance(e, BadRequest):
#             code = 400
#
#     # replace the body with JSON
#     response.data = json.dumps({
#         "code": code,
#         "name": e.name,
#         "description": e.description,
#     })
#     response.content_type = "application/json"
#     return response
