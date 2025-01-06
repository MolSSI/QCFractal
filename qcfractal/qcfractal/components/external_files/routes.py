from flask import redirect

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.api_v1.helpers import wrap_route


@api_v1.route("/external_files/<int:file_id>", methods=["GET"])
@wrap_route("READ")
def get_external_file_metadata_v1(file_id: int):
    return storage_socket.external_files.get_metadata(file_id)


@api_v1.route("/external_files/<int:file_id>/download", methods=["GET"])
@wrap_route("READ")
def download_external_file_v1(file_id: int):
    _, url = storage_socket.external_files.get_url(file_id)
    return redirect(url, code=302)
