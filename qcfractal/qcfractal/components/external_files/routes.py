from flask import redirect, Response, stream_with_context, current_app

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.wrap_route import wrap_global_route


@api_v1.route("/external_files/<int:file_id>", methods=["GET"])
@wrap_global_route("records", "read")
def get_external_file_metadata_v1(file_id: int):
    return storage_socket.external_files.get_metadata(file_id)


@api_v1.route("/external_files/<int:file_id>/download", methods=["GET"])
@wrap_global_route("records", "read")
def download_external_file_v1(file_id: int):
    passthrough = current_app.config["QCFRACTAL_CONFIG"].s3.passthrough

    if passthrough:
        file_name, streamer_func = storage_socket.external_files.get_file_streamer(file_id)

        return Response(
            stream_with_context(streamer_func()),
            content_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{file_name}"'},
        )
    else:
        _, url = storage_socket.external_files.get_url(file_id)
        return redirect(url, code=302)


@api_v1.route("/external_files/<int:file_id>/direct_link", methods=["GET"])
@wrap_global_route("records", "read")
def get_direct_link_external_file_v1(file_id: int):
    passthrough = current_app.config["QCFRACTAL_CONFIG"].s3.passthrough

    if passthrough:
        # Will check for missing file id
        meta = storage_socket.external_files.get_metadata(file_id)
        return f"/api/v1/external_files/{meta['id']}/download"
    else:
        _, url = storage_socket.external_files.get_url(file_id)
        return url
