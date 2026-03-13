from flask import current_app

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.decorators import check_permissions, serialization
from qcportal.exceptions import InvalidUpdateError
from qcportal.internal_jobs import InternalJobQueryFilters, InternalJobStatusEnum
from qcportal.utils import calculate_limit


@api_v1.route("/internal_jobs/<int:job_id>", methods=["GET"])
@check_permissions("internal_jobs", "read")
@serialization()
def get_internal_job_v1(job_id: int):
    return storage_socket.internal_jobs.get(job_id=job_id)


@api_v1.route("/internal_jobs/<int:job_id>", methods=["DELETE"])
@check_permissions("internal_jobs", "delete")
@serialization()
def delete_internal_job_v1(job_id: int):
    return storage_socket.internal_jobs.delete(job_id=job_id)


@api_v1.route("/internal_jobs/<int:job_id>/status", methods=["PUT"])
@check_permissions("internal_jobs", "modify")
@serialization()
def cancel_internal_job_v1(job_id: int, body_data: InternalJobStatusEnum):
    if body_data == InternalJobStatusEnum.cancelled:
        return storage_socket.internal_jobs.cancel(job_id=job_id)
    else:
        raise InvalidUpdateError(f"Invalid status for updating internal job: {body_data}")


@api_v1.route("/internal_jobs/query", methods=["POST"])
@check_permissions("internal_jobs", "read")
@serialization()
def query_internal_jobs_v1(body_data: InternalJobQueryFilters):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_internal_jobs
    body_data.limit = calculate_limit(max_limit, body_data.limit)
    return storage_socket.internal_jobs.query(body_data)
