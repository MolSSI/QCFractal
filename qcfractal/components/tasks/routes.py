from flask import current_app

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import check_access, wrap_route
from qcportal.tasks import (
    TaskClaimBody,
    TaskReturnBody,
)
from qcportal.utils import calculate_limit


@main.route("/v1/task/claim", methods=["POST"])
@wrap_route(TaskClaimBody, None)
@check_access
def claim_tasks_v1(body_data: TaskClaimBody):
    """Claims tasks from the task queue"""

    # check here, but also in the socket
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.manager_tasks

    return storage_socket.tasks.claim_tasks(
        manager_name=body_data.name_data.fullname, limit=calculate_limit(max_limit, body_data.limit)
    )


@main.route("/v1/task/return", methods=["POST"])
@wrap_route(TaskReturnBody, None)
@check_access
def return_tasks_v1(body_data: TaskReturnBody):
    """Return finished tasks"""

    return storage_socket.tasks.update_finished(manager_name=body_data.name_data.fullname, results=body_data.results)
