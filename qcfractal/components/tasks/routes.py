from flask import current_app

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.tasks import (
    TaskClaimBody,
    TaskReturnBody,
)
from qcportal.utils import calculate_limit

# WRITE action is not a mistake. Claim does some changes to the DB, so require
# a bit more than read
@main.route("/v1/tasks/claim", methods=["POST"])
@wrap_route(TaskClaimBody, None, "WRITE")
def claim_tasks_v1(body_data: TaskClaimBody):
    """Claims tasks from the task queue"""

    # check here, but also in the socket
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.manager_tasks_claim

    return storage_socket.tasks.claim_tasks(
        manager_name=body_data.name_data.fullname, limit=calculate_limit(max_limit, body_data.limit)
    )


@main.route("/v1/tasks/return", methods=["POST"])
@wrap_route(TaskReturnBody, None, "WRITE")
def return_tasks_v1(body_data: TaskReturnBody):
    """Return finished tasks"""

    return storage_socket.tasks.update_finished(manager_name=body_data.name_data.fullname, results=body_data.results)
