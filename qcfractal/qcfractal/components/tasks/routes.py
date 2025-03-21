from flask import current_app

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.helpers import wrap_route  # uses the same wrap_route as the user api
from qcfractal.flask_app.compute_v1.blueprint import compute_v1
from qcportal.exceptions import LimitExceededError
from qcportal.tasks import TaskClaimBody, TaskReturnBody
from qcportal.utils import calculate_limit


# WRITE action is not a mistake. Claim does some changes to the DB, so require
# a bit more than read
@compute_v1.route("/tasks/claim", methods=["POST"])
@wrap_route("WRITE")
def claim_tasks_v1(body_data: TaskClaimBody):
    """Claims tasks from the task queue"""

    # check here, but also in the socket
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.manager_tasks_claim

    return storage_socket.tasks.claim_tasks(
        manager_name=body_data.name_data.fullname,
        compute_tags=body_data.compute_tags,
        programs=body_data.programs,
        limit=calculate_limit(max_limit, body_data.limit),
    )


@compute_v1.route("/tasks/return", methods=["POST"])
@wrap_route("WRITE")
def return_tasks_v1(body_data: TaskReturnBody):
    """Return finished tasks"""

    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.manager_tasks_claim
    if len(body_data.results_compressed) > max_limit:
        raise LimitExceededError(f"Attempted to return too many results - limit is {max_limit}")

    return storage_socket.tasks.update_finished(
        manager_name=body_data.name_data.fullname, results_compressed=body_data.results_compressed
    )
