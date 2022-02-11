from flask import current_app

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import prefix_projection
from qcfractal.app.routes import wrap_route
from qcportal.base_models import ProjURLParameters
from qcportal.exceptions import LimitExceededError
from qcportal.records.optimization import OptimizationAddBody, OptimizationQueryBody
from qcportal.utils import calculate_limit


@main.route("/v1/records/optimization/bulkCreate", methods=["POST"])
@wrap_route(OptimizationAddBody, None)
def add_optimization_records_v1(body_data: OptimizationAddBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_records
    if len(body_data.initial_molecules) > limit:
        raise LimitExceededError(
            f"Cannot add {len(body_data.initial_molecules)} optimization records - limit is {limit}"
        )

    return storage_socket.records.optimization.add(
        initial_molecules=body_data.initial_molecules,
        opt_spec=body_data.specification,
        tag=body_data.tag,
        priority=body_data.priority,
    )


@main.route("/v1/records/optimization/<int:record_id>/trajectory", methods=["GET"])
@wrap_route(None, ProjURLParameters)
def get_optimization_trajectory_v1(record_id: int, *, url_params: ProjURLParameters):
    # adjust the includes/excludes to refer to the trajectory
    ch_includes, ch_excludes = prefix_projection(url_params, "trajectory")
    rec = storage_socket.records.optimization.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["trajectory"]


@main.route("/v1/records/optimization/query", methods=["POST"])
@wrap_route(OptimizationQueryBody, None)
def query_optimization_v1(body_data: OptimizationQueryBody):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.records.optimization.query(body_data)
