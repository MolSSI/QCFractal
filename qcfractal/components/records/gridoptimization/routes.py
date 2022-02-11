from flask import current_app

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import prefix_projection
from qcfractal.app.routes import wrap_route
from qcportal.base_models import ProjURLParameters
from qcportal.exceptions import LimitExceededError
from qcportal.records.gridoptimization import GridoptimizationAddBody, GridoptimizationQueryBody
from qcportal.utils import calculate_limit


@main.route("/v1/records/gridoptimization/bulkCreate", methods=["POST"])
@wrap_route(GridoptimizationAddBody, None, "WRITE")
def add_gridoptimization_records_v1(body_data: GridoptimizationAddBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_records
    if len(body_data.initial_molecules) > limit:
        raise LimitExceededError(
            f"Cannot add {len(body_data.initial_molecules)} gridoptimization records - limit is {limit}"
        )

    return storage_socket.records.gridoptimization.add(
        initial_molecules=body_data.initial_molecules,
        go_spec=body_data.specification,
        tag=body_data.tag,
        priority=body_data.priority,
    )


@main.route("/v1/records/gridoptimization/<int:record_id>/optimizations", methods=["GET"])
@wrap_route(None, ProjURLParameters, "READ")
def get_gridoptimization_optimizations_v1(record_id: int, *, url_params: ProjURLParameters):
    # adjust the includes/excludes to refer to the optimizations
    ch_includes, ch_excludes = prefix_projection(url_params, "optimizations")
    rec = storage_socket.records.gridoptimization.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["optimizations"]


@main.route("/v1/records/gridoptimization/query", methods=["POST"])
@wrap_route(GridoptimizationQueryBody, None, "READ")
def query_gridoptimization_v1(body_data: GridoptimizationQueryBody):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.records.gridoptimization.query(body_data)
