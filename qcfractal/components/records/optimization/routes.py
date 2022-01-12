from typing import Optional

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper, prefix_projection
from qcfractal.app.routes import check_access, wrap_route
from qcportal.base_models import CommonGetProjURLParameters
from qcportal.records.optimization import OptimizationAddBody, OptimizationQueryBody


@main.route("/v1/record/optimization", methods=["POST"])
@wrap_route(OptimizationAddBody, None)
@check_access
def add_optimization_records_v1(body_data: OptimizationAddBody):
    return storage_socket.records.optimization.add(
        opt_spec=body_data.specification,
        initial_molecules=body_data.initial_molecules,
        tag=body_data.tag,
        priority=body_data.priority,
    )


@main.route("/v1/record/optimization", methods=["GET"])
@main.route("/v1/record/optimization/<int:record_id>", methods=["GET"])
@wrap_route(None, CommonGetProjURLParameters)
@check_access
def get_optimization_records_v1(record_id: Optional[int] = None, *, url_params: CommonGetProjURLParameters):
    return get_helper(
        record_id,
        url_params.id,
        url_params.include,
        None,
        url_params.missing_ok,
        storage_socket.records.optimization.get,
    )


@main.route("/v1/record/optimization/<int:record_id>/trajectory", methods=["GET"])
@wrap_route(None, CommonGetProjURLParameters)
@check_access
def get_optimization_trajectory_v1(record_id: int, *, url_params: CommonGetProjURLParameters):
    # adjust the includes/excludes to refer to the trajectory
    ch_includes, ch_excludes = prefix_projection(url_params, "trajectory")
    rec = storage_socket.records.optimization.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["trajectory"]


@main.route("/v1/record/optimization/query", methods=["POST"])
@wrap_route(OptimizationQueryBody, None)
@check_access
def query_optimization_v1(body_data: OptimizationQueryBody):
    return storage_socket.records.optimization.query(body_data)
