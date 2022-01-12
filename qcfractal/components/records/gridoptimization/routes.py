from typing import Optional

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper, prefix_projection
from qcfractal.app.routes import check_access, wrap_route
from qcportal.base_models import CommonGetProjURLParameters
from qcportal.records.gridoptimization import GridoptimizationAddBody, GridoptimizationQueryBody


@main.route("/v1/record/gridoptimization", methods=["POST"])
@wrap_route(GridoptimizationAddBody, None)
@check_access
def add_gridoptimization_records_v1(body_data: GridoptimizationAddBody):
    return storage_socket.records.gridoptimization.add(
        go_spec=body_data.specification,
        initial_molecules=body_data.initial_molecules,
        tag=body_data.tag,
        priority=body_data.priority,
    )


@main.route("/v1/record/gridoptimization", methods=["GET"])
@main.route("/v1/record/gridoptimization/<int:record_id>", methods=["GET"])
@wrap_route(None, CommonGetProjURLParameters)
@check_access
def get_gridoptimization_records_v1(record_id: Optional[int] = None, *, url_params: CommonGetProjURLParameters):
    return get_helper(
        record_id,
        url_params.id,
        url_params.include,
        None,
        url_params.missing_ok,
        storage_socket.records.gridoptimization.get,
    )


@main.route("/v1/record/gridoptimization/<int:record_id>/optimizations", methods=["GET"])
@wrap_route(None, CommonGetProjURLParameters)
@check_access
def get_gridoptimization_optimizations_v1(record_id: int, *, url_params: CommonGetProjURLParameters):
    # adjust the includes/excludes to refer to the optimizations
    ch_includes, ch_excludes = prefix_projection(url_params, "optimizations")
    rec = storage_socket.records.gridoptimization.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["optimizations"]


@main.route("/v1/record/gridoptimization/query", methods=["POST"])
@wrap_route(GridoptimizationQueryBody, None)
@check_access
def query_gridoptimization_v1(body_data: GridoptimizationQueryBody):
    return storage_socket.records.gridoptimization.query(body_data)
