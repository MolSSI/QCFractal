from typing import Optional

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import get_helper
from qcfractal.app.routes import check_access, wrap_route
from qcfractal.portal.base_models import CommonGetProjURLParameters
from qcfractal.portal.records.torsiondrive import TorsiondriveAddBody, TorsiondriveQueryBody


@main.route("/v1/record/torsiondrive", methods=["POST"])
@wrap_route(TorsiondriveAddBody, None)
@check_access
def add_torsiondrive_records_v1(body_data: TorsiondriveAddBody):
    return storage_socket.records.torsiondrive.add(
        td_spec=body_data.specification,
        initial_molecules=body_data.initial_molecules,
        as_service=body_data.as_service,
        tag=body_data.tag,
        priority=body_data.priority,
    )


@main.route("/v1/record/torsiondrive", methods=["GET"])
@main.route("/v1/record/torsiondrive/<int:record_id>", methods=["GET"])
@wrap_route(None, CommonGetProjURLParameters)
@check_access
def get_torsiondrive_records_v1(record_id: Optional[int] = None, *, url_params: CommonGetProjURLParameters):
    return get_helper(
        record_id,
        url_params.id,
        url_params.include,
        None,
        url_params.missing_ok,
        storage_socket.records.torsiondrive.get,
    )


@main.route("/v1/record/torsiondrive/<int:record_id>/optimization_history", methods=["GET"])
@wrap_route(None, CommonGetProjURLParameters)
@check_access
def get_torsiondrive_optimization_history_v1(record_id: int, *, url_params: CommonGetProjURLParameters):
    return storage_socket.records.torsiondrive.get_optimization_history(
        record_id, url_params.include, url_params.exclude, url_params.missing_ok
    )


@main.route("/v1/record/torsiondrive/query", methods=["POST"])
@wrap_route(TorsiondriveQueryBody, None)
@check_access
def query_torsiondrive_v1(body_data: TorsiondriveQueryBody):
    return storage_socket.records.torsiondrive.query(body_data)
