from flask import current_app

from qcfractal.app import main, storage_socket
from qcfractal.app.helpers import prefix_projection
from qcfractal.app.routes import wrap_route
from qcportal.base_models import ProjURLParameters
from qcportal.exceptions import LimitExceededError
from qcportal.records.reaction import ReactionAddBody, ReactionQueryBody
from qcportal.utils import calculate_limit


@main.route("/v1/records/reaction/bulkCreate", methods=["POST"])
@wrap_route(ReactionAddBody, None, "WRITE")
def add_reaction_records_v1(body_data: ReactionAddBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_records
    if len(body_data.stoichiometries) > limit:
        raise LimitExceededError(f"Cannot add {len(body_data.stoichiometries)} reaction records - limit is {limit}")

    return storage_socket.records.reaction.add(
        stoichiometries=body_data.stoichiometries,
        qc_spec=body_data.specification,
        tag=body_data.tag,
        priority=body_data.priority,
    )


@main.route("/v1/records/reaction/<int:record_id>/components", methods=["GET"])
@wrap_route(None, ProjURLParameters, "READ")
def get_reaction_components_v1(record_id: int, *, url_params: ProjURLParameters):
    # adjust the includes/excludes to refer to the components
    ch_includes, ch_excludes = prefix_projection(url_params, "components")
    rec = storage_socket.records.reaction.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["components"]


@main.route("/v1/records/reaction/<int:record_id>/stoichiometries", methods=["GET"])
@wrap_route(None, ProjURLParameters, "READ")
def get_reaction_molecules_v1(record_id: int, *, url_params: ProjURLParameters):
    # adjust the includes/excludes to refer to the molecules
    ch_includes, ch_excludes = prefix_projection(url_params, "stoichiometries")
    rec = storage_socket.records.reaction.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["stoichiometries"]


@main.route("/v1/records/reaction/query", methods=["POST"])
@wrap_route(ReactionQueryBody, None, "READ")
def query_reaction_v1(body_data: ReactionQueryBody):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.records.reaction.query(body_data)
