from flask import current_app

from qcfractal.api_v1.blueprint import api_v1
from qcfractal.api_v1.helpers import wrap_route
from qcfractal.flask_app import prefix_projection, storage_socket
from qcportal.base_models import ProjURLParameters
from qcportal.exceptions import LimitExceededError
from qcportal.manybody import ManybodyAddBody, ManybodyQueryFilters
from qcportal.utils import calculate_limit


@api_v1.route("/records/manybody/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_manybody_records_v1(body_data: ManybodyAddBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_records
    if len(body_data.initial_molecules) > limit:
        raise LimitExceededError(f"Cannot add {len(body_data.initial_molecules)} manybody records - limit is {limit}")

    return storage_socket.records.manybody.add(
        initial_molecules=body_data.initial_molecules,
        mb_spec=body_data.specification,
        tag=body_data.tag,
        priority=body_data.priority,
    )


@api_v1.route("/records/manybody/<int:record_id>/clusters", methods=["GET"])
@wrap_route("READ")
def get_manybody_components_v1(record_id: int, url_params: ProjURLParameters):
    # adjust the includes/excludes to refer to the components
    ch_includes, ch_excludes = prefix_projection(url_params, "clusters")
    rec = storage_socket.records.manybody.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["clusters"]


@api_v1.route("/records/manybody/<int:record_id>/initial_molecules", methods=["GET"])
@wrap_route("READ")
def get_manybody_molecules_v1(record_id: int, url_params: ProjURLParameters):
    # adjust the includes/excludes to refer to the molecules
    ch_includes, ch_excludes = prefix_projection(url_params, "initial_molecules")
    rec = storage_socket.records.manybody.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["initial_molecules"]


@api_v1.route("/records/manybody/query", methods=["POST"])
@wrap_route("READ")
def query_manybody_v1(body_data: ManybodyQueryFilters):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.records.manybody.query(body_data)
