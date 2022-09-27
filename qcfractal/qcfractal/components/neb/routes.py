from typing import List

from flask import current_app

from qcfractal.api_v1.blueprint import api_v1
from qcfractal.api_v1.helpers import wrap_route
from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.helpers import prefix_projection
from qcportal.base_models import ProjURLParameters
from qcportal.exceptions import LimitExceededError
from qcportal.neb import NEBDatasetSpecification, NEBDatasetNewEntry, NEBAddBody, NEBQueryFilters
from qcportal.utils import calculate_limit


#####################
# Record
#####################


@api_v1.route("/records/neb/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_neb_records_v1(body_data: NEBAddBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_records
    if len(body_data.initial_chains) > limit:
        raise LimitExceededError(f"Cannot add {len(body_data.initial_chains)} neb records - limit is {limit}")

    return storage_socket.records.neb.add(
        initial_chains=body_data.initial_chains,
        neb_spec=body_data.specification,
        tag=body_data.tag,
        priority=body_data.priority,
        owner_user=g.username,
        owner_group=body_data.owner_group,
    )


@api_v1.route("/records/neb/<int:record_id>/singlepoints", methods=["GET"])
@wrap_route("READ")
def get_neb_singlepoints_v1(record_id: int, url_params: ProjURLParameters):
    # adjust the includes/excludes to refer to the singlepoints
    ch_includes, ch_excludes = prefix_projection(url_params, "singlepoints")
    rec = storage_socket.records.neb.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["singlepoints"]


@api_v1.route("/records/neb/<int:record_id>/neb_result", methods=["GET"])
@wrap_route("READ")
def get_neb_result_v1(record_id: int, url_params: ProjURLParameters):
    return storage_socket.records.neb.get_neb_result(record_id, url_params.include, url_params.exclude)


@api_v1.route("/records/neb/<int:record_id>/optimizations", methods=["GET"])
@wrap_route("READ")
def get_neb_optimizations_v1(record_id: int, url_params: ProjURLParameters):
    # adjust the includes/excludes to refer to the optimizations
    ch_includes, ch_excludes = prefix_projection(url_params, "optimizations")
    rec = storage_socket.records.neb.get([record_id], include=ch_includes, exclude=ch_excludes)
    return rec[0]["optimizations"]


@api_v1.route("/records/neb/<int:record_id>/initial_chain", methods=["GET"])
@wrap_route("READ")
def get_neb_initial_chain_v1(record_id: int):
    rec = storage_socket.records.neb.get([record_id], include=["initial_chain"])
    return rec[0]["initial_chain"]


@api_v1.route("/records/neb/query", methods=["POST"])
@wrap_route("READ")
def query_neb_v1(body_data: NEBQueryFilters):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.records.neb.query(body_data)


#####################
# Dataset
#####################


@api_v1.route("/datasets/neb/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_neb_dataset_specifications_v1(dataset_id: int, body_data: List[NEBDatasetSpecification]):
    return storage_socket.datasets.neb.add_specifications(dataset_id, body_data)


@api_v1.route("/datasets/neb/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_neb_dataset_entries_v1(dataset_id: int, body_data: List[NEBDatasetNewEntry]):
    return storage_socket.datasets.neb.add_entries(
        dataset_id,
        new_entries=body_data,
    )
