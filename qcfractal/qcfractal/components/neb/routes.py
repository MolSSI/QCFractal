from typing import List

from flask import current_app, g

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.api_v1.helpers import wrap_route
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
        compute_tag=body_data.compute_tag,
        compute_priority=body_data.compute_priority,
        owner_user=g.username,
        owner_group=body_data.owner_group,
        find_existing=body_data.find_existing,
    )


@api_v1.route("/records/neb/<int:record_id>/neb_result", methods=["GET"])
@wrap_route("READ")
def get_neb_result_v1(record_id: int):
    return storage_socket.records.neb.get_neb_result(record_id)


@api_v1.route("/records/neb/<int:record_id>/singlepoints", methods=["GET"])
@wrap_route("READ")
def get_neb_singlepoints_v1(record_id: int):
    return storage_socket.records.neb.get_singlepoints(record_id)


@api_v1.route("/records/neb/<int:record_id>/optimizations", methods=["GET"])
@wrap_route("READ")
def get_neb_optimizations_v1(record_id: int):
    return storage_socket.records.neb.get_optimizations(record_id)


@api_v1.route("/records/neb/<int:record_id>/initial_chain", methods=["GET"])
@wrap_route("READ")
def get_neb_initial_chain_molecule_ids_v1(record_id: int):
    return storage_socket.records.neb.get_initial_molecules_ids(record_id)


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
    return storage_socket.datasets.neb.add_entries(dataset_id, new_entries=body_data)


@api_v1.route("/datasets/neb/<int:dataset_id>/background_add_entries", methods=["POST"])
@wrap_route("WRITE")
def background_add_neb_dataset_entries_v1(dataset_id: int, body_data: List[NEBDatasetNewEntry]):
    return storage_socket.datasets.neb.background_add_entries(dataset_id, new_entries=body_data)
