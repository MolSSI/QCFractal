from typing import List

from flask import current_app, g

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.decorators import check_permissions, serialization
from qcportal.exceptions import LimitExceededError
from qcportal.reaction import (
    ReactionDatasetSpecification,
    ReactionDatasetNewEntry,
    ReactionAddBody,
    ReactionQueryFilters,
)
from qcportal.utils import calculate_limit


#####################
# Record
#####################


@api_v1.route("/records/reaction/bulkCreate", methods=["POST"])
@check_permissions("records", "add")
@serialization()
def add_reaction_records_v1(body_data: ReactionAddBody):
    limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.add_records
    if len(body_data.stoichiometries) > limit:
        raise LimitExceededError(f"Cannot add {len(body_data.stoichiometries)} reaction records - limit is {limit}")

    return storage_socket.records.reaction.add(
        stoichiometries=body_data.stoichiometries,
        rxn_spec=body_data.specification,
        compute_tag=body_data.compute_tag,
        compute_priority=body_data.compute_priority,
        creator_user=g.username,
        find_existing=body_data.find_existing,
    )


@api_v1.route("/records/reaction/<int:record_id>/components", methods=["GET"])
@check_permissions("records", "read")
@serialization()
def get_reaction_components_v1(record_id: int):
    return storage_socket.records.reaction.get_components(record_id)


@api_v1.route("/records/reaction/query", methods=["POST"])
@check_permissions("records", "read")
@serialization()
def query_reaction_v1(body_data: ReactionQueryFilters):
    max_limit = current_app.config["QCFRACTAL_CONFIG"].api_limits.get_records
    body_data.limit = calculate_limit(max_limit, body_data.limit)

    return storage_socket.records.reaction.query(body_data)


#####################
# Dataset
#####################


@api_v1.route("/datasets/reaction/<int:dataset_id>/specifications", methods=["POST"])
@check_permissions("datasets", "modify")
@serialization()
def add_reaction_dataset_specifications_v1(dataset_id: int, body_data: List[ReactionDatasetSpecification]):
    return storage_socket.datasets.reaction.add_specifications(dataset_id, body_data)


@api_v1.route("/datasets/reaction/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@check_permissions("datasets", "modify")
@serialization()
def add_reaction_dataset_entries_v1(dataset_id: int, body_data: List[ReactionDatasetNewEntry]):
    return storage_socket.datasets.reaction.add_entries(dataset_id, new_entries=body_data)


@api_v1.route("/datasets/reaction/<int:dataset_id>/background_add_entries", methods=["POST"])
@check_permissions("datasets", "modify")
@serialization()
def background_add_reaction_dataset_entries_v1(dataset_id: int, body_data: List[ReactionDatasetNewEntry]):
    return storage_socket.datasets.reaction.background_add_entries(dataset_id, new_entries=body_data)
