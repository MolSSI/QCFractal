from typing import List

from qcfractal.api import wrap_route
from qcfractal.flask_app import api, storage_socket
from qcportal.reaction import (
    ReactionDatasetSpecification,
    ReactionDatasetNewEntry,
)


@api.route("/v1/datasets/reaction/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_reaction_dataset_specifications_v1(dataset_id: int, *, body_data: List[ReactionDatasetSpecification]):
    return storage_socket.datasets.reaction.add_specifications(dataset_id, body_data)


@api.route("/v1/datasets/reaction/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_reaction_dataset_entries_v1(dataset_id: int, *, body_data: List[ReactionDatasetNewEntry]):
    return storage_socket.datasets.reaction.add_entries(
        dataset_id,
        new_entries=body_data,
    )
