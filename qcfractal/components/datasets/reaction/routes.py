from typing import List

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.datasets.reaction import (
    ReactionDatasetAddBody,
    ReactionDatasetSpecification,
    ReactionDatasetNewEntry,
)


@main.route("/v1/datasets/reaction", methods=["POST"])
@wrap_route("WRITE")
def add_reaction_dataset_v1(body_data: ReactionDatasetAddBody):
    return storage_socket.datasets.reaction.add(
        name=body_data.name,
        description=body_data.description,
        tagline=body_data.tagline,
        tags=body_data.tags,
        group=body_data.group,
        provenance=body_data.provenance,
        visibility=body_data.visibility,
        default_tag=body_data.default_tag,
        default_priority=body_data.default_priority,
    )


@main.route("/v1/datasets/reaction/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_reaction_dataset_specifications_v1(dataset_id: int, *, body_data: List[ReactionDatasetSpecification]):
    return storage_socket.datasets.reaction.add_specifications(dataset_id, body_data)


@main.route("/v1/datasets/reaction/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_reaction_dataset_entries_v1(dataset_id: int, *, body_data: List[ReactionDatasetNewEntry]):
    return storage_socket.datasets.reaction.add_entries(
        dataset_id,
        new_entries=body_data,
    )
