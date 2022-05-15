from typing import List

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.datasets.manybody import (
    ManybodyDatasetAddBody,
    ManybodyDatasetSpecification,
    ManybodyDatasetNewEntry,
)


@main.route("/v1/datasets/manybody", methods=["POST"])
@wrap_route("WRITE")
def add_manybody_dataset_v1(body_data: ManybodyDatasetAddBody):
    return storage_socket.datasets.manybody.add(
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


@main.route("/v1/datasets/manybody/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_manybody_dataset_specifications_v1(dataset_id: int, body_data: List[ManybodyDatasetSpecification]):
    return storage_socket.datasets.manybody.add_specifications(dataset_id, body_data)


@main.route("/v1/datasets/manybody/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_manybody_dataset_entries_v1(dataset_id: int, body_data: List[ManybodyDatasetNewEntry]):
    return storage_socket.datasets.manybody.add_entries(
        dataset_id,
        new_entries=body_data,
    )
