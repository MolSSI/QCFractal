from typing import List

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.datasets.torsiondrive import (
    TorsiondriveDatasetAddBody,
    TorsiondriveDatasetSpecification,
    TorsiondriveDatasetNewEntry,
)


@main.route("/v1/datasets/torsiondrive", methods=["POST"])
@wrap_route("WRITE")
def add_torsiondrive_dataset_v1(body_data: TorsiondriveDatasetAddBody):
    return storage_socket.datasets.torsiondrive.add(
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


@main.route("/v1/datasets/torsiondrive/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_torsiondrive_dataset_specifications_v1(dataset_id: int, *, body_data: List[TorsiondriveDatasetSpecification]):
    return storage_socket.datasets.torsiondrive.add_specifications(dataset_id, body_data)


@main.route("/v1/datasets/torsiondrive/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_torsiondrive_dataset_entries_v1(dataset_id: int, *, body_data: List[TorsiondriveDatasetNewEntry]):
    return storage_socket.datasets.torsiondrive.add_entries(
        dataset_id,
        new_entries=body_data,
    )
