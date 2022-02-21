from typing import List

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.datasets.singlepoint import (
    SinglepointDatasetAddBody,
    SinglepointDatasetInputSpecification,
    SinglepointDatasetNewEntry,
)


@main.route("/v1/datasets/singlepoint", methods=["POST"])
@wrap_route(SinglepointDatasetAddBody, None, "WRITE")
def add_singlepoint_dataset_v1(body_data: SinglepointDatasetAddBody):
    return storage_socket.datasets.singlepoint.add(
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


@main.route("/v1/datasets/singlepoint/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route(List[SinglepointDatasetInputSpecification], None, "WRITE")
def add_singlepoint_dataset_specifications_v1(
    dataset_id: int, *, body_data: List[SinglepointDatasetInputSpecification]
):
    return storage_socket.datasets.singlepoint.add_specifications(dataset_id, body_data)


@main.route("/v1/datasets/singlepoint/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route(List[SinglepointDatasetNewEntry], None, "WRITE")
def add_singlepoint_dataset_entries_v1(dataset_id: int, *, body_data: List[SinglepointDatasetNewEntry]):
    return storage_socket.datasets.singlepoint.add_entries(
        dataset_id,
        new_entries=body_data,
    )
