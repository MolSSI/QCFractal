from typing import List

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.datasets.singlepoint import (
    SinglepointDatasetSpecification,
    SinglepointDatasetNewEntry,
)


@main.route("/v1/datasets/singlepoint/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_singlepoint_dataset_specifications_v1(dataset_id: int, body_data: List[SinglepointDatasetSpecification]):
    return storage_socket.datasets.singlepoint.add_specifications(dataset_id, body_data)


@main.route("/v1/datasets/singlepoint/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_singlepoint_dataset_entries_v1(dataset_id: int, body_data: List[SinglepointDatasetNewEntry]):
    return storage_socket.datasets.singlepoint.add_entries(
        dataset_id,
        new_entries=body_data,
    )
