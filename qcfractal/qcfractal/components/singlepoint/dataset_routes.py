from typing import List

from qcfractal.api import wrap_route
from qcfractal.flask_app import api, storage_socket
from qcportal.singlepoint import (
    SinglepointDatasetSpecification,
    SinglepointDatasetNewEntry,
)


@api.route("/v1/datasets/singlepoint/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_singlepoint_dataset_specifications_v1(dataset_id: int, body_data: List[SinglepointDatasetSpecification]):
    return storage_socket.datasets.singlepoint.add_specifications(dataset_id, body_data)


@api.route("/v1/datasets/singlepoint/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_singlepoint_dataset_entries_v1(dataset_id: int, body_data: List[SinglepointDatasetNewEntry]):
    return storage_socket.datasets.singlepoint.add_entries(
        dataset_id,
        new_entries=body_data,
    )
