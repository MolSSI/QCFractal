from typing import List

from qcfractal.api import wrap_route
from qcfractal.flask_app import api, storage_socket
from qcportal.manybody import (
    ManybodyDatasetSpecification,
    ManybodyDatasetNewEntry,
)


@api.route("/v1/datasets/manybody/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_manybody_dataset_specifications_v1(dataset_id: int, body_data: List[ManybodyDatasetSpecification]):
    return storage_socket.datasets.manybody.add_specifications(dataset_id, body_data)


@api.route("/v1/datasets/manybody/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_manybody_dataset_entries_v1(dataset_id: int, body_data: List[ManybodyDatasetNewEntry]):
    return storage_socket.datasets.manybody.add_entries(
        dataset_id,
        new_entries=body_data,
    )
