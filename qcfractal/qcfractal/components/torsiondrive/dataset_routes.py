from typing import List

from qcfractal.api_v1.blueprint import api_v1
from qcfractal.api_v1.helpers import wrap_route
from qcfractal.flask_app import storage_socket
from qcportal.torsiondrive import (
    TorsiondriveDatasetSpecification,
    TorsiondriveDatasetNewEntry,
)


@api_v1.route("/datasets/torsiondrive/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_torsiondrive_dataset_specifications_v1(dataset_id: int, body_data: List[TorsiondriveDatasetSpecification]):
    return storage_socket.datasets.torsiondrive.add_specifications(dataset_id, body_data)


@api_v1.route("/datasets/torsiondrive/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_torsiondrive_dataset_entries_v1(dataset_id: int, body_data: List[TorsiondriveDatasetNewEntry]):
    return storage_socket.datasets.torsiondrive.add_entries(
        dataset_id,
        new_entries=body_data,
    )
