from typing import List

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.datasets.neb import (
    NEBDatasetSpecification,
    NEBDatasetNewEntry,
)


@main.route("/v1/datasets/neb/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_neb_dataset_specifications_v1(dataset_id: int, body_data: List[NEBDatasetSpecification]):
    return storage_socket.datasets.neb.add_specifications(dataset_id, body_data)


@main.route("/v1/datasets/neb/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_neb_dataset_entries_v1(dataset_id: int, body_data: List[NEBDatasetNewEntry]):
    return storage_socket.datasets.neb.add_entries(
        dataset_id,
        new_entries=body_data,
    )