from typing import List

from qcfractal.api import wrap_route
from qcfractal.flask_app import api, storage_socket
from qcportal.neb import (
    NEBDatasetSpecification,
    NEBDatasetNewEntry,
)


@api.route("/v1/datasets/neb/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_neb_dataset_specifications_v1(dataset_id: int, body_data: List[NEBDatasetSpecification]):
    return storage_socket.datasets.neb.add_specifications(dataset_id, body_data)


@api.route("/v1/datasets/neb/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_neb_dataset_entries_v1(dataset_id: int, body_data: List[NEBDatasetNewEntry]):
    return storage_socket.datasets.neb.add_entries(
        dataset_id,
        new_entries=body_data,
    )
