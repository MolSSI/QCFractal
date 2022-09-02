from typing import List

from qcfractal.api_v1.blueprint import api_v1
from qcfractal.api_v1.helpers import wrap_route
from qcfractal.flask_app import storage_socket
from qcportal.neb import (
    NEBDatasetSpecification,
    NEBDatasetNewEntry,
)


@api_v1.route("/datasets/neb/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_neb_dataset_specifications_v1(dataset_id: int, body_data: List[NEBDatasetSpecification]):
    return storage_socket.datasets.neb.add_specifications(dataset_id, body_data)


@api_v1.route("/datasets/neb/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_neb_dataset_entries_v1(dataset_id: int, body_data: List[NEBDatasetNewEntry]):
    return storage_socket.datasets.neb.add_entries(
        dataset_id,
        new_entries=body_data,
    )
