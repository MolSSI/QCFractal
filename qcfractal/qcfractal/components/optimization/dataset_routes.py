from typing import List

from qcfractal.api_v1.blueprint import api_v1
from qcfractal.api_v1.helpers import wrap_route
from qcfractal.flask_app import storage_socket
from qcportal.optimization import (
    OptimizationDatasetSpecification,
    OptimizationDatasetNewEntry,
)


@api_v1.route("/datasets/optimization/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_optimization_dataset_specifications_v1(dataset_id: int, body_data: List[OptimizationDatasetSpecification]):
    return storage_socket.datasets.optimization.add_specifications(dataset_id, body_data)


@api_v1.route("/datasets/optimization/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_optimization_dataset_entries_v1(dataset_id: int, body_data: List[OptimizationDatasetNewEntry]):
    return storage_socket.datasets.optimization.add_entries(
        dataset_id,
        new_entries=body_data,
    )
