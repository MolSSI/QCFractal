from typing import List

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.datasets.optimization import (
    OptimizationDatasetSpecification,
    OptimizationDatasetNewEntry,
)


@main.route("/v1/datasets/optimization/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_optimization_dataset_specifications_v1(dataset_id: int, body_data: List[OptimizationDatasetSpecification]):
    return storage_socket.datasets.optimization.add_specifications(dataset_id, body_data)


@main.route("/v1/datasets/optimization/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_optimization_dataset_entries_v1(dataset_id: int, body_data: List[OptimizationDatasetNewEntry]):
    return storage_socket.datasets.optimization.add_entries(
        dataset_id,
        new_entries=body_data,
    )
