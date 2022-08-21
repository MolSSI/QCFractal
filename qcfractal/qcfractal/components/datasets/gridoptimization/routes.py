from typing import List

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.datasets.gridoptimization import (
    GridoptimizationDatasetSpecification,
    GridoptimizationDatasetNewEntry,
)


@main.route("/v1/datasets/gridoptimization/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route("WRITE")
def add_gridoptimization_dataset_specifications_v1(
    dataset_id: int, body_data: List[GridoptimizationDatasetSpecification]
):
    return storage_socket.datasets.gridoptimization.add_specifications(dataset_id, body_data)


@main.route("/v1/datasets/gridoptimization/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route("WRITE")
def add_gridoptimization_dataset_entries_v1(dataset_id: int, body_data: List[GridoptimizationDatasetNewEntry]):
    return storage_socket.datasets.gridoptimization.add_entries(
        dataset_id,
        new_entries=body_data,
    )
