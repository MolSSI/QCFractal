from typing import List

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.datasets.gridoptimization import (
    GridoptimizationDatasetAddBody,
    GridoptimizationDatasetSpecification,
    GridoptimizationDatasetNewEntry,
)


@main.route("/v1/datasets/gridoptimization", methods=["POST"])
@wrap_route(GridoptimizationDatasetAddBody, None, "WRITE")
def add_gridoptimization_dataset_v1(body_data: GridoptimizationDatasetAddBody):
    return storage_socket.datasets.gridoptimization.add(
        name=body_data.name,
        description=body_data.description,
        tagline=body_data.tagline,
        tags=body_data.tags,
        group=body_data.group,
        provenance=body_data.provenance,
        visibility=body_data.visibility,
        default_tag=body_data.default_tag,
        default_priority=body_data.default_priority,
    )


@main.route("/v1/datasets/gridoptimization/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route(List[GridoptimizationDatasetSpecification], None, "WRITE")
def add_gridoptimization_dataset_specifications_v1(
    dataset_id: int, *, body_data: List[GridoptimizationDatasetSpecification]
):
    return storage_socket.datasets.gridoptimization.add_specifications(dataset_id, body_data)


@main.route("/v1/datasets/gridoptimization/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route(List[GridoptimizationDatasetNewEntry], None, "WRITE")
def add_gridoptimization_dataset_entries_v1(dataset_id: int, *, body_data: List[GridoptimizationDatasetNewEntry]):
    return storage_socket.datasets.gridoptimization.add_entries(
        dataset_id,
        new_entries=body_data,
    )
