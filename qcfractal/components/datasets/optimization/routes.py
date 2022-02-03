from typing import List

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.datasets.optimization import (
    OptimizationDatasetAddBody,
    OptimizationDatasetSubmitBody,
    OptimizationDatasetInputSpecification,
    OptimizationDatasetNewEntry,
)


@main.route("/v1/dataset/optimization", methods=["POST"])
@wrap_route(OptimizationDatasetAddBody, None)
def add_optimization_dataset_v1(body_data: OptimizationDatasetAddBody):
    return storage_socket.datasets.optimization.add(
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


@main.route("/v1/dataset/optimization/<int:dataset_id>/specification", methods=["POST"])
@wrap_route(List[OptimizationDatasetInputSpecification], None)
def add_optimization_dataset_specifications_v1(
    dataset_id: int, *, body_data: List[OptimizationDatasetInputSpecification]
):
    return storage_socket.datasets.optimization.add_specifications(dataset_id, body_data)


@main.route("/v1/dataset/optimization/<int:dataset_id>/entry", methods=["POST"])
@wrap_route(List[OptimizationDatasetNewEntry], None)
def add_optimization_dataset_entries_v1(dataset_id: int, *, body_data: List[OptimizationDatasetNewEntry]):
    return storage_socket.datasets.optimization.add_entries(
        dataset_id,
        new_entries=body_data,
    )


@main.route("/v1/dataset/optimization/<int:dataset_id>/submit", methods=["POST"])
@wrap_route(OptimizationDatasetSubmitBody, None)
def submit_optimization_dataset_v1(dataset_id: int, *, body_data: OptimizationDatasetSubmitBody):
    return storage_socket.datasets.optimization.submit(
        dataset_id,
        specification_names=body_data.specification_name,
        entry_names=body_data.entry_name,
        tag=body_data.tag,
        priority=body_data.priority,
    )
