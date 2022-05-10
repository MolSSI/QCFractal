from typing import List

from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.datasets.neb import (
    NEBDatasetAddBody,
    NEBDatasetSpecification,
    NEBDatasetNewEntry,
)


@main.route("/v1/datasets/neb", methods=["POST"])
@wrap_route(NEBDatasetAddBody, None, "WRITE")
def add_neb_dataset_v1(body_data: NEBDatasetAddBody):
    return storage_socket.datasets.neb.add(
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


@main.route("/v1/datasets/neb/<int:dataset_id>/specifications", methods=["POST"])
@wrap_route(List[NEBDatasetSpecification], None, "WRITE")
def add_neb_dataset_specifications_v1(dataset_id: int, *, body_data: List[NEBDatasetSpecification]):
    return storage_socket.datasets.neb.add_specifications(dataset_id, body_data)


@main.route("/v1/datasets/neb/<int:dataset_id>/entries/bulkCreate", methods=["POST"])
@wrap_route(List[NEBDatasetNewEntry], None, "WRITE")
def add_neb_dataset_entries_v1(dataset_id: int, *, body_data: List[NEBDatasetNewEntry]):
    return storage_socket.datasets.neb.add_entries(
        dataset_id,
        new_entries=body_data,
    )
