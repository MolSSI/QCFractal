from flask import g

from qcfractal.flask_app import storage_socket
from qcfractal.flask_app.api_v1.blueprint import api_v1
from qcfractal.flask_app.wrap_route import wrap_global_route
from qcportal.base_models import ProjURLParameters
from qcportal.project_models import (
    ProjectAddBody,
    ProjectQueryModel,
    ProjectDeleteParams,
    ProjectDatasetAddBody,
    ProjectRecordAddBody,
    ProjectLinkDatasetBody,
    ProjectUnlinkDatasetsBody,
    ProjectLinkRecordBody,
    ProjectUnlinkRecordsBody,
)


@api_v1.route("/projects", methods=["GET"])
@wrap_global_route("projects", "read")
def list_project_v1():
    return storage_socket.projects.list()


@api_v1.route("/projects/<int:project_id>", methods=["GET"])
@wrap_global_route("projects", "read")
def get_project_v1(project_id: int, url_params: ProjURLParameters):
    return storage_socket.projects.get(project_id, url_params.include, url_params.exclude)


@api_v1.route("/projects/query", methods=["POST"])
@wrap_global_route("projects", "read")
def query_general_project_v1(body_data: ProjectQueryModel):
    with storage_socket.session_scope(True) as session:
        project_id = storage_socket.projects.lookup_id(body_data.project_name, session=session)
        return storage_socket.projects.get(project_id, body_data.include, body_data.exclude, session=session)


@api_v1.route("/projects/<int:project_id>", methods=["DELETE"])
@wrap_global_route("projects", "delete")
def delete_project_v1(project_id: int, url_params: ProjectDeleteParams):
    return storage_socket.projects.delete_project(
        project_id,
        delete_records=url_params.delete_records,
        delete_datasets=url_params.delete_datasets,
        delete_dataset_records=url_params.delete_dataset_records,
    )


########################
# Adding a project
########################
@api_v1.route("/projects", methods=["POST"])
@wrap_global_route("projects", "add")
def add_project_v1(body_data: ProjectAddBody):
    return storage_socket.projects.add(
        name=body_data.name,
        description=body_data.description,
        tagline=body_data.tagline,
        tags=body_data.tags,
        default_compute_tag=body_data.default_compute_tag,
        default_compute_priority=body_data.default_compute_priority,
        extras=body_data.extras,
        owner_user=g.username,
        existing_ok=body_data.existing_ok,
    )


#########################
# Getting info
#########################
@api_v1.route("/projects/<int:project_id>/status", methods=["GET"])
@wrap_global_route("projects", "read")
def get_project_status_v1(project_id: int):
    return storage_socket.projects.status(project_id)


@api_v1.route("/projects/<int:project_id>/record_metadata", methods=["GET"])
@wrap_global_route("projects", "read")
def get_project_record_metadata_v1(project_id: int):
    return storage_socket.projects.get_record_metadata(project_id)


@api_v1.route("/projects/<int:project_id>/dataset_metadata", methods=["GET"])
@wrap_global_route("projects", "read")
def get_project_dataset_metadata_v1(project_id: int):
    return storage_socket.projects.get_dataset_metadata(project_id)


#########################
# Datasets
#########################
@api_v1.route("/projects/<int:project_id>/datasets", methods=["POST"])
@wrap_global_route("projects", "modify")
def add_project_dataset_v1(project_id: int, body_data: ProjectDatasetAddBody):
    return storage_socket.projects.add_dataset(
        project_id=project_id,
        dataset_type=body_data.dataset_type,
        dataset_name=body_data.name,
        description=body_data.description,
        tagline=body_data.tagline,
        tags=body_data.tags,
        provenance=body_data.provenance,
        default_compute_tag=body_data.default_compute_tag,
        default_compute_priority=body_data.default_compute_priority,
        extras=body_data.extras,
        creator_user=g.username,
        existing_ok=body_data.existing_ok,
    )


@api_v1.route("/projects/<int:project_id>/datasets/link", methods=["POST"])
@wrap_global_route("projects", "modify")
def project_link_dataset_v1(project_id: int, body_data: ProjectLinkDatasetBody):
    return storage_socket.projects.link_dataset(
        project_id=project_id,
        dataset_id=body_data.dataset_id,
        name=body_data.name,
        description=body_data.description,
        tagline=body_data.tagline,
        tags=body_data.tags,
    )


@api_v1.route("/projects/<int:project_id>/datasets/unlink", methods=["POST"])
@wrap_global_route("projects", "modify")
def project_unlink_datasets_v1(project_id: int, body_data: ProjectUnlinkDatasetsBody):
    return storage_socket.projects.unlink_datasets(
        project_id=project_id,
        dataset_ids=body_data.dataset_ids,
        delete_datasets=body_data.delete_datasets,
        delete_dataset_records=body_data.delete_dataset_records,
    )


@api_v1.route("/projects/<int:project_id>/datasets/<int:dataset_id>", methods=["GET"])
@wrap_global_route("projects", "read")
def get_project_dataset_v1(project_id: int, dataset_id: int):
    return storage_socket.projects.get_dataset(project_id, dataset_id)


#########################
# Records
#########################
@api_v1.route("/projects/<int:project_id>/records", methods=["POST"])
@wrap_global_route("projects", "modify")
def add_project_record_v1(project_id: int, body_data: ProjectRecordAddBody):
    return storage_socket.projects.add_record(
        project_id,
        name=body_data.name,
        description=body_data.description,
        tags=body_data.tags,
        record_input=body_data.record_input,
        compute_tag=body_data.compute_tag,
        compute_priority=body_data.compute_priority,
        creator_user=g.username,
        find_existing=body_data.find_existing,
    )


@api_v1.route("/projects/<int:project_id>/records/link", methods=["POST"])
@wrap_global_route("projects", "modify")
def project_link_record_v1(project_id: int, body_data: ProjectLinkRecordBody):
    return storage_socket.projects.link_record(
        project_id=project_id,
        record_id=body_data.record_id,
        name=body_data.name,
        description=body_data.description,
        tags=body_data.tags,
    )


@api_v1.route("/projects/<int:project_id>/records/unlink", methods=["POST"])
@wrap_global_route("projects", "modify")
def project_unlink_records_v1(project_id: int, body_data: ProjectUnlinkRecordsBody):
    return storage_socket.projects.unlink_records(
        project_id=project_id,
        record_ids=body_data.record_ids,
        delete_records=body_data.delete_records,
    )


@api_v1.route("/projects/<int:project_id>/records/<int:record_id>", methods=["GET"])
@wrap_global_route("projects", "read")
def get_project_record_v1(project_id: int, record_id: int, url_params: ProjURLParameters):
    return storage_socket.projects.get_record(
        project_id, record_id, include=url_params.include, exclude=url_params.exclude
    )


#########################
# Modifying metadata
#########################
# @api_v1.route("/projects/<string:project_type>/<int:project_id>", methods=["PATCH"])
# @wrap_global_route("projects", "modify")
# def modify_project_metadata_v1(project_type: str, project_id: int, body_data: DatasetModifyMetadata):
#    ds_socket = storage_socket.projects.get_socket(project_type)
#    return ds_socket.update_metadata(project_id, new_metadata=body_data)


#########################
# Attachments
#################################
@api_v1.route("/projects/<int:project_id>/attachments", methods=["GET"])
@wrap_global_route("projects", "read")
def fetch_project_attachments_v1(project_id: int):
    return storage_socket.projects.get_attachments(project_id)


@api_v1.route("/projects/<int:project_id>/attachments/<int:attachment_id>", methods=["DELETE"])
@wrap_global_route("projects", "modify")
def delete_project_attachment_v1(project_id: int, attachment_id: int):
    return storage_socket.projects.delete_attachment(project_id, attachment_id)
