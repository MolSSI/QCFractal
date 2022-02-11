from qcfractal.app import main, storage_socket
from qcfractal.app.routes import wrap_route
from qcportal.base_models import ProjURLParameters
from qcportal.datasets import DatasetQueryModel, DatasetGetRecordItemsBody, DatasetGetEntryBody


@main.route("/v1/dataset/<int:dataset_id>", methods=["GET"])
@wrap_route(None, ProjURLParameters)
def get_dataset_v1(dataset_id: int, *, url_params: ProjURLParameters):

    with storage_socket.session_scope(True) as session:
        ds_type = storage_socket.datasets.lookup_type(dataset_id, session=session)
        ds_socket = storage_socket.datasets.get_socket(ds_type)
        return ds_socket.get(dataset_id, url_params.include, url_params.exclude, session=session)


@main.route("/v1/dataset/query", methods=["POST"])
@wrap_route(DatasetQueryModel, None)
def query_dataset_v1(body_data: DatasetQueryModel):
    with storage_socket.session_scope(True) as session:
        dataset_id = storage_socket.datasets.lookup_id(body_data.dataset_type, body_data.name, session=session)
        ds_socket = storage_socket.datasets.lookup_type(dataset_id, session=session)
        return ds_socket.get(dataset_id, body_data.include, body_data.exclude, session=session)


#################################################################
# COMMON FUNCTIONS
# These functions are common to all datasets
# Note that the inputs are all the same, but the returned dicts
# are different
#################################################################


@main.route("/v1/dataset/<string:dataset_type>/<int:dataset_id>", methods=["GET"])
@wrap_route(None, ProjURLParameters)
def get_general_dataset_v1(dataset_type: str, dataset_id: int, *, url_params: ProjURLParameters):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.get(
        dataset_id,
        url_params.include,
        url_params.exclude,
    )


@main.route("/v1/dataset/<string:dataset_type>/<int:dataset_id>/status", methods=["GET"])
@wrap_route(None, None)
def get_general_dataset_status_v1(dataset_type: str, dataset_id: int):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.status(dataset_id)


@main.route("/v1/dataset/<string:dataset_type>/<int:dataset_id>/specification", methods=["GET"])
@wrap_route(None, None)
def get_general_dataset_specifications_v1(dataset_type: str, dataset_id: int):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    ds_data = ds_socket.get(dataset_id, ["specifications.*", "specifications.specification"], None, False)
    return ds_data["specifications"]


@main.route("/v1/dataset/<string:dataset_type>/<int:dataset_id>/entry/bulkGet", methods=["GET"])
@wrap_route(DatasetGetEntryBody, None)
def get_general_dataset_entries_v1(dataset_type: str, dataset_id: int, *, body_data: DatasetGetEntryBody):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.get_entries(
        dataset_id,
        entry_names=body_data.name,
        include=body_data.include,
        exclude=body_data.exclude,
        missing_ok=body_data.missing_ok,
    )


@main.route("/v1/dataset/<string:dataset_type>/<int:dataset_id>/record/bulkGet", methods=["GET"])
@wrap_route(DatasetGetRecordItemsBody, None)
def get_general_dataset_records_v1(dataset_type: str, dataset_id: int, *, body_data: DatasetGetRecordItemsBody):
    ds_socket = storage_socket.datasets.get_socket(dataset_type)
    return ds_socket.get_records(
        dataset_id,
        specification_names=body_data.specification_name,
        entry_names=body_data.entry_name,
        include=body_data.include,
        exclude=body_data.exclude,
    )
