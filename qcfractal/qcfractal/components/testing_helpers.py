"""
Tests the general record socket
"""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING

from qcelemental.models import FailedOperation, ComputeError

from qcfractal.components.optimization.testing_helpers import load_test_data as load_opt_test_data
from qcfractal.components.record_db_models import BaseRecordORM
from qcfractal.components.singlepoint.testing_helpers import load_test_data as load_sp_test_data
from qcfractal.testing_helpers import mname1
from qcfractalcompute.compress import compress_result
from qcportal.compression import decompress
from qcportal.record_models import PriorityEnum, RecordStatusEnum

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def populate_records_status(storage_socket: SQLAlchemySocket):
    """
    Populates the db with tasks in all statuses
    """

    manager_programs = {
        "qcengine": ["unknown"],
        "psi4": ["unknown"],
        "qchem": ["v3.0"],
        "rdkit": ["unknown"],
        "geometric": ["unknown"],
    }

    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs=manager_programs,
        compute_tags=["tag1", "tag2", "tag3", "tag6"],
    )

    input_spec_0, molecule_0, result_data_0 = load_opt_test_data("opt_psi4_methane_sometraj")
    input_spec_1, molecule_1, result_data_1 = load_sp_test_data("sp_psi4_water_gradient")
    input_spec_2, molecule_2, result_data_2 = load_sp_test_data("sp_psi4_water_hessian")
    input_spec_3, molecule_3, result_data_3 = load_opt_test_data("opt_psi4_benzene")
    input_spec_4, molecule_4, result_data_4 = load_sp_test_data("sp_rdkit_water_energy")
    input_spec_5, molecule_5, result_data_5 = load_sp_test_data("sp_psi4_benzene_energy_2")
    input_spec_6, molecule_6, result_data_6 = load_sp_test_data("sp_psi4_water_energy")

    meta, id_0 = storage_socket.records.optimization.add(
        [molecule_0], input_spec_0, "tag0", PriorityEnum.normal, None, None, True
    )
    meta, id_1 = storage_socket.records.singlepoint.add(
        [molecule_1], input_spec_1, "tag1", PriorityEnum.high, None, None, True
    )
    meta, id_2 = storage_socket.records.singlepoint.add(
        [molecule_2], input_spec_2, "tag2", PriorityEnum.high, None, None, True
    )
    meta, id_3 = storage_socket.records.optimization.add(
        [molecule_3],
        input_spec_3,
        "tag3",
        PriorityEnum.high,
        None,
        None,
        True,
    )
    meta, id_4 = storage_socket.records.singlepoint.add(
        [molecule_4], input_spec_4, "tag4", PriorityEnum.normal, None, None, True
    )
    meta, id_5 = storage_socket.records.singlepoint.add(
        [molecule_5], input_spec_5, "tag5", PriorityEnum.normal, None, None, True
    )
    meta, id_6 = storage_socket.records.singlepoint.add(
        [molecule_6], input_spec_6, "tag6", PriorityEnum.normal, None, None, True
    )
    all_id = id_0 + id_1 + id_2 + id_3 + id_4 + id_5 + id_6

    # 0 = waiting   1 = complete   2 = running
    # 3 = error     4 = cancelled  5 = deleted
    # 6 = invalid

    # claim only the ones we want to be complete, running, or error (1, 2, 3, 6)
    # 6 needs to be complete to be invalidated
    tasks = storage_socket.tasks.claim_tasks(
        mname1.fullname, manager_programs, ["tag1", "tag2", "tag3", "tag6"], limit=4
    )
    assert len(tasks) == 4

    fop = FailedOperation(error=ComputeError(error_type="test_error", error_message="this is a test error"))

    # we don't send back the one we want to be 'running' still (#2)
    storage_socket.tasks.update_finished(
        mname1.fullname,
        {
            # tasks[1] is left running (corresponds to record 2)
            tasks[0]["id"]: compress_result(result_data_1.dict()),
            tasks[2]["id"]: compress_result(fop.dict()),
            tasks[3]["id"]: compress_result(result_data_6.dict()),
        },
    )

    # Add some more entries to the history of #3 (failing)
    for i in range(4):
        meta = storage_socket.records.reset(id_3)
        assert meta.success
        tasks = storage_socket.tasks.claim_tasks(
            mname1.fullname, manager_programs, ["tag1", "tag2", "tag3", "tag6"], limit=1
        )
        assert len(tasks) == 1
        assert tasks[0]["tag"] == "tag3"

        fop_compress = compress_result(fop.dict())
        storage_socket.tasks.update_finished(mname1.fullname, {tasks[0]["id"]: fop_compress})

    meta = storage_socket.records.cancel(id_4)
    assert meta.n_updated == 1
    meta = storage_socket.records.delete(id_5)
    assert meta.n_deleted == 1
    meta = storage_socket.records.invalidate(id_6)
    assert meta.n_updated == 1

    with storage_socket.session_scope() as session:
        all_rec = [session.get(BaseRecordORM, i) for i in all_id]
        assert all_rec[0].status == RecordStatusEnum.waiting
        assert all_rec[1].status == RecordStatusEnum.complete
        assert all_rec[2].status == RecordStatusEnum.running
        assert all_rec[3].status == RecordStatusEnum.error
        assert all_rec[4].status == RecordStatusEnum.cancelled
        assert all_rec[5].status == RecordStatusEnum.deleted
        assert all_rec[6].status == RecordStatusEnum.invalid

    return all_id


def convert_to_plain_qcschema_result(result):
    """
    Converts a manager-mangled qcschema result into a plain qcschema result.

    Managers typically compress outputs and native files then store them in extras.
    This removes those and puts them back in the proper place
    """

    update = {}

    extras = deepcopy(result.extras)

    compressed_outputs = extras.pop("_qcfractal_compressed_outputs", None)
    compressed_native_files = extras.pop("_qcfractal_compressed_native_files", None)

    if compressed_outputs or compressed_native_files:
        update["extras"] = extras

    if compressed_outputs:
        # Keys are stdout, stderr, error, which match the fields of the result (I hope)
        for k, v in compressed_outputs.items():
            update[k] = decompress(v["data"], v["compression_type"])

    if compressed_native_files:
        update["native_files"] = {}
        for k, v in compressed_native_files.items():
            update["native_files"][k] = decompress(v["data"], v["compression_type"])

    if update:
        return result.copy(update=update)
    else:
        return result
