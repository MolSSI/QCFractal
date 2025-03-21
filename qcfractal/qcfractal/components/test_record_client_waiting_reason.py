"""
Tests the tasks socket (claiming & returning data)
"""

from __future__ import annotations

import re

from qcarchivetesting.testing_classes import QCATestingSnowflake
from qcfractal.components.optimization.testing_helpers import load_test_data as load_opt_test_data
from qcfractal.components.singlepoint.testing_helpers import (
    load_test_data as load_sp_test_data,
)
from qcfractal.components.torsiondrive.testing_helpers import load_test_data as load_td_test_data
from qcportal.managers import ManagerName
from qcportal.record_models import PriorityEnum


def test_record_client_waiting_reason(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    input_spec_1, molecule_1, result_data_1 = load_sp_test_data("sp_psi4_water_energy")
    input_spec_2, molecule_2, result_data_2 = load_opt_test_data("opt_psi4_benzene")
    input_spec_3, molecule_3, result_data_3 = load_sp_test_data("sp_rdkit_benzene_energy")

    meta, id_1 = storage_socket.records.singlepoint.add(
        [molecule_1], input_spec_1, "tag1", PriorityEnum.low, None, None, True
    )
    meta, id_2 = storage_socket.records.optimization.add(
        [molecule_2], input_spec_2, "tag2", PriorityEnum.normal, None, None, True
    )
    meta, id_3 = storage_socket.records.singlepoint.add(
        [molecule_3], input_spec_3, "tag3", PriorityEnum.high, None, None, True
    )
    id_1 = id_1[0]
    id_2 = id_2[0]
    id_3 = id_3[0]
    all_id = [id_1, id_2, id_3]

    for i in all_id:
        reason = snowflake_client.get_waiting_reason(i)
        assert reason["reason"] == "No active managers"

    # Activate some managers
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": ["unknown"], "other_prog": ["unknown"]},
        compute_tags=["tag1"],
    )

    mname2 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-7890")
    storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": ["unknown"], "geometric": ["unknown"]},
        compute_tags=["tag2"],
    )

    mname3 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-8888")
    storage_socket.managers.activate(
        name_data=mname3,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": ["unknown"], "psi4": ["unknown"]},
        compute_tags=["tag999"],
    )

    mname4 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-0123")
    storage_socket.managers.activate(
        name_data=mname4,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["unknown"]},
        compute_tags=["tag999"],
    )

    reason = snowflake_client.get_waiting_reason(id_1)
    assert reason["reason"] == "No manager matches programs & tags"
    assert re.search(r"missing programs.*psi4", reason["details"][mname1.fullname])
    assert re.search(r"missing programs.*psi4", reason["details"][mname2.fullname])
    assert re.search(r"does not handle tag.*tag1", reason["details"][mname3.fullname])
    assert re.search(r"does not handle tag.*tag1", reason["details"][mname4.fullname])

    reason = snowflake_client.get_waiting_reason(id_2)
    assert reason["reason"] == "No manager matches programs & tags"
    assert re.search(r"missing programs.*psi4", reason["details"][mname1.fullname])
    assert re.search(r"missing programs.*geometric", reason["details"][mname1.fullname])
    assert re.search(r"missing programs.*psi4", reason["details"][mname2.fullname])
    assert re.search(r"missing programs.*geometric", reason["details"][mname3.fullname])
    assert re.search(r"does not handle tag.*tag2", reason["details"][mname4.fullname])

    # Add a working manager
    mname5 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-2222")
    storage_socket.managers.activate(
        name_data=mname5,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["unknown"]},
        compute_tags=["tag1", "tag2"],
    )

    reason = snowflake_client.get_waiting_reason(id_1)
    assert reason["reason"] == "Waiting for a free manager"
    assert reason["details"][mname5.fullname] == "Manager is busy"

    reason = snowflake_client.get_waiting_reason(id_2)
    assert reason["reason"] == "Waiting for a free manager"
    assert reason["details"][mname5.fullname] == "Manager is busy"

    # third test record requires rdkit, which we haven't given yet
    reason = snowflake_client.get_waiting_reason(id_3)
    assert reason["reason"] == "No manager matches programs & tags"
    assert re.search(r"missing programs.*rdkit", reason["details"][mname1.fullname])
    assert re.search(r"missing programs.*rdkit", reason["details"][mname2.fullname])
    assert re.search(r"missing programs.*rdkit", reason["details"][mname3.fullname])
    assert re.search(r"missing programs.*rdkit", reason["details"][mname4.fullname])
    assert re.search(r"missing programs.*rdkit", reason["details"][mname5.fullname])

    # Also try through the record
    record = snowflake_client.get_records(id_3)
    assert record.get_waiting_reason() == reason

    # Add a working manager with * tag
    mname6 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-0011")
    storage_socket.managers.activate(
        name_data=mname6,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["unknown"], "rdkit": ["unknown"]},
        compute_tags=["tag1", "*"],
    )

    reason = snowflake_client.get_waiting_reason(id_3)
    assert reason["reason"] == "Waiting for a free manager"
    assert re.search(r"Manager is busy", reason["details"][mname6.fullname])


def test_record_client_waiting_reason_2(snowflake: QCATestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    snowflake_client = snowflake.client()

    input_spec_1, molecule_1, result_data_1 = load_sp_test_data("sp_psi4_water_energy")
    input_spec_2, molecule_2, _ = load_td_test_data("td_H2O2_mopac_pm6")

    meta, id_1 = storage_socket.records.singlepoint.add(
        [molecule_1], input_spec_1, "tag1", PriorityEnum.low, None, None, True
    )

    meta, id_2 = snowflake_client.add_torsiondrives(
        [molecule_2],
        "torsiondrive",
        keywords=input_spec_2.keywords,
        optimization_specification=input_spec_2.optimization_specification,
    )

    id_1 = id_1[0]
    id_2 = id_2[0]

    # Add a working manager with * tag
    mname6 = ManagerName(cluster="test_cluster", hostname="a_host1", uuid="1234-5678-1234-0011")
    storage_socket.managers.activate(
        name_data=mname6,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": ["unknown"], "psi4": ["unknown"], "geometric": ["unknown"], "rdkit": ["unknown"]},
        compute_tags=["tag1", "*"],
    )

    # Should be able to be picked up
    reason = snowflake_client.get_waiting_reason(id_1)
    assert reason["reason"] == "Waiting for a free manager"
    assert re.search(r"Manager is busy", reason["details"][mname6.fullname])

    # Reason: not actually waiting
    storage_socket.records.cancel([id_1])
    reason = snowflake_client.get_waiting_reason(id_1)
    assert reason["reason"] == "Record is not waiting"

    # Reason: does not exist
    reason = snowflake_client.get_waiting_reason(id_1 + id_2)
    assert reason["reason"] == "Record does not exist"

    # Reason: is a service
    reason = snowflake_client.get_waiting_reason(id_2)
    assert reason["reason"] == "Record is a service"
