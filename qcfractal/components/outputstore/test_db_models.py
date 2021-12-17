"""
Tests the output_store models
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcfractal.components.outputstore.db_models import OutputStoreORM
from qcfractal.portal.managers import ManagerName
from qcfractal.portal.outputstore import OutputTypeEnum, CompressionEnum, OutputStore
from qcfractal.portal.records import PriorityEnum
from qcfractal.testing import load_procedure_data

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


@pytest.fixture()
def existing_history_id(storage_socket):
    """
    Build a singlepoint calculation

    Needed for adding entries to the output store, which require a relationship
    with an existing calculation
    """

    # Need a manager to claim the tasks
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"psi4": None, "qchem": "v3.0"},
        tags=["tag1"],
    )

    input_spec, molecule, result_data = load_procedure_data("psi4_benzene_energy_1")
    meta, id = storage_socket.records.singlepoint.add(input_spec, [molecule], "tag1", PriorityEnum.normal)
    tasks = storage_socket.tasks.claim_tasks(mname1.fullname)

    rmeta = storage_socket.tasks.update_finished(mname1.fullname, {tasks[0]["id"]: result_data})
    assert rmeta.accepted_ids == [tasks[0]["id"]]

    # Now there should be a compute history id that we can return
    rec = storage_socket.records.get(id)
    yield rec[0]["compute_history"][0]["id"]


@pytest.mark.parametrize("compression", CompressionEnum)
@pytest.mark.parametrize("compression_level", [None, 1, 5])
@pytest.mark.parametrize("output_type", OutputTypeEnum)
def test_outputs_models_roundtrip_str(
    storage_socket: SQLAlchemySocket, existing_history_id, compression, compression_level, output_type
):
    """
    Tests storing/retrieving plain string data in OutputStore
    """

    input_str = "This is some input " * 20
    output = OutputStore.compress(output_type, input_str, compression, compression_level)

    # Add both as the OutputStore and as the plain str
    out_orm = OutputStoreORM.from_model(output)
    out_orm.record_history_id = existing_history_id

    with storage_socket.session_scope() as session:
        session.add(out_orm)
        session.flush()
        out_id = out_orm.id

    # Retrieve again
    with storage_socket.session_scope() as session:
        stored_orm = session.query(OutputStoreORM).where(OutputStoreORM.id == out_id).one()
        out_model = OutputStore(**stored_orm.dict())

    assert out_model.id == out_id
    assert out_model.compression == compression
    assert out_model.get_string() == input_str

    # if compression_level is None (and compression is requested),
    # then a sensible default is used
    if compression_level is not None and compression is not CompressionEnum.none:
        assert out_model.compression_level == compression_level


@pytest.mark.parametrize("compression", CompressionEnum)
@pytest.mark.parametrize("compression_level", [None, 1, 5])
@pytest.mark.parametrize("output_type", OutputTypeEnum)
def test_outputs_models_roundtrip_dict(
    storage_socket: SQLAlchemySocket, existing_history_id, compression, compression_level, output_type
):
    """
    Tests storing/retrieving dict/json data in OutputStore
    """

    input_dict = {str(k): "This is some input " * k for k in range(5)}
    output = OutputStore.compress(output_type, input_dict, compression, compression_level)

    # Add both as the OutputStore and as the plain str
    out_orm = OutputStoreORM.from_model(output)
    out_orm.record_history_id = existing_history_id

    with storage_socket.session_scope() as session:
        session.add(out_orm)
        session.flush()
        out_id = out_orm.id

    # Retrieve again
    with storage_socket.session_scope() as session:
        stored_orm = session.query(OutputStoreORM).where(OutputStoreORM.id == out_id).one()
        out_model = OutputStore(**stored_orm.dict())

    assert out_model.id == out_id
    assert out_model.compression == compression
    assert out_model.get_json() == input_dict
    assert out_model.output_type == output_type

    # if compression_level is None (and compression is requested),
    # then a sensible default is used
    if compression_level is not None and compression is not CompressionEnum.none:
        assert out_model.compression_level == compression_level
