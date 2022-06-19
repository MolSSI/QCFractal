from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcfractal.components.records.singlepoint.testing_helpers import submit_test_data
from qcfractal.components.wavefunctions.db_models import WavefunctionStoreORM
from qcfractaltesting import load_wavefunction_data
from qcportal.wavefunctions import WavefunctionProperties

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


@pytest.fixture()
def existing_record_id(storage_socket):
    """
    Build a singlepoint calculation

    Needed for adding entries to the wavefunction store, which require a relationship
    with an existing calculation
    """

    record_id, _ = submit_test_data(storage_socket, "sp_psi4_benzene_energy_1")

    yield record_id


def assert_wfn_equal(wfn1: WavefunctionProperties, wfn2: WavefunctionProperties):
    return wfn1.dict(encoding="json") == wfn2.dict(encoding="json")


@pytest.mark.parametrize("wfn_data", ["psi4_peroxide", "psi4_fluoroethane"])
def test_wavefunction_models_roundtrip(storage_socket: SQLAlchemySocket, existing_record_id, wfn_data):
    wfn = load_wavefunction_data(wfn_data)

    # Add wavefunction data
    wfn_orm = WavefunctionStoreORM.from_model(wfn)
    wfn_orm.record_id = existing_record_id

    with storage_socket.session_scope() as session:
        session.add(wfn_orm)

    # Retrieve again
    with storage_socket.session_scope() as session:
        stored_orm = (
            session.query(WavefunctionStoreORM).where(WavefunctionStoreORM.record_id == existing_record_id).one()
        )
        wfn_model = WavefunctionProperties(**stored_orm.model_dict())

    assert_wfn_equal(wfn_model, wfn)
