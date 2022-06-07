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

    record_id, _ = submit_test_data(storage_socket, "psi4_benzene_energy_1")

    yield record_id


def assert_wfn_equal(wfn1: WavefunctionProperties, wfn2: WavefunctionProperties):
    assert wfn1.restricted == wfn2.restricted
    assert wfn1.basis == wfn2.basis

    # These are just strings
    ret_fields = WavefunctionProperties._return_results_names

    # These are all other fields (numpy arrays)
    np_fields = WavefunctionProperties.__fields__.keys() - ["basis", "restricted"] - ret_fields

    for f in ret_fields:
        v1 = getattr(wfn1, f)
        v2 = getattr(wfn2, f)
        assert v1 == v2

    for f in np_fields:
        v1 = getattr(wfn1, f)
        v2 = getattr(wfn2, f)

        if v1 is None or v2 is None:
            assert v1 is None
            assert v2 is None
        else:
            assert (v1 == v2).all()


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
