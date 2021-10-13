"""
Tests the wavefunction store socket
"""

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.exceptions import MissingDataError
from qcfractal.interface.models import WavefunctionProperties
from qcfractal.testing import load_wavefunction_data


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


def test_wavefunctions_socket_basic(storage_socket: SQLAlchemySocket):

    wfn1 = load_wavefunction_data("psi4_peroxide")
    wfn2 = load_wavefunction_data("psi4_fluoroethane")

    # Add wavefunction data
    added_ids = storage_socket.wavefunctions.add([wfn1, wfn2])
    assert len(added_ids) == 2

    r = storage_socket.wavefunctions.get([added_ids[1], added_ids[0], added_ids[1]], missing_ok=False)
    assert len(r) == 3

    r0 = WavefunctionProperties(**r[0])
    r1 = WavefunctionProperties(**r[1])
    r2 = WavefunctionProperties(**r[2])

    assert_wfn_equal(r0, wfn2)
    assert_wfn_equal(r1, wfn1)
    assert_wfn_equal(r2, wfn2)


def test_wavefunctions_socket_get_proj(storage_socket: SQLAlchemySocket):
    wfn1 = load_wavefunction_data("psi4_peroxide")
    wfn2 = load_wavefunction_data("psi4_fluoroethane")

    # Add wavefunction data
    added_ids = storage_socket.wavefunctions.add([wfn1, wfn2])
    assert len(added_ids) == 2

    r = storage_socket.wavefunctions.get(
        added_ids, include=["scf_orbitals_a", "scf_occupations_a", "basis", "orbitals_a"]
    )
    assert len(r) == 2

    assert set(r[0].keys()) == {"scf_orbitals_a", "scf_occupations_a", "basis", "orbitals_a"}
    assert set(r[1].keys()) == {"scf_orbitals_a", "basis", "orbitals_a"}  # occupations not included and stripped

    r = storage_socket.wavefunctions.get(
        added_ids, exclude=["scf_orbitals_a", "scf_occupations_a", "basis", "orbitals_a"]
    )
    r = storage_socket.wavefunctions.get(
        added_ids, exclude=["scf_orbitals_a", "scf_occupations_a", "basis", "orbitals_a"]
    )
    assert len(r) == 2
    assert set(r[0].keys()).intersection({"scf_orbitals_a", "scf_occupations_a", "basis", "orbitals_a"}) == set()
    assert set(r[1].keys()).intersection({"scf_orbitals_a", "scf_occupations_a", "basis", "orbitals_a"}) == set()


def test_wavefunctions_socket_get_nonexist(storage_socket: SQLAlchemySocket):

    wfn1 = load_wavefunction_data("psi4_peroxide")

    # Add wavefunction data
    added_ids = storage_socket.wavefunctions.add([wfn1])
    assert len(added_ids) == 1

    r = storage_socket.wavefunctions.get([added_ids[0] + 1], missing_ok=True)
    assert r == [None]

    with pytest.raises(MissingDataError, match="Could not find all requested records"):
        storage_socket.wavefunctions.get([added_ids[0] + 1], missing_ok=False)


def test_wavefunctions_socket_get_empty(storage_socket: SQLAlchemySocket):

    wfn1 = load_wavefunction_data("psi4_peroxide")

    # Add wavefunction data
    added_ids = storage_socket.wavefunctions.add([wfn1])
    assert len(added_ids) == 1

    r = storage_socket.wavefunctions.get([], missing_ok=True)
    assert r == []
