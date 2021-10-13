"""
Tests the keywords subsocket
"""

import pytest

from qcfractal.db_socket import SQLAlchemySocket
from qcfractal.interface.models import WavefunctionProperties
from qcfractal.portal import PortalClient
from qcfractal.portal.client import PortalRequestError
from qcfractal.testing import load_wavefunction_data
from .test_sockets import assert_wfn_equal


def test_wavefunction_client_basic(storage_socket: SQLAlchemySocket, fractal_test_client: PortalClient):

    # Add via the storage socket - the client doesn't allow for adding wavefunctions
    wfn1 = load_wavefunction_data("psi4_peroxide")
    wfn2 = load_wavefunction_data("psi4_fluoroethane")

    added_ids = storage_socket.wavefunctions.add([wfn1, wfn2])

    # Now get via the client
    r = fractal_test_client.get_wavefunctions([added_ids[1], added_ids[1], added_ids[0]], missing_ok=False)
    assert len(r) == 3
    assert_wfn_equal(wfn2, r[0])
    assert_wfn_equal(wfn2, r[1])
    assert_wfn_equal(wfn1, r[2])

    # Get with a single id
    r = fractal_test_client.get_wavefunctions(added_ids[1])
    assert isinstance(r, WavefunctionProperties)
    assert_wfn_equal(r, wfn2)


def test_wavefunction_client_get_nonexist(storage_socket: SQLAlchemySocket, fractal_test_client: PortalClient):
    # Add via the storage socket - the client doesn't allow for adding wavefunctions
    wfn1 = load_wavefunction_data("psi4_peroxide")
    wfn2 = load_wavefunction_data("psi4_fluoroethane")

    added_ids = storage_socket.wavefunctions.add([wfn1, wfn2])

    # Now get via the client
    r = fractal_test_client.get_wavefunctions([added_ids[1], 999, added_ids[0]], missing_ok=True)
    assert len(r) == 3
    assert_wfn_equal(wfn2, r[0])
    assert_wfn_equal(wfn1, r[2])
    assert r[1] is None

    # Now try with missing_ok = False. This should raise an exception
    with pytest.raises(PortalRequestError, match=r"Could not find all requested records"):
        fractal_test_client._get_outputs([added_ids[0], 999], missing_ok=False)


def test_outputs_client_get_empty(storage_socket: SQLAlchemySocket, fractal_test_client: PortalClient):
    # Add via the storage socket - the client doesn't allow for adding wavefunctions
    wfn1 = load_wavefunction_data("psi4_peroxide")
    wfn2 = load_wavefunction_data("psi4_fluoroethane")

    added_ids = storage_socket.wavefunctions.add([wfn1, wfn2])

    out = fractal_test_client.get_wavefunctions([])
    assert out == []
