"""
Tests the server compute capabilities.
"""

import pytest
import requests

import qcfractal.interface as portal
from qcfractal import testing
from qcfractal.testing import fractal_compute_server, reset_server_database, using_psi4, using_rdkit


@pytest.mark.parametrize("data", [
    pytest.param(("psi4", "HF", "sto-3g"), id="psi4", marks=using_psi4),
    pytest.param(("rdkit", "UFF", None), id="rdkit", marks=using_rdkit)
])
def test_task_molecule_no_orientation(data, fractal_compute_server):
    """
    Molecule orientation should not change on compute
    """

    # Reset database each run
    reset_server_database(fractal_compute_server)

    client = portal.FractalClient(fractal_compute_server)

    mol = portal.Molecule(symbols=["H", "H"], geometry=[0, 0, 0, 0, 5, 0], connectivity=[(0, 1, 1)])

    mol_id = client.add_molecules([mol])[0]

    program, method, basis = data
    ret = client.add_compute(program, method, basis, "energy", None, [mol_id])

    # Manually handle the compute
    fractal_compute_server.await_results()

    # Check for the single result
    ret = client.get_results(id=ret.submitted)
    assert len(ret) == 1
    assert ret[0]["status"] == "COMPLETE"
    assert ret[0]["molecule"] == mol_id

    # Make sure no other molecule was added
    ret = client.get_molecules(["H2"], index="molecular_formula")
    assert len(ret) == 1
    assert ret[0].id == mol_id


@testing.using_rdkit
def test_task_error(fractal_compute_server):
    client = portal.FractalClient(fractal_compute_server)

    mol = portal.models.common_models.Molecule(**{
        "geometry": [0, 0, 0],
        "symbols": ["He"]
    })
    # Cookiemonster is an invalid method
    ret = client.add_compute("rdkit", "cookiemonster", "", "energy", None, [mol])

    # Manually handle the compute
    fractal_compute_server.await_results()

    # Check for error
    ret = client.check_results(id=ret.submitted)

    assert len(ret) == 1
    assert ret[0]["status"] == "ERROR"
    # assert "run_rdkit" in ret[0]["error"]["error_message"]
