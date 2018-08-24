"""
Tests the on-node procedures compute capabilities.
"""

from qcfractal.queue_handlers import build_queue
from qcfractal import testing
from qcfractal.testing import dask_server_fixture
import requests
import pytest

import qcfractal.interface as portal


### Tests the compute queue stack
@testing.using_crank
@testing.using_geometric
@testing.using_psi4
@pytest.mark.slow
def test_service_crank(dask_server_fixture):

    client = portal.FractalClient(dask_server_fixture.get_address())

    # Add a HOOH
    hooh = portal.data.get_molecule("hooh.json")
    mol_ret = client.add_molecules({"hooh": hooh})

    # Geometric options
    crank_options = {
        "crank_meta": {
           "dihedrals": [[0, 1, 2, 3]],
           "grid_spacing": [90]
        },
        "geometric_meta": {
            "coordsys": "tric"
        },
        "qc_meta": {
            "driver": "gradient",
            "method": "HF",
            "basis": "sto-3g",
            "options": "none",
            "program": "psi4"
        },
    }

    ret = client.add_service("crank", [mol_ret["hooh"]], crank_options)
    compute_key = ret[0]

    # Manually handle the compute
    nanny = dask_server_fixture.objects["queue_nanny"]
    nanny.await_services(max_iter=5)
    assert len(nanny.list_current_tasks()) == 0

    # Get a CrankORM result and check data
    result = client.get_service(compute_key)[0]
    assert pytest.approx(-148.7505629010982, 1e-5) == result.final_energies(0)
    assert pytest.approx(-148.76416544463615, 1e-5) == result.final_energies(90)
    assert pytest.approx(-148.76501336999286, 1e-5) == result.final_energies(180)
    assert pytest.approx(-148.7641654446591, 1e-5) == result.final_energies(-90)
