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

    # Add a HOOH
    hooh = portal.data.get_molecule("hooh.json")
    db = dask_server_fixture.objects["db_socket"]
    mol_ret = db.add_molecules({"hooh": hooh.to_json()})

    # Add compute
    compute = {
        "meta": {
            "service": "crank",
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
        },
        "data": [mol_ret["data"]["hooh"]],
    }

    # Ask the server to compute a new computation
    r = requests.post(dask_server_fixture.get_address("service_scheduler"), json=compute)
    assert r.status_code == 200
    compute_key = r.json()["data"][0]

    # Manually handle the compute
    nanny = dask_server_fixture.objects["queue_nanny"]
    nanny.await_services(max_iter=5)
    assert len(nanny.list_current_tasks()) == 0

    raw_result = db.get_services([compute_key], by_id=True)["data"][0]

    result = portal.orm.CrankORM.from_json(raw_result)

    assert pytest.approx(-148.7505629010982, 1e-5) == result.final_energies(0)
    assert pytest.approx(-148.76416544463615, 1e-5) == result.final_energies(90)
    assert pytest.approx(-148.76501336999286, 1e-5) == result.final_energies(180)
    assert pytest.approx(-148.7641654446591, 1e-5) == result.final_energies(-90)
