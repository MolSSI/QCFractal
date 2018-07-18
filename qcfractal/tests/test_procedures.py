"""
Tests the on-node procedures compute capabilities.
"""



import qcfractal.interface as qp
import qcfractal as qf

from qcfractal.queue_handlers import build_queue
from qcfractal import testing
from qcfractal.testing import dask_server_fixture
import qcengine
import requests
import pytest


### Tests the compute queue stack
@testing.using_geometric
@testing.using_psi4
def test_procedure_optimization(dask_server_fixture):

    # Add a hydrogen molecule
    hydrogen = qp.Molecule([[1, 0, 0, -0.672], [1, 0, 0, 0.672]], dtype="numpy", units="bohr")
    db = dask_server_fixture.objects["db_socket"]
    mol_ret = db.add_molecules({"hydrogen": hydrogen.to_json()})

    # Add compute
    compute = {
        "meta": {
            "procedure": "optimization",
            "options": "none",
            "program": "geometric",
            "qc_meta": {
                "driver": "gradient",
                "method": "HF",
                "basis": "sto-3g",
                "options": "none",
                "program": "psi4"
            },
        },
        "data": [mol_ret["data"]["hydrogen"]],
    }

    # Ask the server to compute a new computation
    r = requests.post(dask_server_fixture.get_address("scheduler"), json=compute)
    assert r.status_code == 200
    compute_key = tuple(r.json()["data"][0])

    # Manually handle the compute
    nanny = dask_server_fixture.objects["queue_nanny"]
    nanny.await_results()
    assert len(nanny.list_current_tasks()) == 0

    # # Query result and check against out manual pul
    # results_query = {
    #     "program": "psi4",
    #     "molecule_id": compute["data"][0],
    #     "method": compute["meta"]["method"],
    #     "basis": compute["meta"]["basis"]
    # }
    # results = db.get_results(results_query)["data"]

    # assert len(results) == 1
    # assert pytest.approx(-1.0660263371078127, 1e-6) == results[0]["properties"]["scf_total_energy"]