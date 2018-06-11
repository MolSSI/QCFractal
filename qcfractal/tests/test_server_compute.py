"""
Tests the server compute capabilities.
"""

import qcfractal.interface as qp
import qcfractal as qf

from qcfractal.queue_handlers import build_queue
from qcfractal import testing
import qcengine
import requests
import pytest

dask_server = testing.test_dask_server
scheduler_api_addr = testing.test_server_address + "scheduler"

@testing.using_psi4
@testing.using_dask
def test_queue_stack_dask(dask_server):


    # Add a hydrogen molecule
    hydrogen = qp.Molecule([[1, 0, 0, -0.5], [1, 0, 0, 0.5]], dtype="numpy", units="bohr")
    db = dask_server.objects["db_socket"]
    mol_ret = db.add_molecules({"hydrogen": hydrogen.to_json()})

    option = qp.data.get_options("psi_default")
    opt_ret = db.add_options([option])
    opt_key = option["name"]

    # Add compute
    compute = {
        "molecule_id": mol_ret["data"]["hydrogen"],
        "driver": "energy",
        "method": "HF",
        "basis": "sto-3g",
        "options": opt_key
    }

    # Ask the server to compute a new computation
    r = requests.post(scheduler_api_addr, json={"meta": {"program": "psi4"}, "data":[compute]})
    assert r.status_code == 200
    compute_key = tuple(r.json()["data"][0])

    # Manually handle the compute
    nanny = dask_server.objects["queue_nanny"]
    ret = nanny.queue[compute_key].result()
    nanny.update()
    assert len(nanny.queue) == 0

    # Query result and check against out manual pul
    results_query = {"program": "psi4", "molecule_id": compute["molecule_id"], "method": compute["method"], "basis": compute["basis"]}
    results = db.get_results(results_query)["data"]

    assert len(results) == 1
    assert pytest.approx(ret["properties"]["scf_total_energy"], 1e-6) == -1.0660263371078127






