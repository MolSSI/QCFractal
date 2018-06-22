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
fireworks_server = testing.test_fireworks_server
scheduler_api_addr = testing.test_server_address + "scheduler"


@testing.using_psi4
def test_queue_stack(fireworks_server):

    server = fireworks_server
    # Add a hydrogen molecule
    hydrogen = qp.Molecule([[1, 0, 0, -0.5], [1, 0, 0, 0.5]], dtype="numpy", units="bohr")
    db = server.objects["db_socket"]
    mol_ret = db.add_molecules({"hydrogen": hydrogen.to_json()})

    option = qp.data.get_options("psi_default")
    opt_ret = db.add_options([option])
    opt_key = option["name"]

    # Add compute
    compute = {
        "meta": {
            "driver": "energy",
            "method": "HF",
            "basis": "sto-3g",
            "options": opt_key,
            "program": "psi4",
        },
        "data": [mol_ret["data"]["hydrogen"]],
    }

    # Ask the server to compute a new computation
    r = requests.post(scheduler_api_addr, json=compute)
    assert r.status_code == 200
    compute_key = tuple(r.json()["data"][0])

    # Manually handle the compute
    nanny = server.objects["queue_nanny"]
    nanny.await_results()
    assert len(nanny.list_current_tasks()) == 0

    # Query result and check against out manual pul
    results_query = {
        "program": "psi4",
        "molecule_id": compute["data"][0],
        "method": compute["meta"]["method"],
        "basis": compute["meta"]["basis"]
    }
    results = db.get_results(results_query)["data"]

    assert len(results) == 1
    assert pytest.approx(-1.0660263371078127, 1e-6) == results[0]["properties"]["scf_total_energy"]


@testing.using_psi4
def test_server_database(fireworks_server):

    server = fireworks_server
    portal = qp.QCPortal(testing.test_server_address)
    db_name = "He_PES"
    db = qp.Database(db_name, portal, db_type="ie")

    # Adds options
    option = qp.data.get_options("psi_default")

    opt_ret = portal.add_options([option])
    opt_key = option["name"]

    # Add two helium dimers to the DB at 4 and 8 bohr
    He1 = qp.Molecule([[2, 0, 0, -2], [2, 0, 0, 2]], dtype="numpy", units="bohr", frags=[1])
    db.add_ie_rxn("He1", He1, attributes={"r": 4}, reaction_results={"default": {"Benchmark": 0.0009608501557}})

    He2 = qp.Molecule([[2, 0, 0, -4], [2, 0, 0, 4]], dtype="numpy", units="bohr", frags=[1])
    db.add_ie_rxn("He2", He2, attributes={"r": 4}, reaction_results={"default": {"Benchmark": -0.00001098794749}})

    # Save the DB
    db.save()

    # Open a new database
    db = qp.Database(db_name, portal)

    # Compute SCF/sto-3g
    ret = db.compute("SCF", "STO-3G")
    server.objects["queue_nanny"].await_results()

    # Query computed results
    assert db.query("SCF", "STO-3G")
    assert pytest.approx(0.6024530476071095, 1.e-6) == db.df.ix["He1", "SCF/STO-3G"]
    assert pytest.approx(-0.006895035942673289, 1.e-6) == db.df.ix["He2", "SCF/STO-3G"]

    # Check results
    assert db.query("Benchmark", "", reaction_results=True)
    assert pytest.approx(0.00024477933196125805, 1.e-4) == db.statistics("MUE", "SCF/STO-3G")
