"""
Tests the server compute capabilities.
"""

import qcfractal.interface as portal
from qcfractal import testing
from qcfractal.testing import fractal_compute_server
import requests
import pytest


### Tests the compute queue stack
@testing.using_psi4
def test_compute_queue_stack(fractal_compute_server):

    # Add a hydrogen and helium molecule
    hydrogen = portal.Molecule([[1, 0, 0, -0.5], [1, 0, 0, 0.5]], dtype="numpy", units="bohr")
    helium = portal.Molecule([[2, 0, 0, 0.0]], dtype="numpy", units="bohr")

    storage = fractal_compute_server.objects["storage_socket"]
    mol_ret = storage.add_molecules({"hydrogen": hydrogen.to_json(), "helium": helium.to_json()})

    hydrogen_mol_id = mol_ret["data"]["hydrogen"]
    helium_mol_id = mol_ret["data"]["helium"]

    option = portal.data.get_options("psi_default")
    opt_ret = storage.add_options([option])
    opt_key = option["name"]

    # Add compute
    compute = {
        "meta": {
            "procedure": "single",
            "driver": "energy",
            "method": "HF",
            "basis": "sto-3g",
            "options": opt_key,
            "program": "psi4",
        },
        "data": [hydrogen_mol_id, helium.to_json()],
    }

    # Ask the server to compute a new computation
    r = requests.post(fractal_compute_server.get_address("task_scheduler"), json=compute)
    assert r.status_code == 200
    compute_key = tuple(r.json()["data"][0])

    # Manually handle the compute
    nanny = fractal_compute_server.objects["queue_nanny"]
    nanny.await_results()
    assert len(nanny.list_current_tasks()) == 0

    # Query result and check against out manual pul
    results_query = {
        "program": "psi4",
        "molecule_id": [hydrogen_mol_id, helium_mol_id],
        "method": compute["meta"]["method"],
        "basis": compute["meta"]["basis"]
    }
    results = storage.get_results(results_query)["data"]

    assert len(results) == 2
    for r in results:
        if r["molecule_id"] == hydrogen_mol_id:
            assert pytest.approx(-1.0660263371078127, 1e-5) == r["properties"]["scf_total_energy"]
        elif r["molecule_id"] == helium_mol_id:
            assert pytest.approx(-2.807913354492941, 1e-5) == r["properties"]["scf_total_energy"]
        else:
            raise KeyError("Returned unexpected Molecule ID.")


### Tests the compute queue stack
@testing.using_geometric
@testing.using_psi4
def test_procedure_optimization(fractal_compute_server):

    # Add a hydrogen molecule
    hydrogen = portal.Molecule([[1, 0, 0, -0.672], [1, 0, 0, 0.672]], dtype="numpy", units="bohr")
    client = portal.FractalClient(fractal_compute_server.get_address(""))
    mol_ret = client.add_molecules({"hydrogen": hydrogen.to_json()})

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
        "data": [mol_ret["hydrogen"]],
    }

    # Ask the server to compute a new computation
    r = requests.post(fractal_compute_server.get_address("task_scheduler"), json=compute)
    assert r.status_code == 200
    compute_key = tuple(r.json()["data"][0])

    # Manually handle the compute
    nanny = fractal_compute_server.objects["queue_nanny"]
    nanny.await_results()
    assert len(nanny.list_current_tasks()) == 0

    # # Query result and check against out manual pul
    results = client.get_procedures({"program": "geometric"})

    assert len(results) == 1
    assert isinstance(str(results[0]), str)  # Check that repr runs
    assert pytest.approx(-1.117530188962681, 1e-5) == results[0].final_energy()


### Tests an entire server and interaction energy database run
@testing.using_psi4
def test_compute_database(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server.get_address(""))
    db_name = "He_PES"
    db = portal.collections.Database(db_name, client, db_type="ie")

    # Adds options
    option = portal.data.get_options("psi_default")

    opt_ret = client.add_options([option])
    opt_key = option["name"]

    # Add two helium dimers to the DB at 4 and 8 bohr
    He1 = portal.Molecule([[2, 0, 0, -2], [2, 0, 0, 2]], dtype="numpy", units="bohr", frags=[1])
    db.add_ie_rxn("He1", He1, attributes={"r": 4}, reaction_results={"default": {"Benchmark": 0.0009608501557}})

    # Save the DB and re-acquire
    r = db.save()
    db = portal.collections.Database(db_name, client)

    He2 = portal.Molecule([[2, 0, 0, -4], [2, 0, 0, 4]], dtype="numpy", units="bohr", frags=[1])
    db.add_ie_rxn("He2", He2, attributes={"r": 4}, reaction_results={"default": {"Benchmark": -0.00001098794749}})

    # Save the DB and overwrite the result
    r = db.save(overwrite=True)

    # Open a new database
    db = portal.collections.Database(db_name, client)

    # Compute SCF/sto-3g
    ret = db.compute("SCF", "STO-3G")
    fractal_compute_server.objects["queue_nanny"].await_results()

    # Query computed results
    assert db.query("SCF", "STO-3G")
    assert pytest.approx(0.6024530476071095, 1.e-5) == db.df.ix["He1", "SCF/STO-3G"]
    assert pytest.approx(-0.006895035942673289, 1.e-5) == db.df.ix["He2", "SCF/STO-3G"]

    # Check results
    assert db.query("Benchmark", "", reaction_results=True)
    assert pytest.approx(0.00024477933196125805, 1.e-5) == db.statistics("MUE", "SCF/STO-3G")
