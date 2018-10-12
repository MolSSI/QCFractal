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
    r = requests.post(fractal_compute_server.get_address("task_queue"), json=compute)
    assert r.status_code == 200

    # Manually handle the compute
    fractal_compute_server.await_results()
    manager = fractal_compute_server.objects["queue_manager"]
    assert len(manager.list_current_tasks()) == 0

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
            "program": "geometric",
            "options": "none",
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
    r = requests.post(fractal_compute_server.get_address("task_queue"), json=compute)
    assert r.status_code == 200

    # Get the first submitted job, the second index will be a hash_index
    submitted = r.json()["data"]["submitted"]
    compute_key = submitted[0]

    # Manually handle the compute
    fractal_compute_server.await_results()
    manager = fractal_compute_server.objects["queue_manager"]
    assert len(manager.list_current_tasks()) == 0

    # # Query result and check against out manual pul
    results1 = client.get_procedures({"program": "geometric"})
    results2 = client.get_procedures({"queue_id": compute_key})

    for results in [results1, results2]:
        assert len(results) == 1
        assert isinstance(str(results[0]), str)  # Check that repr runs
        assert pytest.approx(-1.117530188962681, 1e-5) == results[0].final_energy()

        # Check pulls
        traj = results[0].get_trajectory(projection={"properties": True})
        energies = results[0].energies()
        assert len(traj) == len(energies)
        assert results[0].final_molecule()["symbols"] == ["H", "H"]

        # Check individual elements
        for ind in range(len(results[0]._trajectory)):
            raw_energy = traj[ind]["properties"]["return_energy"]
            assert pytest.approx(raw_energy, 1.e-5) == energies[ind]

    # Check that duplicates are caught
    r = requests.post(fractal_compute_server.get_address("task_queue"), json=compute)
    assert r.status_code == 200
    assert len(r.json()["data"]["completed"]) == 1
