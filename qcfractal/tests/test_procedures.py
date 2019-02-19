"""
Tests the server compute capabilities.
"""

import pytest
import requests

import qcfractal.interface as portal
from qcfractal import testing
from qcfractal.testing import fractal_compute_server


### Tests the compute queue stack
@testing.using_psi4
def test_compute_queue_stack(fractal_compute_server):

    # Build a client
    client = portal.FractalClient(fractal_compute_server)

    # Add a hydrogen and helium molecule
    hydrogen = portal.Molecule.from_data([[1, 0, 0, -0.5], [1, 0, 0, 0.5]], dtype="numpy", units="bohr")
    helium = portal.Molecule.from_data([[2, 0, 0, 0.0]], dtype="numpy", units="bohr")

    hydrogen_mol_id, helium_mol_id = client.add_molecules([hydrogen, helium])

    kw = portal.models.KeywordSet(**{"program": "psi4", "values": {"e_convergence": 1.e-8}})
    kw_id = client.add_keywords([kw])[0]

    # Add compute
    compute = {
        "meta": {
            "procedure": "single",
            "driver": "energy",
            "method": "HF",
            "basis": "sto-3g",
            "keywords": kw_id,
            "program": "psi4",
        },
        "data": [hydrogen_mol_id, helium.json_dict()],
    }

    # Ask the server to compute a new computation
    r = client.add_compute("psi4", "HF", "sto-3g", "energy", kw_id, [hydrogen_mol_id, helium])
    assert len(r.ids) == 2

    # Manually handle the compute
    fractal_compute_server.await_results()
    assert len(fractal_compute_server.list_current_tasks()) == 0

    # Query result and check against out manual pul
    results_query = {
        "program": "psi4",
        "molecule": [hydrogen_mol_id, helium_mol_id],
        "method": compute["meta"]["method"],
        "basis": compute["meta"]["basis"]
    }
    results = client.get_results(**results_query)

    assert len(results) == 2
    for r in results:
        if r["molecule"] == hydrogen_mol_id:
            assert pytest.approx(-1.0660263371078127, 1e-5) == r["properties"]["scf_total_energy"]
        elif r["molecule"] == helium_mol_id:
            assert pytest.approx(-2.807913354492941, 1e-5) == r["properties"]["scf_total_energy"]
        else:
            raise KeyError("Returned unexpected Molecule ID.")


### Tests the compute queue stack
@testing.using_geometric
@testing.using_psi4
def test_procedure_optimization(fractal_compute_server):

    # Add a hydrogen molecule
    hydrogen = portal.Molecule.from_data([[1, 0, 0, -0.672], [1, 0, 0, 0.672]], dtype="numpy", units="bohr")
    client = portal.FractalClient(fractal_compute_server.get_address(""))
    mol_ret = client.add_molecules([hydrogen])

    # Add compute
    options = {
        "keywords": None,
        "qc_spec": {
            "driver": "gradient",
            "method": "HF",
            "basis": "sto-3g",
            "keywords": None,
            "program": "psi4"
        },
    }

    # Ask the server to compute a new computation
    r = client.add_procedure("optimization", "geometric", options, mol_ret)
    assert len(r.ids) == 1
    compute_key = r.ids[0]

    # Manually handle the compute
    fractal_compute_server.await_results()
    assert len(fractal_compute_server.list_current_tasks()) == 0

    # # Query result and check against out manual pul
    results1 = client.get_procedures({"program": "geometric"})
    results2 = client.get_procedures({"id": compute_key})

    for results in [results1, results2]:
        assert len(results) == 1
        assert isinstance(str(results[0]), str)  # Check that repr runs
        assert pytest.approx(-1.117530188962681, 1e-5) == results[0].final_energy()

        # Check pulls
        traj = results[0].get_trajectory(projection={"properties": True})
        energies = results[0].energies()
        assert len(traj) == len(energies)
        assert results[0].final_molecule().symbols == ["H", "H"]

        # Check individual elements
        for ind in range(len(results[0]._trajectory)):
            raw_energy = traj[ind]["properties"]["return_energy"]
            assert pytest.approx(raw_energy, 1.e-5) == energies[ind]

    # Check that duplicates are caught
    r = client.add_procedure("optimization", "geometric", options, [mol_ret[0]])
    assert len(r.ids) == 1
    assert len(r.existing) == 1
