"""
Tests the server compute capabilities.
"""

import numpy as np
import pytest
import requests
from qcelemental.util import parse_version

import qcengine as qcng
import qcfractal.interface as ptl
from qcfractal import testing
from qcfractal.testing import fractal_compute_server


### Tests the compute queue stack
@testing.using_psi4
def test_compute_queue_stack(fractal_compute_server):

    # Build a client
    client = ptl.FractalClient(fractal_compute_server)

    # Add a hydrogen and helium molecule
    hydrogen = ptl.Molecule.from_data([[1, 0, 0, -0.5], [1, 0, 0, 0.5]], dtype="numpy", units="bohr")
    helium = ptl.Molecule.from_data([[2, 0, 0, 0.0]], dtype="numpy", units="bohr")

    hydrogen_mol_id, helium_mol_id = client.add_molecules([hydrogen, helium])

    kw = ptl.models.KeywordSet(**{"values": {"e_convergence": 1.0e-8}})
    kw_id = client.add_keywords([kw])[0]

    # Add compute
    compute_args = {"driver": "energy", "method": "HF", "basis": "sto-3g", "keywords": kw_id, "program": "psi4"}

    # Ask the server to compute a new computation
    r = client.add_compute("psi4", "HF", "sto-3g", "energy", kw_id, [hydrogen_mol_id, helium])
    assert len(r.ids) == 2

    r2 = client.add_compute(**compute_args, molecule=[hydrogen_mol_id, helium])
    assert len(r2.ids) == 2
    assert len(r2.submitted) == 0
    assert set(r2.ids) == set(r.ids)

    # Manually handle the compute
    fractal_compute_server.await_results()
    assert len(fractal_compute_server.list_current_tasks()) == 0

    # Query result and check against out manual pul
    results_query = {
        "program": "psi4",
        "molecule": [hydrogen_mol_id, helium_mol_id],
        "method": compute_args["method"],
        "basis": compute_args["basis"],
    }
    results = client.query_results(**results_query, status=None)

    assert len(results) == 2
    for r in results:
        assert r.provenance.creator.lower() == "psi4"
        if r.molecule == hydrogen_mol_id:
            assert pytest.approx(-1.0660263371078127, 1e-5) == r.properties.scf_total_energy
        elif r.molecule == helium_mol_id:
            assert pytest.approx(-2.807913354492941, 1e-5) == r.properties.scf_total_energy
        else:
            raise KeyError("Returned unexpected Molecule ID.")

    assert "RHF Reference" in results[0].get_stdout()


### Tests the compute queue stack
@testing.using_psi4
def test_compute_wavefunction(fractal_compute_server):

    psiver = qcng.get_program("psi4").get_version()
    if parse_version(psiver) < parse_version("1.4a2.dev160"):
        pytest.skip("Must be used a modern version of Psi4 to execute")

    # Build a client
    client = ptl.FractalClient(fractal_compute_server)

    # Add a hydrogen and helium molecule
    hydrogen = ptl.Molecule.from_data([[1, 0, 0, -0.5], [1, 0, 0, 0.5]], dtype="numpy", units="bohr")

    # Ask the server to compute a new computation
    r = client.add_compute(
        program="psi4",
        driver="energy",
        method="HF",
        basis="sto-3g",
        molecule=hydrogen,
        protocols={"wavefunction": "orbitals_and_eigenvalues"},
    )

    fractal_compute_server.await_results()
    assert len(fractal_compute_server.list_current_tasks()) == 0

    result = client.query_results(id=r.ids)[0]
    assert result.wavefunction

    r = result.get_wavefunction("orbitals_a")
    assert isinstance(r, np.ndarray)
    assert r.shape == (2, 2)

    r = result.get_wavefunction(["orbitals_a", "basis"])
    assert r.keys() == {"orbitals_a", "basis"}


### Tests the compute queue stack
@testing.using_geometric
@testing.using_psi4
def test_procedure_optimization_single(fractal_compute_server):

    # Add a hydrogen molecule
    hydrogen = ptl.Molecule.from_data([[1, 0, 0, -0.672], [1, 0, 0, 0.672]], dtype="numpy", units="bohr")
    client = ptl.FractalClient(fractal_compute_server.get_address(""))
    mol_ret = client.add_molecules([hydrogen])

    kw = ptl.models.KeywordSet(values={"scf_properties": ["quadrupole", "wiberg_lowdin_indices"]})
    kw_id = client.add_keywords([kw])[0]

    # Add compute
    options = {
        "keywords": None,
        "qc_spec": {"driver": "gradient", "method": "HF", "basis": "sto-3g", "keywords": kw_id, "program": "psi4"},
    }

    # Ask the server to compute a new computation
    r = client.add_procedure("optimization", "geometric", options, mol_ret)
    assert len(r.ids) == 1
    compute_key = r.ids[0]

    # Manually handle the compute
    fractal_compute_server.await_results()
    assert len(fractal_compute_server.list_current_tasks()) == 0

    # # Query result and check against out manual pul
    query1 = client.query_procedures(procedure="optimization", program="geometric")
    query2 = client.query_procedures(id=compute_key)

    for query in [query1, query2]:
        assert len(query) == 1
        opt_result = query[0]

        assert isinstance(opt_result.provenance.creator, str)
        assert isinstance(str(opt_result), str)  # Check that repr runs
        assert pytest.approx(-1.117530188962681, 1e-5) == opt_result.get_final_energy()

        # Check pulls
        traj = opt_result.get_trajectory()
        assert len(traj) == len(opt_result.energies)

        assert np.array_equal(opt_result.get_final_molecule().symbols, ["H", "H"])

        # Check individual elements
        for ind in range(len(opt_result.trajectory)):
            assert traj[ind].program == "psi4"

            # Check keywords went through
            assert traj[ind].provenance.creator.lower() == "psi4"
            assert int(traj[ind].keywords) == int(kw_id)
            assert "SCF QUADRUPOLE XY" in traj[ind].extras["qcvars"]
            assert "WIBERG_LOWDIN_INDICES" in traj[ind].extras["qcvars"]

            # Make sure extra was popped
            assert "_qcfractal_tags" not in traj[ind].extras

            raw_energy = traj[ind].properties.return_energy
            assert pytest.approx(raw_energy, 1.0e-5) == opt_result.energies[ind]

        # Check result stdout
        assert "RHF Reference" in traj[0].get_stdout()

        assert opt_result.get_molecular_trajectory()[0].id == opt_result.initial_molecule
        assert opt_result.get_molecular_trajectory()[-1].id == opt_result.final_molecule

        # Check stdout
        assert "internal coordinates" in opt_result.get_stdout()

    # Check that duplicates are caught
    r = client.add_procedure("optimization", "geometric", options, [mol_ret[0]])
    assert len(r.ids) == 1
    assert len(r.existing) == 1


@testing.using_geometric
@testing.using_psi4
def test_procedure_optimization_protocols(fractal_compute_server):

    # Add a hydrogen molecule
    hydrogen = ptl.Molecule.from_data([[1, 0, 0, -0.673], [1, 0, 0, 0.673]], dtype="numpy", units="bohr")
    client = fractal_compute_server.client()

    # Add compute
    options = {
        "keywords": None,
        "qc_spec": {"driver": "gradient", "method": "HF", "basis": "sto-3g", "program": "psi4"},
        "protocols": {"trajectory": "final"},
    }

    # Ask the server to compute a new computation
    r = client.add_procedure("optimization", "geometric", options, [hydrogen])
    assert len(r.ids) == 1
    compute_key = r.ids[0]

    # Manually handle the compute
    fractal_compute_server.await_results()
    assert len(fractal_compute_server.list_current_tasks()) == 0

    # # Query result and check against out manual pul
    proc = client.query_procedures(id=r.ids)[0]
    assert proc.status == "COMPLETE"

    assert len(proc.trajectory) == 1
    assert len(proc.energies) > 1
