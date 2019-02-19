"""
Tests the on-node procedures compute capabilities.
"""

import copy

import pytest

import qcfractal.interface as portal
from qcfractal.testing import fractal_compute_server, recursive_dict_merge, using_geometric, using_rdkit

from qcfractal.interface.models.gridoptimization import GridOptimizationInput
from qcfractal.interface.models.torsiondrive import TorsionDriveInput


@pytest.fixture(scope="module")
def torsiondrive_fixture(fractal_compute_server):

    # Cannot use this fixture without these services. Also cannot use `mark` and `fixture` decorators
    pytest.importorskip("torsiondrive")
    pytest.importorskip("geometric")
    pytest.importorskip("rdkit")

    client = portal.FractalClient(fractal_compute_server)

    # Add a HOOH
    hooh = portal.data.get_molecule("hooh.json")
    mol_ret = client.add_molecules([hooh])

    # Geometric options
    torsiondrive_options = {
        "initial_molecule": mol_ret[0],
        "keywords": {
            "dihedrals": [[0, 1, 2, 3]],
            "grid_spacing": [90]
        },
        "optimization_spec": {
            "program": "geometric",
            "keywords": {
                "coordsys": "tric",
            }
        },
        "qc_spec": {
            "driver": "gradient",
            "method": "UFF",
            "basis": "",
            "keywords": None,
            "program": "rdkit",
        },
    }

    def spin_up_test(**keyword_augments):

        instance_options = copy.deepcopy(torsiondrive_options)
        recursive_dict_merge(instance_options, keyword_augments)

        inp = TorsionDriveInput(**instance_options)
        ret = client.add_service([inp], return_full=True)

        if ret.meta.n_inserted:  # In case test already submitted
            compute_key = ret.data.ids[0]
            status = client.check_services({"procedure_id": compute_key}, return_full=True)
            assert 'READY' in status.data[0]['status']
            assert status.data[0]['id'] != compute_key  # Hash should never be id

        fractal_compute_server.await_services()
        assert len(fractal_compute_server.list_current_tasks()) == 0
        return ret.data

    yield spin_up_test, client


def test_service_torsiondrive_single(torsiondrive_fixture):
    """"Tests torsiondrive pathway and checks the result result"""

    spin_up_test, client = torsiondrive_fixture

    ret = spin_up_test()

    # Get a TorsionDriveORM result and check data
    result = client.get_procedures({"id": ret.ids[0]})[0]
    assert isinstance(str(result), str)  # Check that repr runs

    assert pytest.approx(0.002597541340221565, 1e-5) == result.final_energies(0)
    assert pytest.approx(0.000156553761859276, 1e-5) == result.final_energies(90)
    assert pytest.approx(0.000156553761859271, 1e-5) == result.final_energies(-90)
    assert pytest.approx(0.000753492556057886, 1e-5) == result.final_energies(180)

    assert hasattr(result.final_molecules()[(-90, )], "symbols")


def test_service_torsiondrive_multi_single(torsiondrive_fixture):
    spin_up_test, client = torsiondrive_fixture

    hooh = portal.data.get_molecule("hooh.json")
    hooh2 = hooh.copy(deep=True)
    hooh2.geometry[0] += 0.0004

    ret = spin_up_test(initial_molecule=[hooh, hooh2])

    result = client.get_procedures({"id": ret.ids[0]})[0]
    assert result.status == "COMPLETE"


def test_service_torsiondrive_duplicates(torsiondrive_fixture):
    """Ensure that duplicates are properly caught and yield the same results without calculation"""

    spin_up_test, client = torsiondrive_fixture

    # Run the test without modifications
    id1 = spin_up_test().ids[0]

    # Augment the input for torsion drive to yield a new hash procedure hash,
    # but not a new task set
    id2 = spin_up_test(keywords={"meaningless_entry_to_change_hash": "Waffles!"}).ids[0]

    assert id1 != id2
    procedures = client.get_procedures({"id": [id1, id2]})
    assert len(procedures) == 2  # Make sure only 2 procedures are yielded

    base_run, duplicate_run = procedures
    assert base_run.optimization_history == duplicate_run.optimization_history


def test_service_iterate_error(torsiondrive_fixture):
    """Ensure errors are caught and logged when iterating serivces"""

    spin_up_test, client = torsiondrive_fixture

    # Run the test without modifications
    ret = spin_up_test(keywords={"dihedrals": [[0, 1, 2, 50]]})

    status = client.check_services({"procedure_id": ret.ids[0]})
    assert len(status) == 1

    assert status[0]["status"] == "ERROR"
    assert "Service Build" in status[0]["error_message"]


def test_service_torsiondrive_compute_error(torsiondrive_fixture):
    """Ensure errors are caught and logged when iterating serivces"""

    spin_up_test, client = torsiondrive_fixture

    # Run the test without modifications
    ret = spin_up_test(qc_spec={"method": "waffles_crasher"})

    status = client.check_services({"procedure_id": ret.ids[0]})
    assert len(status) == 1

    assert status[0]["status"] == "ERROR"
    assert "All tasks" in status[0]["error_message"]


@using_geometric
@using_rdkit
def test_service_gridoptimization_single_opt(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server)

    # Add a HOOH
    hooh = portal.data.get_molecule("hooh.json")
    initial_distance = hooh.measure([1, 2])
    mol_ret = client.add_molecules([hooh])

    # Options
    service = GridOptimizationInput(**{
        "keywords": {
            "preoptimization":
            True,
            "scans": [{
                "type": "distance",
                "indices": [1, 2],
                "steps": [-0.1, 0.0],
                "step_type": "relative"
            }, {
                "type": "dihedral",
                "indices": [0, 1, 2, 3],
                "steps": [-90, 0],
                "step_type": "absolute"
            }]
        },
        "optimization_spec": {
            "program": "geometric",
            "keywords": {
                "coordsys": "tric",
            }
        },
        "qc_spec": {
            "driver": "gradient",
            "method": "UFF",
            "basis": "",
            "keywords": None,
            "program": "rdkit",
        },
        "initial_molecule": mol_ret[0],
    })

    ret = client.add_service([service])
    fractal_compute_server.await_services()
    assert len(fractal_compute_server.list_current_tasks()) == 0

    result = client.get_procedures({"id": ret.ids[0]})[0]

    assert result.starting_grid == (1, 0)
    assert pytest.approx(result.final_energies((0, 0)), abs=1.e-4) == 0.0010044105443485617
    assert pytest.approx(result.final_energies((1, 1)), abs=1.e-4) == 0.0026440964897817623

    assert result.starting_molecule != result.initial_molecule

    # Check initial vs startin molecule
    assert result.initial_molecule == mol_ret[0]
    starting_mol = client.get_molecules([result.starting_molecule])[0]
    assert pytest.approx(starting_mol.measure([1, 2])) != initial_distance
    assert pytest.approx(starting_mol.measure([1, 2])) == 2.488686479260597


@using_geometric
@using_rdkit
def test_service_gridoptimization_single_noopt(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server)

    # Add a HOOH
    hooh = portal.data.get_molecule("hooh.json")
    initial_distance = hooh.measure([1, 2])

    # Options
    service = GridOptimizationInput(**{
        "keywords": {
            "preoptimization": False,
            "scans": [{
                "type": "distance",
                "indices": [1, 2],
                "steps": [-0.1, 0.0],
                "step_type": "relative"
            }]
        },
        "optimization_spec": {
            "program": "geometric",
            "keywords": {
                "coordsys": "tric",
            }
        },
        "qc_spec": {
            "driver": "gradient",
            "method": "UFF",
            "basis": "",
            "keywords": None,
            "program": "rdkit",
        },
        "initial_molecule": hooh,
    })

    ret = client.add_service([service])
    fractal_compute_server.await_services()
    assert len(fractal_compute_server.list_current_tasks()) == 0

    result = client.get_procedures({"id": ret.ids[0]})[0]

    assert result.starting_grid == (1, )
    assert pytest.approx(result.final_energies((0, )), abs=1.e-4) == 0.00032145876568280524

    assert result.starting_molecule == result.initial_molecule

    # Check initial vs startin molecule
    assert result.initial_molecule == result.starting_molecule

    mol = client.get_molecules([result.starting_molecule])[0]
    assert pytest.approx(mol.measure([1, 2])) == initial_distance