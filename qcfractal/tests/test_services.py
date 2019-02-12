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
    mol_ret = client.add_molecules({"hooh": hooh})

    # Geometric options
    torsiondrive_options = {
        "initial_molecule": mol_ret["hooh"],
        "torsiondrive_meta": {
            "dihedrals": [[0, 1, 2, 3]],
            "grid_spacing": [90]
        },
        "optimization_meta": {
            "program": "geometric",
            "coordsys": "tric",
        },
        "qc_meta": {
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
        ret = client.add_service(inp, return_full=True)

        if ret.meta.n_inserted:  # In case test already submitted
            compute_key = ret.data.hash_index
            status = client.check_services({"hash_index": compute_key}, return_full=True)
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
    result = client.get_procedures({"hash_index": ret.hash_index})[0]
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

    result = client.get_procedures({"hash_index": ret.hash_index})[0]
    assert result.success


def test_service_torsiondrive_duplicates(torsiondrive_fixture):
    """Ensure that duplicates are properly caught and yield the same results without calculation"""

    spin_up_test, client = torsiondrive_fixture

    # Run the test without modifications
    hash_index1 = spin_up_test().hash_index

    # Augment the input for torsion drive to yield a new hash procedure hash,
    # but not a new task set
    hash_index2 = spin_up_test(torsiondrive_meta={"meaningless_entry_to_change_hash": "Waffles!"}).hash_index

    assert hash_index1 != hash_index2
    procedures = client.get_procedures({"hash_index": [hash_index1, hash_index2]})
    assert len(procedures) == 2  # Make sure only 2 procedures are yielded

    base_run, duplicate_run = procedures
    assert base_run.optimization_history == duplicate_run.optimization_history


def test_service_iterate_error(torsiondrive_fixture):
    """Ensure errors are caught and logged when iterating serivces"""

    spin_up_test, client = torsiondrive_fixture

    # Run the test without modifications
    ret = spin_up_test(torsiondrive_meta={"dihedrals": [[0, 1, 2, 50]]})

    status = client.check_services({"hash_index": ret.hash_index})
    assert len(status) == 1

    assert status[0]["status"] == "ERROR"
    assert "Service Build" in status[0]["error_message"]


def test_service_torsiondrive_compute_error(torsiondrive_fixture):
    """Ensure errors are caught and logged when iterating serivces"""

    spin_up_test, client = torsiondrive_fixture

    # Run the test without modifications
    ret = spin_up_test(qc_meta={"method": "waffles_crasher"})

    status = client.check_services({"hash_index": ret.hash_index})
    assert len(status) == 1

    assert status[0]["status"] == "ERROR"
    assert "All tasks" in status[0]["error_message"]


@using_geometric
@using_rdkit
def test_service_gridoptimization_single(fractal_compute_server):

    client = portal.FractalClient(fractal_compute_server)

    # Add a HOOH
    hooh = portal.data.get_molecule("hooh.json")
    mol_ret = client.add_molecules({"hooh": hooh})

    # Options
    service = GridOptimizationInput(**{
        "gridoptimization_meta": {
            "starting_grid":
            "relative",
            "scans": [{
                "type": "distance",
                "indices": [1, 2],
                "steps": [2.5, 2.6]
            }, {
                "type": "dihedral",
                "indices": [0, 1, 2, 3],
                "steps": [90, 180]
            }]
        },
        "optimization_meta": {
            "program": "geometric",
            "coordsys": "tric",
        },
        "qc_meta": {
            "driver": "gradient",
            "method": "UFF",
            "basis": "",
            "keywords": None,
            "program": "rdkit",
        },
        "initial_molecule": mol_ret["hooh"],
    })

    ret = client.add_service(service)
    fractal_compute_server.await_services()
    assert len(fractal_compute_server.list_current_tasks()) == 0

    result = client.get_procedures({"procedure": "gridoptimization"})[0]

    assert result.starting_grid == (1, 0)
    assert pytest.approx(result.final_energies((0, 0)), abs=1.e-4) == 0.4115125808975514
    assert pytest.approx(result.final_energies((1, 1)), abs=1.e-4) == 0.4867717471566498
