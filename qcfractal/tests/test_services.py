"""
Tests the on-node procedures compute capabilities.
"""

import copy

import pytest

import qcfractal.interface as ptl
from qcfractal.interface.models import GridOptimizationInput, TorsionDriveInput
from qcfractal.testing import fractal_compute_server, recursive_dict_merge, using_geometric, using_rdkit


@pytest.fixture(scope="module")
def torsiondrive_fixture(fractal_compute_server):

    # Cannot use this fixture without these services. Also cannot use `mark` and `fixture` decorators
    pytest.importorskip("torsiondrive")
    pytest.importorskip("geometric")
    pytest.importorskip("rdkit")

    client = ptl.FractalClient(fractal_compute_server)

    # Add a HOOH
    hooh = ptl.data.get_molecule("hooh.json")
    mol_ret = client.add_molecules([hooh])

    # Geometric options
    torsiondrive_options = {
        "initial_molecule": mol_ret[0],
        "keywords": {"dihedrals": [[0, 1, 2, 3]], "grid_spacing": [90]},
        "optimization_spec": {
            "program": "geometric",
            "keywords": {"coordsys": "tric"},
            "protocols": {"trajectory": "initial_and_final"},
        },
        "qc_spec": {"driver": "gradient", "method": "UFF", "basis": "", "keywords": None, "program": "rdkit"},
    }  # yapf: disable

    def spin_up_test(**keyword_augments):
        run_service = keyword_augments.pop("run_service", True)

        instance_options = copy.deepcopy(torsiondrive_options)
        recursive_dict_merge(instance_options, keyword_augments)

        inp = TorsionDriveInput(**instance_options)
        ret = client.add_service([inp], full_return=True)

        if ret.meta.n_inserted:  # In case test already submitted
            compute_key = ret.data.ids[0]
            service = client.query_services(procedure_id=compute_key)[0]
            assert "WAITING" in service["status"]

        if run_service:
            fractal_compute_server.await_services()
            assert len(fractal_compute_server.list_current_tasks()) == 0

        return ret.data

    yield spin_up_test, client


def test_service_torsiondrive_service_incomplete(fractal_compute_server, torsiondrive_fixture):
    hooh = ptl.data.get_molecule("hooh.json")
    hooh.geometry[0] += 0.00031

    spin_up_test, client = torsiondrive_fixture
    ret = spin_up_test(run_service=False)

    # Check the blank
    result = client.query_procedures(id=ret.ids)[0]
    assert len(result.final_energy_dict) == 0
    assert len(result.optimization_history) == 0
    assert result.status == "INCOMPLETE"

    # Update the service, but no compute
    fractal_compute_server.update_services()
    result = client.query_procedures(id=ret.ids)[0]
    status = result.detailed_status()
    assert result.status == "RUNNING"
    assert status["incomplete_tasks"] == 1

    fractal_compute_server.await_results()

    # Take a compute step
    fractal_compute_server.await_services(max_iter=1)
    result = client.query_procedures(id=ret.ids)[0]
    status = result.detailed_status()
    assert status["total_points"] == 4
    assert status["computed_points"] == 3
    assert status["complete_tasks"] >= 3
    assert status["incomplete_tasks"] == 0
    assert len(result.final_energy_dict) == 1  # One complete
    assert len(result.optimization_history) == 3  # Three spawned
    assert result.minimum_positions["[-90]"] == 0
    assert result.status == "RUNNING"

    # Repeat compute step checking for updates
    fractal_compute_server.await_services(max_iter=1)
    result = client.query_procedures(id=ret.ids)[0]
    assert len(result.final_energy_dict) == 3
    assert len(result.optimization_history) == 4
    assert result.minimum_positions["[-90]"] == 0
    assert result.status == "RUNNING"

    # Finalize
    fractal_compute_server.await_services(max_iter=6)
    result = client.query_procedures(id=ret.ids)[0]
    assert len(result.final_energy_dict) == 4
    assert len(result.optimization_history) == 4
    assert result.minimum_positions["[-90]"] == 2
    assert result.status == "COMPLETE"


def test_service_manipulation(torsiondrive_fixture):

    spin_up_test, client = torsiondrive_fixture

    hooh = ptl.data.get_molecule("hooh.json")
    hooh.geometry[0] += 3.1

    ret = spin_up_test(run_service=False, initial_molecule=[hooh])

    service = client.query_services(procedure_id=ret.ids)[0]
    assert service["status"] == "WAITING"

    client.modify_services("restart", id=service["id"])

    service = client.query_services(procedure_id=ret.ids)[0]
    assert service["status"] == "RUNNING"


def test_service_torsiondrive_single(torsiondrive_fixture):
    """"Tests torsiondrive pathway and checks the result """

    spin_up_test, client = torsiondrive_fixture

    ret = spin_up_test()

    # Get a TorsionDriveORM result and check data
    result = client.query_procedures(id=ret.ids)[0]
    assert result.status == "COMPLETE"
    assert isinstance(str(result), str)  # Check that repr runs

    assert pytest.approx(0.002597541340221565, abs=1e-6) == result.get_final_energies(0)
    assert pytest.approx(0.000156553761859276, abs=1e-6) == result.get_final_energies(90)
    assert pytest.approx(0.000156553761859271, abs=1e-6) == result.get_final_energies(-90)
    assert pytest.approx(0.000753492556057886, abs=1e-6) == result.get_final_energies(180)

    assert hasattr(result.get_final_molecules()[(-90,)], "symbols")

    opt = result.get_history(0)[0]
    assert opt.protocols.trajectory == "initial_and_final"
    assert len(opt.trajectory) == 2


@pytest.mark.slow
def test_service_torsiondrive_multi_single(torsiondrive_fixture):
    spin_up_test, client = torsiondrive_fixture

    hooh = ptl.data.get_molecule("hooh.json")
    hooh2 = hooh.copy(deep=True)
    hooh2.geometry[0] += 0.0004

    ret = spin_up_test(initial_molecule=[hooh, hooh2])

    result = client.query_procedures(id=ret.ids)[0]
    assert result.status == "COMPLETE"


def test_service_torsiondrive_duplicates(torsiondrive_fixture):
    """Ensure that duplicates are properly caught and yield the same results without calculation"""

    spin_up_test, client = torsiondrive_fixture

    # Run the test without modifications
    id1 = spin_up_test().ids[0]

    # Augment the input for torsion drive to yield a new hash procedure hash,
    # but not a new task set
    id2 = spin_up_test(keywords={"energy_upper_limit": 1000}).ids[0]

    assert id1 != id2
    procedures = client.query_procedures(id=[id1, id2])
    assert len(procedures) == 2  # Make sure only 2 procedures are yielded

    base_run, duplicate_run = procedures
    assert base_run.optimization_history == duplicate_run.optimization_history


@pytest.mark.slow
def test_service_torsiondrive_option_dihedral_ranges(torsiondrive_fixture):
    """"Tests torsiondrive with dihedral_ranges optional keyword """

    spin_up_test, client = torsiondrive_fixture
    ret = spin_up_test(keywords={"grid_spacing": [30], "dihedral_ranges": [[-150, -60]]})

    result = client.query_procedures(id=ret.ids)[0]
    assert result.status == "COMPLETE"

    # The dihedral range should be limited to -150, -90, -60
    final_energies = result.get_final_energies()
    assert set(final_energies.keys()) == {(-150,), (-120,), (-90,), (-60,)}
    assert pytest.approx(0.0005683235570009067, abs=1e-6) == final_energies[(-150,)]
    assert pytest.approx(0.0002170694130912583, abs=1e-6) == final_energies[(-120,)]
    assert pytest.approx(0.0001565537585121726, abs=1e-6) == final_energies[(-90,)]
    assert pytest.approx(0.0007991274441437338, abs=1e-6) == final_energies[(-60,)]

    # Check final molecules
    final_molecules = result.get_final_molecules()
    assert set(final_molecules.keys()) == {(-150,), (-120,), (-90,), (-60,)}
    assert all(hasattr(m, "symbols") for m in final_molecules.values())


@pytest.mark.slow
def test_service_torsiondrive_option_energy_decrease_thresh(torsiondrive_fixture):
    """"Tests torsiondrive with energy_decrease_thresh optional keyword"""

    spin_up_test, client = torsiondrive_fixture
    ret = spin_up_test(keywords={"grid_spacing": [90], "energy_decrease_thresh": 3e-5})

    result = client.query_procedures(id=ret.ids)[0]
    assert result.status == "COMPLETE"

    # the final energies are the same as the default setting, because this molecule is too simple
    final_energies = result.get_final_energies()
    assert set(final_energies.keys()) == {(-90,), (-0,), (90,), (180,)}
    assert pytest.approx(0.002597541340221565, abs=1e-6) == final_energies[(0,)]
    assert pytest.approx(0.000156553761859276, abs=1e-6) == final_energies[(90,)]
    assert pytest.approx(0.000156553761859271, abs=1e-6) == final_energies[(-90,)]
    assert pytest.approx(0.000753492556057886, abs=1e-6) == final_energies[(180,)]


@pytest.mark.slow
def test_service_torsiondrive_option_energy_upper_limit(torsiondrive_fixture):
    """"Tests torsiondrive with energy_upper_limit optional keyword"""

    spin_up_test, client = torsiondrive_fixture
    ret = spin_up_test(keywords={"grid_spacing": [30], "energy_upper_limit": 1e-4})

    result = client.query_procedures(id=ret.ids)[0]
    assert result.status == "COMPLETE"

    # The energy_upper_limit should limit the range of the scan
    final_energies = result.get_final_energies()
    assert set(final_energies.keys()) == {(-150,), (-120,), (-90,), (-60,)}
    assert pytest.approx(0.0005683235570009067, abs=1e-6) == final_energies[(-150,)]
    assert pytest.approx(0.0002170694130912583, abs=1e-6) == final_energies[(-120,)]
    assert pytest.approx(0.0001565537585121726, abs=1e-6) == final_energies[(-90,)]
    assert pytest.approx(0.0007991274441437338, abs=1e-6) == final_energies[(-60,)]


@pytest.mark.slow
def test_service_torsiondrive_option_extra_constraints(torsiondrive_fixture):
    """"Tests torsiondrive with extra_constraints in optimization_spec """

    spin_up_test, client = torsiondrive_fixture
    ret = spin_up_test(
        optimization_spec={
            "program": "geometric",
            "keywords": {"coordsys": "tric", "constraints": {"freeze": [{"type": "xyz", "indices": [0]}]}},
        }
    )

    result = client.query_procedures(id=ret.ids)[0]
    assert result.status == "COMPLETE"

    # The final energies are the same as the default setting, because this molecule is too simple
    final_energies = result.get_final_energies()
    assert set(final_energies.keys()) == {(-90,), (-0,), (90,), (180,)}
    assert pytest.approx(0.002597541340221565, abs=1e-6) == final_energies[(0,)]
    assert pytest.approx(0.000156553761859276, abs=1e-6) == final_energies[(90,)]
    assert pytest.approx(0.000156553761859271, abs=1e-6) == final_energies[(-90,)]
    assert pytest.approx(0.000753492556057886, abs=1e-6) == final_energies[(180,)]

    # Check final molecules
    hooh = ptl.data.get_molecule("hooh.json")
    final_molecules = result.get_final_molecules()
    for m in final_molecules.values():
        # the coordinate of the first atom should be "frozen"
        assert pytest.approx(m.geometry[0], abs=1e-3) == hooh.geometry[0]


def test_service_iterate_error(torsiondrive_fixture):
    """Ensure errors are caught and logged when iterating serivces"""

    spin_up_test, client = torsiondrive_fixture

    # Run the test without modifications
    ret = spin_up_test(keywords={"dihedrals": [[0, 1, 2, 50]]})

    status = client.query_services(procedure_id=ret.ids)
    assert len(status) == 1

    assert status[0]["status"] == "ERROR"
    assert "Service Build" in status[0]["error"]["error_message"]

    # Test that the error is propagated to the procedure
    proc_status = client.query_procedures(ret.ids)
    assert len(proc_status) == 1
    assert proc_status[0].status == "ERROR"
    assert "Service Build" in proc_status[0].get_error().error_message


def test_service_torsiondrive_compute_error(torsiondrive_fixture):
    """Ensure errors are caught and logged when computing serivces"""

    spin_up_test, client = torsiondrive_fixture

    # Run the test without modifications
    ret = spin_up_test(qc_spec={"method": "waffles_crasher"})

    status = client.query_services(procedure_id=ret.ids)
    assert len(status) == 1

    assert status[0]["status"] == "ERROR"
    assert "All tasks" in status[0]["error"]["error_message"]


def test_service_torsiondrive_visualization(torsiondrive_fixture):
    """Test the visualization function for the 1-D case"""

    spin_up_test, client = torsiondrive_fixture
    ret = spin_up_test()

    # Get a TorsionDriveORM result and check data
    result = client.query_procedures(id=ret.ids)[0]
    assert result.status == "COMPLETE"

    result.visualize()


def test_service_torsiondrive_get_final_results(torsiondrive_fixture):
    """Test the get_final_results function for the 1-D case"""

    spin_up_test, client = torsiondrive_fixture
    ret = spin_up_test()

    # Get a TorsionDriveORM result and check data
    result = client.query_procedures(id=ret.ids)[0]
    assert result.status == "COMPLETE"

    final_result_records = result.get_final_results()
    assert set(final_result_records.keys()) == {(-90,), (-0,), (90,), (180,)}

    stdout = result.get_stdout()
    assert "All optimizations converged at lowest energy" in stdout


@using_geometric
@using_rdkit
def test_service_gridoptimization_single_opt(fractal_compute_server):

    client = ptl.FractalClient(fractal_compute_server)

    # Add a HOOH
    hooh = ptl.data.get_molecule("hooh.json")
    initial_distance = hooh.measure([1, 2])
    mol_ret = client.add_molecules([hooh])

    # Options
    service = GridOptimizationInput(
        **{
            "keywords": {
                "preoptimization": True,
                "scans": [
                    {"type": "distance", "indices": [1, 2], "steps": [-0.1, 0.0], "step_type": "relative"},
                    {"type": "dihedral", "indices": [0, 1, 2, 3], "steps": [-90, 0], "step_type": "absolute"},
                ],
            },
            "optimization_spec": {"program": "geometric", "keywords": {"coordsys": "tric"}},
            "qc_spec": {"driver": "gradient", "method": "UFF", "basis": "", "keywords": None, "program": "rdkit"},
            "initial_molecule": mol_ret[0],
        }
    )  # yapf: disable

    ret = client.add_service([service], tag="gridopt", priority="low")

    fractal_compute_server.await_services(max_iter=1)
    result = client.query_procedures(id=ret.ids)[0]
    assert result.grid_optimizations.keys() == {'"preoptimization"'}
    assert result.status == "RUNNING"

    fractal_compute_server.await_services(max_iter=1)
    result = client.query_procedures(id=ret.ids)[0]
    status = result.detailed_status()
    assert status["total_points"] == 5
    assert status["complete_tasks"] == 2
    assert result.grid_optimizations.keys() == {'"preoptimization"', "[1, 0]"}
    assert result.status == "RUNNING"

    fractal_compute_server.await_services(max_iter=1)
    result = client.query_procedures(id=ret.ids)[0]
    assert result.grid_optimizations.keys() == {'"preoptimization"', "[1, 0]", "[0, 0]", "[1, 1]"}
    assert result.status == "RUNNING"

    fractal_compute_server.await_services(max_iter=6)
    result = client.query_procedures(id=ret.ids)[0]
    status = result.detailed_status()
    assert status["complete_tasks"] == 5
    assert result.status == "COMPLETE"

    assert result.starting_grid == (1, 0)
    assert pytest.approx(result.get_final_energies((0, 0)), abs=1.0e-4) == 0.0010044105443485617
    assert pytest.approx(result.get_final_energies((1, 1)), abs=1.0e-4) == 0.0026440964897817623

    assert result.starting_molecule != result.initial_molecule

    # Check initial vs starting molecule
    assert result.initial_molecule == mol_ret[0]
    starting_mol = client.query_molecules(id=result.starting_molecule)[0]
    assert pytest.approx(starting_mol.measure([1, 2])) != initial_distance
    assert pytest.approx(starting_mol.measure([1, 2])) == 2.488686479260597

    # Check tags on individual procedures
    proc_id = result.grid_optimizations["[0, 0]"]

    # completed tasks should be deleted
    task = client.query_tasks(base_result=proc_id)
    assert not task

    # Check final ResultRecords
    final_result_records = result.get_final_results()
    assert len(final_result_records) == 5
    assert final_result_records["preoptimization"].molecule == result.starting_molecule

    # Pull the full history
    preopt = result.get_history()["preoptimization"]
    assert preopt.initial_molecule == result.initial_molecule
    assert preopt.final_molecule == result.starting_molecule


@using_geometric
@using_rdkit
def test_service_gridoptimization_single_noopt(fractal_compute_server):

    client = ptl.FractalClient(fractal_compute_server)

    # Add a HOOH
    hooh = ptl.data.get_molecule("hooh.json")
    initial_distance = hooh.measure([1, 2])

    # Options
    service = GridOptimizationInput(
        **{
            "keywords": {
                "preoptimization": False,
                "scans": [{"type": "distance", "indices": [1, 2], "steps": [-0.1, 0.0], "step_type": "relative"}],
            },
            "optimization_spec": {"program": "geometric", "keywords": {"coordsys": "tric"}},
            "qc_spec": {"driver": "gradient", "method": "UFF", "basis": "", "keywords": None, "program": "rdkit"},
            "initial_molecule": hooh,
        }
    )  # yapf: disable

    ret = client.add_service([service])
    fractal_compute_server.await_services()
    assert len(fractal_compute_server.list_current_tasks()) == 0

    result = client.query_procedures(id=ret.ids)[0]

    assert result.status == "COMPLETE"
    assert result.starting_grid == (1,)
    assert pytest.approx(result.get_final_energies((0,)), abs=1.0e-4) == 0.00032145876568280524

    assert result.starting_molecule == result.initial_molecule

    # Check initial vs startin molecule
    assert result.initial_molecule == result.starting_molecule

    mol = client.query_molecules(id=result.starting_molecule)[0]
    assert pytest.approx(mol.measure([1, 2])) == initial_distance


@pytest.mark.skip
def test_query_time(fractal_compute_server):

    client = ptl.FractalClient(fractal_compute_server)

    import time

    t = time.time()
    p = client.query_procedures(procedure="optimization")
    total = time.time() - t
    print(total)
    print(len(p))
    print("---- per optmization proc: ", total / len(p))

    import time

    t = time.time()
    p = client.query_procedures(procedure="torsiondrive")
    total = time.time() - t
    print(total)
    print(len(p))
    print("---- per Torsion proc: ", total / len(p))
