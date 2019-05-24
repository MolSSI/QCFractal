"""
Tests the server compute capabilities.
"""

import pytest

import qcfractal.interface as ptl
from qcfractal import testing
from qcfractal.testing import fractal_compute_server, reset_server_database, using_psi4, using_rdkit

bad_id1 = "000000000000000000000000"
bad_id2 = "000000000000000000000001"


def get_manager(fractal_compute_server):

    manager = fractal_compute_server.storage.get_managers()['data']
    if len(manager) == 0:
        fractal_compute_server.storage.manager_update('test manager')
        return 'test manager'
    else:
        return manager[0]['name']

@pytest.mark.parametrize("data", [
    pytest.param(("psi4", "HF", "sto-3g"), id="psi4", marks=using_psi4),
    pytest.param(("rdkit", "UFF", None), id="rdkit", marks=using_rdkit)
])
def test_task_molecule_no_orientation(data, fractal_compute_server):
    """
    Molecule orientation should not change on compute
    """

    # Reset database each run
    reset_server_database(fractal_compute_server)

    client = ptl.FractalClient(fractal_compute_server)

    mol = ptl.Molecule(symbols=["H", "H"], geometry=[0, 0, 0, 0, 5, 0], connectivity=[(0, 1, 1)])

    mol_id = client.add_molecules([mol])[0]

    program, method, basis = data
    ret = client.add_compute(program, method, basis, "energy", None, [mol_id])
    assert "nsubmitted" in str(ret)

    # Manually handle the compute
    fractal_compute_server.await_results()

    # Check for the single result
    ret = client.query_results(id=ret.submitted)
    assert len(ret) == 1
    assert ret[0].status == "COMPLETE"
    assert ret[0].molecule == mol_id

    # Make sure no other molecule was added
    ret = client.query_molecules(molecular_formula=["H2"])
    assert len(ret) == 1
    assert ret[0].id == mol_id


@testing.using_rdkit
def test_task_error(fractal_compute_server):
    client = ptl.FractalClient(fractal_compute_server)

    mol = ptl.models.Molecule(**{"geometry": [0, 0, 0], "symbols": ["He"]})
    # Cookiemonster is an invalid method
    ret = client.add_compute("rdkit", "cookiemonster", "", "energy", None, [mol])

    # Manually handle the compute
    fractal_compute_server.await_results()

    # Check for error
    results = client.query_results(id=ret.submitted)
    assert len(results) == 1
    assert results[0].status == "ERROR"

    assert "connectivity" in results[0].get_error().error_message

    # Check manager
    m = fractal_compute_server.storage.get_managers()["data"]
    assert len(m) == 1
    assert m[0]["failures"] > 0
    assert m[0]["completed"] > 0


@testing.using_rdkit
def test_queue_error(fractal_compute_server):
    reset_server_database(fractal_compute_server)

    client = ptl.FractalClient(fractal_compute_server)

    hooh = ptl.data.get_molecule("hooh.json").json_dict()
    del hooh["connectivity"]

    compute_ret = client.add_compute("rdkit", "UFF", "", "energy", None, hooh)

    # Pull out a special iteration on the queue manager
    fractal_compute_server.update_tasks()
    assert len(fractal_compute_server.list_current_tasks()) == 1

    fractal_compute_server.await_results()
    assert len(fractal_compute_server.list_current_tasks()) == 0

    # Pull from database, raw JSON
    db = fractal_compute_server.objects["storage_socket"]
    queue_ret = db.get_queue(status="ERROR")["data"]
    result = db.get_results(id=compute_ret.ids)['data'][0]

    assert len(queue_ret) == 1
    # TODO: task.error is not used anymore
    # assert "connectivity graph" in queue_ret[0].error.error_message
    assert result['status'] == 'ERROR'

    # Force a complete mark and test
    fractal_compute_server.objects["storage_socket"].queue_mark_complete([queue_ret[0].id])
    result = db.get_results(id=compute_ret.ids)['data'][0]
    assert result['status'] == 'COMPLETE'


@testing.using_rdkit
def test_queue_duplicate_compute(fractal_compute_server):
    reset_server_database(fractal_compute_server)

    client = ptl.FractalClient(fractal_compute_server)

    hooh = ptl.data.get_molecule("hooh.json").json_dict()
    mol_ret = client.add_molecules([hooh])

    ret = client.add_compute("rdkit", "UFF", "", "energy", None, mol_ret)
    assert len(ret.ids) == 1
    assert len(ret.existing) == 0

    # Wait for the compute to execute
    fractal_compute_server.await_results()

    db = fractal_compute_server.objects["storage_socket"]

    # Should catch duplicates both ways
    ret = client.add_compute("RDKIT", "uff", None, "energy", None, mol_ret)
    assert len(ret.ids) == 1
    assert len(ret.existing) == 1

    ret = client.add_compute("rdkit", "uFf", "", "energy", None, mol_ret)
    assert len(ret.ids) == 1
    assert len(ret.existing) == 1

    # Multiple queries
    assert len(client.query_results(program="RDKIT")) == 1
    assert len(client.query_results(program="RDKit")) == 1

    assert len(client.query_results(method="UFF")) == 1
    assert len(client.query_results(method="uff")) == 1

    assert len(client.query_results(basis=None)) == 1
    assert len(client.query_results(basis="")) == 1

    assert len(client.query_results(keywords=None)) == 1


@testing.using_rdkit
def test_queue_compute_mixed_molecule(fractal_compute_server):

    client = ptl.FractalClient(fractal_compute_server)

    mol1 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 2.1")
    mol_ret = client.add_molecules([mol1])

    mol2 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 2.2")

    ret = client.add_compute("RDKIT", "UFF", "", "energy", None, [mol1, mol2, bad_id1], full_return=True)
    assert len(ret.data.ids) == 3
    assert ret.data.ids[2] is None
    assert len(ret.data.submitted) == 2
    assert len(ret.data.existing) == 0

    # Pull out fireworks launchpad and queue nanny
    fractal_compute_server.await_results()

    db = fractal_compute_server.objects["storage_socket"]

    ret = client.add_compute("rdkit", "UFF", "", "energy", None, [mol_ret[0], bad_id2])
    assert len(ret.ids) == 2
    assert ret.ids[1] is None
    assert len(ret.submitted) == 0
    assert len(ret.existing) == 1


@testing.using_rdkit
@testing.using_geometric
def test_queue_duplicate_procedure(fractal_compute_server):

    client = ptl.FractalClient(fractal_compute_server)

    hooh = ptl.data.get_molecule("hooh.json").json_dict()
    mol_ret = client.add_molecules([hooh])

    geometric_options = {
        "keywords": None,
        "qc_spec": {
            "driver": "gradient",
            "method": "UFF",
            "basis": "",
            "keywords": None,
            "program": "rdkit"
        },
    }

    ret = client.add_procedure("optimization", "geometric", geometric_options, [mol_ret[0], bad_id1])
    assert len(ret.ids) == 2
    assert ret.ids[1] is None
    assert len(ret.submitted) == 1
    assert len(ret.existing) == 0

    # Pull out fireworks launchpad and queue nanny
    fractal_compute_server.await_results()

    db = fractal_compute_server.objects["storage_socket"]

    ret2 = client.add_procedure("optimization", "geometric", geometric_options, [bad_id1, hooh])
    assert len(ret2.ids) == 2
    assert ret2.ids[0] is None
    assert len(ret2.submitted) == 0
    assert len(ret2.existing) == 1

    assert ret.ids[0] == ret2.ids[1]


def test_queue_bad_compute_method(fractal_compute_server):

    client = ptl.FractalClient(fractal_compute_server)

    mol1 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 2.1")

    with pytest.raises(IOError) as exc:
        ret = client.add_compute("badprogram", "UFF", "", "energy", None, [mol1], full_return=True)

    assert 'not avail' in str(exc.value)


def test_queue_bad_procedure_method(fractal_compute_server):

    client = ptl.FractalClient(fractal_compute_server)
    mol1 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 2.1")

    geometric_options = {
        "keywords": None,
        "qc_spec": {
            "driver": "gradient",
            "method": "UFF",
            "basis": "",
            "keywords": None,
            "program": "rdkit"
        },
    }

    # Test bad procedure
    with pytest.raises(IOError) as exc:
        ret = client.add_procedure("optimization", "badproc", geometric_options, [mol1])

    assert 'not avail' in str(exc.value)

    # Test procedure class
    with pytest.raises(IOError) as exc:
        ret = client.add_procedure("badprocedure", "geometric", geometric_options, [mol1])

    assert 'Unknown procedure' in str(exc.value)

    # Test bad program
    with pytest.raises(IOError) as exc:
        geometric_options["qc_spec"]["program"] = "badqc"
        ret = client.add_procedure("optimization", "geometric", geometric_options, [mol1])

    assert 'not avail' in str(exc.value)
    assert 'badqc' in str(exc.value)


def test_queue_ordering_time(fractal_compute_server):
    reset_server_database(fractal_compute_server)

    client = ptl.FractalClient(fractal_compute_server)

    mol1 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 1.1")
    mol2 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 2.2")

    ret1 = client.add_compute("RDKIT", "UFF", "", "energy", None, mol1).ids[0]
    ret2 = client.add_compute("RDKIT", "UFF", "", "energy", None, mol2).ids[0]

    manager = get_manager(fractal_compute_server)

    assert len(fractal_compute_server.storage.queue_get_next(manager, [], [], limit=1)) == 0

    queue_id1 = fractal_compute_server.storage.queue_get_next(manager, ["rdkit"], [], limit=1)[0].base_result.id
    queue_id2 = fractal_compute_server.storage.queue_get_next(manager, ["rdkit"], [], limit=1)[0].base_result.id

    assert queue_id1 == ret1
    assert queue_id2 == ret2


def test_queue_ordering_priority(fractal_compute_server):
    reset_server_database(fractal_compute_server)

    client = ptl.FractalClient(fractal_compute_server)

    mol1 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 1.1")
    mol2 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 2.2")
    mol3 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 3.3")

    ret1 = client.add_compute("rdkit", "uff", "", "energy", None, mol1).ids[0]
    ret2 = client.add_compute("RDKIT", "UFF", "", "energy", None, mol2, priority="high").ids[0]
    ret3 = client.add_compute("RDKIT", "UFF", "", "energy", None, mol3, priority="HIGH").ids[0]

    manager = get_manager(fractal_compute_server)

    queue_id1 = fractal_compute_server.storage.queue_get_next(manager, ["rdkit"], [], limit=1)[0].base_result.id
    queue_id2 = fractal_compute_server.storage.queue_get_next(manager, ["RDkit"], [], limit=1)[0].base_result.id
    queue_id3 = fractal_compute_server.storage.queue_get_next(manager, ["RDKIT"], [], limit=1)[0].base_result.id

    assert queue_id1 == ret2
    assert queue_id2 == ret3
    assert queue_id3 == ret1


def test_queue_order_procedure_priority(fractal_compute_server):
    reset_server_database(fractal_compute_server)

    client = ptl.FractalClient(fractal_compute_server)

    geometric_options = {
        "keywords": None,
        "qc_spec": {
            "driver": "gradient",
            "method": "UFF",
            "basis": "",
            "keywords": None,
            "program": "rdkit"
        },
    }

    mol1 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 1.1")
    mol2 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 2.2")
    mol3 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 3.3")

    ret1 = client.add_procedure("optimization", "geometric", geometric_options, [mol1]).ids[0]
    ret2 = client.add_procedure("OPTIMIZATION", "geometric", geometric_options, [mol2], priority="high").ids[0]
    ret3 = client.add_procedure("OPTimization", "GEOmetric", geometric_options, [mol3], priority="HIGH").ids[0]

    manager = get_manager(fractal_compute_server)

    assert len(fractal_compute_server.storage.queue_get_next(manager, ["rdkit"], [], limit=1)) == 0
    assert len(fractal_compute_server.storage.queue_get_next(manager, ["rdkit"], ["geom"], limit=1)) == 0
    assert len(fractal_compute_server.storage.queue_get_next(manager, ["prog1"], ["geometric"], limit=1)) == 0

    queue_id1 = fractal_compute_server.storage.queue_get_next(
        manager, ["rdkit"], ["geometric"], limit=1)[0].base_result.id
    queue_id2 = fractal_compute_server.storage.queue_get_next(
        manager, ["RDKIT"], ["geometric"], limit=1)[0].base_result.id
    queue_id3 = fractal_compute_server.storage.queue_get_next(
        manager, ["rdkit"], ["GEOMETRIC"], limit=1)[0].base_result.id

    assert queue_id1 == ret2
    assert queue_id2 == ret3
    assert queue_id3 == ret1
