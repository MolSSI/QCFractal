"""
Tests the server compute capabilities.
"""

import pytest

import qcfractal.interface as ptl
from qcfractal import testing
from qcfractal.testing import fractal_compute_server, test_server, reset_server_database, using_psi4, using_rdkit

bad_id1 = "000000000000000000000000"
bad_id2 = "000000000000000000000001"


def get_manager_name(fractal_compute_server):

    manager = fractal_compute_server.storage.get_managers()["data"]
    if len(manager) == 0:
        fractal_compute_server.storage.manager_update("test manager")
        return "test manager"
    else:
        return manager[0]["name"]


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(("psi4", "HF", "sto-3g"), id="psi4", marks=using_psi4),
        pytest.param(("rdkit", "UFF", None), id="rdkit", marks=using_rdkit),
    ],
)
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
    mols = client.query_molecules(molecular_formula=["H2"])
    assert len(mols) == 1
    assert mols[0].id == mol_id

    # Check get_molecule
    mol = ret[0].get_molecule()
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
def test_task_client_restart(fractal_compute_server):
    client = ptl.FractalClient(fractal_compute_server)

    mol = ptl.models.Molecule(**{"geometry": [0, 0, 1], "symbols": ["He"]})
    # Cookiemonster is an invalid method
    ret = client.add_compute("rdkit", "cookiemonster", "", "energy", None, [mol])

    # Manually handle the compute
    fractal_compute_server.await_results()

    tasks = client.query_tasks(base_result=ret.submitted)[0]
    assert tasks.status == "ERROR"

    upd = client.modify_tasks("restart", ret.submitted)
    assert upd.n_updated == 1

    tasks = client.query_tasks(base_result=ret.submitted)[0]
    assert tasks.status == "WAITING"


@testing.using_rdkit
@testing.using_geometric
def test_task_regenerate(fractal_compute_server):
    client = ptl.FractalClient(fractal_compute_server)

    # Add a single computation and a geometry optimization
    # Both of these have invalid methods
    mol = ptl.models.Molecule(**{"geometry": [1, 2, 3], "symbols": ["Ne"]})
    geometric_options = {
        "keywords": None,
        "qc_spec": {"driver": "gradient", "method": "cookiemonster", "basis": "", "keywords": None, "program": "rdkit"},
    }

    ret1 = client.add_compute("rdkit", "cookiemonster", "", "energy", None, [mol])
    ret2 = client.add_procedure("optimization", "geometric", geometric_options, [mol])
    fractal_compute_server.await_results()

    base_ids = [ret1.submitted[0], ret2.submitted[0]]
    old_tasks = client.query_tasks(base_result=base_ids)

    # Regenerate, but old one exists. Should be a no-op
    upd = client.modify_tasks("regenerate", base_result=base_ids)
    assert upd.n_updated == 0
    new_tasks = client.query_tasks(base_result=base_ids)

    for old_task, new_task in zip(old_tasks, new_tasks):
        assert old_task.status == "ERROR"
        assert old_task.id == new_task.id
        assert old_task.base_result == new_task.base_result
        assert old_task.modified_on == new_task.modified_on
        assert old_task.created_on == new_task.created_on

    # Manually delete the old task
    db = fractal_compute_server.objects["storage_socket"]
    db.del_tasks([x.id for x in old_tasks])

    # Actually deleted?
    del_task = client.query_tasks(base_result=base_ids)
    assert len(del_task) == 0

    # Now regenerate
    upd = client.modify_tasks("regenerate", base_result=base_ids)
    new_tasks = client.query_tasks(base_result=base_ids)
    assert upd.n_updated == 2
    for old_task, new_task in zip(old_tasks, new_tasks):
        assert new_task.status == "WAITING"
        assert old_task.id != new_task.id  # Task ids must now be different
        assert old_task.base_result == new_task.base_result
        assert old_task.modified_on < new_task.modified_on  # New task must be newer
        assert old_task.created_on < new_task.created_on  # New task must be newer

    assert old_tasks[0].spec.args[0]["molecule"]["id"] == new_tasks[0].spec.args[0]["molecule"]["id"]
    assert (
        old_tasks[0].spec.args[0]["molecule"]["identifiers"]["molecule_hash"]
        == new_tasks[0].spec.args[0]["molecule"]["identifiers"]["molecule_hash"]
    )
    assert old_tasks[1].spec.args[0]["initial_molecule"]["id"] == new_tasks[1].spec.args[0]["initial_molecule"]["id"]
    assert (
        old_tasks[1].spec.args[0]["initial_molecule"]["identifiers"]["molecule_hash"]
        == new_tasks[1].spec.args[0]["initial_molecule"]["identifiers"]["molecule_hash"]
    )

    # The status of the result should be reset to incomplete
    res = client.query_procedures(base_ids)
    assert all(x.status == "INCOMPLETE" for x in res)


def test_task_modify(test_server):
    client = ptl.FractalClient(test_server)

    # Add a single computation
    mol = ptl.models.Molecule(**{"geometry": [0, 0, 1], "symbols": ["He"]})
    ret1 = client.add_compute("rdkit", "cookiemonster", "", "energy", None, [mol], tag="test_tag_1", priority=1)
    base_id = ret1.submitted[0]
    old_task = client.query_tasks(base_result=base_id)[0]

    assert old_task.priority == 1
    assert old_task.tag == "test_tag_1"

    # Modify the priority and tag
    upd = client.modify_tasks("modify", base_result=base_id, new_tag="test_tag_2", new_priority=0)
    assert upd.n_updated == 1
    new_task = client.query_tasks(base_result=base_id)[0]

    assert new_task.tag == "test_tag_2"
    assert new_task.priority == 0


@testing.using_rdkit
def test_queue_error(fractal_compute_server):
    reset_server_database(fractal_compute_server)

    client = ptl.FractalClient(fractal_compute_server)

    hooh = ptl.data.get_molecule("hooh.json").copy(update={"connectivity_": None})
    compute_ret = client.add_compute("rdkit", "UFF", "", "energy", None, hooh)

    # Pull out a special iteration on the queue manager
    fractal_compute_server.update_tasks()
    assert len(fractal_compute_server.list_current_tasks()) == 1

    fractal_compute_server.await_results()
    assert len(fractal_compute_server.list_current_tasks()) == 0

    # Pull from database, raw JSON
    db = fractal_compute_server.objects["storage_socket"]
    queue_ret = db.get_queue(status="ERROR")["data"]
    result = db.get_results(id=compute_ret.ids)["data"][0]

    assert len(queue_ret) == 1
    # TODO: task.error is not used anymore
    # assert "connectivity graph" in queue_ret[0].error.error_message
    assert result["status"] == "ERROR"

    # Force a complete mark and test
    fractal_compute_server.objects["storage_socket"].queue_mark_complete([queue_ret[0].id])
    result = db.get_results(id=compute_ret.ids)["data"][0]
    assert result["status"] == "COMPLETE"


@testing.using_rdkit
def test_queue_duplicate_compute(fractal_compute_server):
    reset_server_database(fractal_compute_server)

    client = ptl.FractalClient(fractal_compute_server)

    hooh = ptl.data.get_molecule("hooh.json")
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

    hooh = ptl.data.get_molecule("hooh.json")
    mol_ret = client.add_molecules([hooh])

    geometric_options = {
        "keywords": None,
        "qc_spec": {"driver": "gradient", "method": "UFF", "basis": "", "keywords": None, "program": "rdkit"},
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

    assert "not avail" in str(exc.value)


def test_queue_bad_procedure_method(fractal_compute_server):

    client = ptl.FractalClient(fractal_compute_server)
    mol1 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 2.1")

    geometric_options = {
        "keywords": None,
        "qc_spec": {"driver": "gradient", "method": "UFF", "basis": "", "keywords": None, "program": "rdkit"},
    }

    # Test bad procedure
    with pytest.raises(IOError) as exc:
        ret = client.add_procedure("optimization", "badproc", geometric_options, [mol1])

    assert "not avail" in str(exc.value)

    # Test procedure class
    with pytest.raises(IOError) as exc:
        ret = client.add_procedure("badprocedure", "geometric", geometric_options, [mol1])

    assert "Unknown procedure" in str(exc.value)

    # Test bad program
    with pytest.raises(IOError) as exc:
        geometric_options["qc_spec"]["program"] = "badqc"
        ret = client.add_procedure("optimization", "geometric", geometric_options, [mol1])

    assert "not avail" in str(exc.value)
    assert "badqc" in str(exc.value)


def test_queue_ordering_time(fractal_compute_server):
    reset_server_database(fractal_compute_server)

    client = ptl.FractalClient(fractal_compute_server)

    mol1 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 1.1")
    mol2 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 2.2")

    ret1 = client.add_compute("RDKIT", "UFF", "", "energy", None, mol1).ids[0]
    ret2 = client.add_compute("RDKIT", "UFF", "", "energy", None, mol2).ids[0]

    manager = get_manager_name(fractal_compute_server)

    assert len(fractal_compute_server.storage.queue_get_next(manager, [], [], limit=1)) == 0

    queue_id1 = fractal_compute_server.storage.queue_get_next(manager, ["rdkit"], [], limit=1)[0].base_result
    queue_id2 = fractal_compute_server.storage.queue_get_next(manager, ["rdkit"], [], limit=1)[0].base_result

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

    manager = get_manager_name(fractal_compute_server)

    queue_id1 = fractal_compute_server.storage.queue_get_next(manager, ["rdkit"], [], limit=1)[0].base_result
    queue_id2 = fractal_compute_server.storage.queue_get_next(manager, ["RDkit"], [], limit=1)[0].base_result
    queue_id3 = fractal_compute_server.storage.queue_get_next(manager, ["RDKIT"], [], limit=1)[0].base_result

    assert queue_id1 == ret2
    assert queue_id2 == ret3
    assert queue_id3 == ret1


def test_queue_order_procedure_priority(fractal_compute_server):
    reset_server_database(fractal_compute_server)

    client = ptl.FractalClient(fractal_compute_server)

    geometric_options = {
        "keywords": None,
        "qc_spec": {"driver": "gradient", "method": "UFF", "basis": "", "keywords": None, "program": "rdkit"},
    }

    mol1 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 1.1")
    mol2 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 2.2")
    mol3 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 3.3")

    ret1 = client.add_procedure("optimization", "geometric", geometric_options, [mol1]).ids[0]
    ret2 = client.add_procedure("OPTIMIZATION", "geometric", geometric_options, [mol2], priority="high").ids[0]
    ret3 = client.add_procedure("OPTimization", "GEOmetric", geometric_options, [mol3], priority="HIGH").ids[0]

    manager = get_manager_name(fractal_compute_server)

    assert len(fractal_compute_server.storage.queue_get_next(manager, ["rdkit"], [], limit=1)) == 0
    assert len(fractal_compute_server.storage.queue_get_next(manager, ["rdkit"], ["geom"], limit=1)) == 0
    assert len(fractal_compute_server.storage.queue_get_next(manager, ["prog1"], ["geometric"], limit=1)) == 0

    queue_id1 = fractal_compute_server.storage.queue_get_next(manager, ["rdkit"], ["geometric"], limit=1)[0].base_result
    queue_id2 = fractal_compute_server.storage.queue_get_next(manager, ["RDKIT"], ["geometric"], limit=1)[0].base_result
    queue_id3 = fractal_compute_server.storage.queue_get_next(manager, ["rdkit"], ["GEOMETRIC"], limit=1)[0].base_result

    assert queue_id1 == ret2
    assert queue_id2 == ret3
    assert queue_id3 == ret1


def test_queue_query_tag(fractal_compute_server):
    reset_server_database(fractal_compute_server)

    client = ptl.FractalClient(fractal_compute_server)

    mol1 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 1.1")
    mol2 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 2.2")
    mol3 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 3.3")

    ret1 = client.add_compute("rdkit", "uff", "", "energy", None, mol1).ids[0]
    ret2 = client.add_compute("RDKIT", "UFF", "", "energy", None, mol2, tag="test").ids[0]
    ret3 = client.add_compute("RDKIT", "UFF", "", "energy", None, mol3, tag="test2").ids[0]

    tasks_tag_test = client.query_tasks(tag="test")
    assert len(tasks_tag_test) == 1
    assert tasks_tag_test[0].base_result == ret2

    tasks_tag_none = client.query_tasks()
    assert len(tasks_tag_none) == 3
    assert {task.base_result for task in tasks_tag_none} == {ret1, ret2, ret3}

    tasks_tagged = client.query_tasks(tag=["test", "test2"])
    assert len(tasks_tagged) == 2
    assert {task.base_result for task in tasks_tagged} == {ret2, ret3}


def test_queue_query_manager(fractal_compute_server):
    reset_server_database(fractal_compute_server)

    client = ptl.FractalClient(fractal_compute_server)

    mol1 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 1.1")
    mol2 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 2.2")
    mol3 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 3.3")

    ret1 = client.add_compute("rdkit", "uff", "", "energy", None, mol1).ids[0]
    ret2 = client.add_compute("RDKIT", "UFF", "", "energy", None, mol2).ids[0]
    ret3 = client.add_compute("RDKIT", "UFF", "", "energy", None, mol3).ids[0]

    manager = get_manager_name(fractal_compute_server)
    fractal_compute_server.storage.queue_get_next(manager, ["rdkit"], [], limit=1)[0]
    tasks_manager = client.query_tasks(manager=manager)
    assert len(tasks_manager) == 1
    assert tasks_manager[0].base_result == ret1

    fractal_compute_server.storage.queue_get_next(manager, ["RDkit"], [], limit=1)[0]
    fractal_compute_server.storage.queue_get_next(manager, ["RDKIT"], [], limit=1)[0]
    tasks_manager = client.query_tasks(manager=manager)
    assert len(tasks_manager) == 3
