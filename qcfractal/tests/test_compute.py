"""
Tests the server compute capabilities.
"""

import pytest

import qcfractal.interface as ptl
from qcfractal.interface.models import RecordStatusEnum
from qcfractal.db_socket.socket import SQLAlchemySocket
from qcfractal.components.tasks.db_models import TaskQueueORM
from qcfractal.testing import using_psi4, using_rdkit, using_geometric

bad_id1 = "99999999998"
bad_id2 = "99999999999"


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(("psi4", "HF", "sto-3g"), id="psi4", marks=using_psi4),
        pytest.param(("rdkit", "UFF", None), id="rdkit", marks=using_rdkit),
    ],
)
def test_task_molecule_no_orientation(data, fractal_test_server):
    """
    Molecule orientation should not change on compute
    """

    client = fractal_test_server.client()
    mol = ptl.Molecule(symbols=["H", "H"], geometry=[0, 0, 0, 0, 5, 0], connectivity=[(0, 1, 1)])

    mol_id = client.add_molecules([mol])[0]

    program, method, basis = data
    ret = client.add_compute(program, method, basis, "energy", None, [mol_id])
    assert "nsubmitted" in str(ret)

    # Perform the computation
    fractal_test_server.start_compute_worker()
    fractal_test_server.await_results()

    # Check for the single result
    ret = client.query_results(id=ret.submitted)
    assert len(ret) == 1
    assert ret[0].status == RecordStatusEnum.complete
    assert ret[0].molecule == mol_id

    # Make sure no other molecule was added
    mols = client.query_molecules(molecular_formula=["H2"])
    assert len(mols) == 1

    assert mols[0].id == mol_id

    # Check get_molecule
    mol = ret[0].get_molecule()

    assert ret[0].molecule == mol.id


@using_rdkit
def test_task_error(fractal_test_server):
    client = fractal_test_server.client()

    mol = ptl.models.Molecule(**{"geometry": [0, 0, 0], "symbols": ["He"]})
    # Cookiemonster is an invalid method
    ret = client.add_compute("rdkit", "cookiemonster", "", "energy", None, [mol])

    # Perform the computation
    fractal_test_server.start_compute_worker()
    fractal_test_server.await_results()

    # Check for error
    results = client.query_results(id=ret.submitted)
    assert len(results) == 1
    assert results[0].status == RecordStatusEnum.error

    assert "connectivity" in results[0].get_error().error_message

    # Check manager
    m = client.query_managers(status=["ACTIVE", "INACTIVE"])
    assert len(m) == 1
    assert m[0]["failures"] > 0
    assert m[0]["completed"] > 0


@using_rdkit
def test_task_client_restart(fractal_test_server):
    client = fractal_test_server.client()

    mol = ptl.models.Molecule(**{"geometry": [0, 0, 1], "symbols": ["He"]})

    # Cookiemonster is an invalid method
    ret = client.add_compute("rdkit", "cookiemonster", "", "energy", None, [mol])

    # Perform the computation
    fractal_test_server.start_compute_worker()
    fractal_test_server.await_results()

    tasks = client.query_tasks(base_result=ret.submitted)[0]
    result = client.query_results(id=tasks.base_result)[0]
    assert result.status == RecordStatusEnum.error

    # Stop the compute worker
    fractal_test_server._compute_proc.stop()

    upd = client.modify_tasks("restart", ret.submitted)
    assert upd.n_updated == 1

    result = client.query_results(id=tasks.base_result)[0]
    assert result.status == RecordStatusEnum.waiting


@using_rdkit
@using_geometric
def test_task_regenerate(fractal_test_server):
    client = fractal_test_server.client()

    # Add a single computation and a geometry optimization
    # Both of these have invalid methods
    mol = ptl.models.Molecule(**{"geometry": [1, 2, 3], "symbols": ["Ne"]})
    geometric_options = {
        "keywords": None,
        "qc_spec": {"driver": "gradient", "method": "cookiemonster", "basis": "", "keywords": None, "program": "rdkit"},
    }

    ret1 = client.add_compute("rdkit", "cookiemonster", "", "energy", None, [mol])
    ret2 = client.add_procedure("optimization", "geometric", geometric_options, [mol])

    # Perform the computation
    fractal_test_server.start_compute_worker()
    fractal_test_server.await_results()

    base_ids = [ret1.submitted[0], ret2.submitted[0]]
    old_tasks = client.query_tasks(base_result=base_ids)

    # Regenerate, but old one exists. Should be a no-op
    upd = client.modify_tasks("regenerate", base_result=base_ids)
    assert upd.n_updated == 0
    new_tasks = client.query_tasks(base_result=base_ids)

    for old_task, new_task in zip(old_tasks, new_tasks):
        r = client.query_procedures(id=old_task.base_result)[0]
        assert r.status == RecordStatusEnum.error
        assert old_task.id == new_task.id
        assert old_task.base_result == new_task.base_result
        assert old_task.created_on == new_task.created_on

    # Manually delete the old task
    storage_socket = fractal_test_server.get_storage_socket()
    with storage_socket.session_scope() as session:
        session.query(TaskQueueORM).filter(TaskQueueORM.id.in_([x.id for x in old_tasks])).delete()

    # Actually deleted?
    del_task = client.query_tasks(base_result=base_ids)
    assert len(del_task) == 0

    # Now regenerate
    upd = client.modify_tasks("regenerate", base_result=base_ids)
    new_tasks = client.query_tasks(base_result=base_ids)
    assert len(new_tasks) == 2

    assert upd.n_updated == 2
    for old_task, new_task in zip(old_tasks, new_tasks):
        r = client.query_procedures(id=old_task.base_result)[0]
        assert r.status == RecordStatusEnum.waiting
        assert old_task.id != new_task.id  # Task ids must now be different
        assert old_task.base_result == new_task.base_result
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

    # The status of the result should be reset to waiting
    res = client.query_procedures(base_ids)
    assert all(x.status == RecordStatusEnum.waiting for x in res)


def test_task_modify(fractal_test_server):
    client = fractal_test_server.client()

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


@using_rdkit
def test_queue_error(fractal_test_server):
    client = fractal_test_server.client()

    hooh = ptl.data.get_molecule("hooh.json").copy(update={"connectivity_": None})
    compute_ret = client.add_compute("rdkit", "UFF", "", "energy", None, hooh)

    # Perform the computation
    fractal_test_server.start_compute_worker()
    fractal_test_server.await_results()

    # Pull from database, raw JSON
    storage_socket = SQLAlchemySocket(fractal_test_server._qcf_config)
    queue_ret = storage_socket.task.query_tasks(status=[RecordStatusEnum.error])[1]
    result = storage_socket.record.query(id=compute_ret.ids)[1][0]

    assert len(queue_ret) == 1
    assert result["status"] == RecordStatusEnum.error


@using_rdkit
def test_queue_duplicate_compute(fractal_test_server):
    client = fractal_test_server.client()

    hooh = ptl.data.get_molecule("hooh.json")
    mol_ret = client.add_molecules([hooh])

    ret = client.add_compute("rdkit", "UFF", "", "energy", None, mol_ret)
    assert len(ret.ids) == 1
    assert len(ret.existing) == 0

    # Perform the computation
    fractal_test_server.start_compute_worker()
    fractal_test_server.await_results()

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


@using_rdkit
def test_queue_compute_mixed_molecule(fractal_test_server):

    client = fractal_test_server.client()

    mol1 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 2.1")
    mol_ret = client.add_molecules([mol1])

    mol2 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 2.2")

    ret = client.add_compute("RDKIT", "UFF", "", "energy", None, [mol1, mol2, bad_id1], full_return=True)
    assert len(ret.data.ids) == 3
    assert ret.data.ids[2] is None
    assert len(ret.data.submitted) == 2
    assert len(ret.data.existing) == 0

    # Perform the computation
    fractal_test_server.start_compute_worker()
    fractal_test_server.await_results()

    ret = client.add_compute("rdkit", "UFF", "", "energy", None, [mol_ret[0], bad_id2])
    assert len(ret.ids) == 2
    assert ret.ids[1] is None
    assert len(ret.submitted) == 0
    assert len(ret.existing) == 1


@using_rdkit
@using_geometric
def test_queue_duplicate_procedure(fractal_test_server):

    client = fractal_test_server.client()

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

    # Perform the computation
    fractal_test_server.start_compute_worker()
    fractal_test_server.await_results()

    ret2 = client.add_procedure("optimization", "geometric", geometric_options, [bad_id1, hooh])
    assert len(ret2.ids) == 2
    assert ret2.ids[0] is None
    assert len(ret2.submitted) == 0
    assert len(ret2.existing) == 1

    assert ret.ids[0] == ret2.ids[1]


def test_queue_query_tag(fractal_test_server):
    client = fractal_test_server.client()

    mol1 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 1.1")
    mol2 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 2.2")
    mol3 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 3.3")

    # We are purposely not starting the compute worker. We just want to examine the queue

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


def test_queue_query_manager(fractal_test_server):
    client = fractal_test_server.client()

    mol1 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 1.1")
    mol2 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 2.2")
    mol3 = ptl.Molecule.from_data("He 0 0 0\nHe 0 0 3.3")

    storage_socket = fractal_test_server.get_storage_socket()
    manager = fractal_test_server.get_compute_manager("qcfractal_test_manager")
    manager_name = manager._name

    # We are purposely not starting the compute worker. We just want to examine the queue

    ret1 = client.add_compute("rdkit", "uff", "", "energy", None, mol1).ids[0]
    ret2 = client.add_compute("RDKIT", "UFF", "", "energy", None, mol2).ids[0]
    ret3 = client.add_compute("RDKIT", "UFF", "", "energy", None, mol3).ids[0]

    storage_socket.task.claim_tasks(manager_name, {"rdkit": None}, limit=1)[0]
    tasks_manager = client.query_tasks(manager=manager_name)
    assert len(tasks_manager) == 1
    assert tasks_manager[0].base_result == ret1

    storage_socket.task.claim_tasks(manager_name, {"rdkit": None}, limit=1)[0]
    storage_socket.task.claim_tasks(manager_name, {"rdkit": None}, limit=1)[0]
    tasks_manager = client.query_tasks(manager=manager_name)
    assert len(tasks_manager) == 3
