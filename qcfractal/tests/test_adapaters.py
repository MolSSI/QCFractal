"""
Explicit tests for queue manipulation.
"""

import pytest

import qcfractal.interface as ptl
from qcfractal import QueueManager, testing
from qcfractal.testing import adapter_client_fixture, managed_compute_server, reset_server_database


@testing.using_rdkit
def test_adapter_single(managed_compute_server):
    client, server, manager = managed_compute_server
    reset_server_database(server)

    # Add compute
    hooh = ptl.data.get_molecule("hooh.json")
    ret = client.add_compute("rdkit", "UFF", "", "energy", None, [hooh.json_dict()], tag="other")

    # Force manager compute and get results
    manager.await_results()
    ret = client.query_results()
    assert len(ret) == 1


@pytest.mark.parametrize("cores_per_task,memory_per_task,scratch_dir", [
    (None, None, None),
    (1, 1, "."),
    (2, 1.9, ".")
])  # yapf: disable
@testing.using_psi4
def test_keyword_args_passing(adapter_client_fixture, cores_per_task, memory_per_task, scratch_dir):
    psi4_mem_buffer = 0.95  # Memory consumption buffer on psi4
    adapter_client = adapter_client_fixture
    task_id = "uuid-{}-{}".format(cores_per_task, memory_per_task)
    tasks = [  # Emulate the QueueManager test function
        {
            "id": task_id,
            "spec": {
                "function":
                "qcengine.compute",
                "args": [{
                    "molecule": ptl.data.get_molecule("hooh.json").json_dict(),
                    "driver": "energy",
                    "model": {
                        "method": "HF",
                        "basis": "sto-3g"
                    },
                    "keywords": {},
                }, "psi4"],
                "kwargs": {}
            },
            "parser": "single",
            "tag": "other"
        }
    ]
    # Spin up a test queue manager
    manager = QueueManager(None, adapter_client,
                           cores_per_task=cores_per_task,
                           memory_per_task=memory_per_task,
                           scratch_directory=scratch_dir)
    # Operate on the adapter since there is no backend QCF Client
    manager.queue_adapter.submit_tasks(tasks)
    manager.queue_adapter.await_results()
    ret = manager.queue_adapter.acquire_complete()
    assert len(ret) == 1

    # Not all return objects, TOFIX
    if hasattr(ret[task_id], "dict"):
        ret = ret[task_id].dict()
    else:
        ret = ret[task_id]

    provenance = ret["provenance"]
    if cores_per_task is not None:
        assert provenance["nthreads"] == cores_per_task
    if memory_per_task is not None:
        assert provenance["memory"] == pytest.approx(memory_per_task * psi4_mem_buffer)
    if scratch_dir is not None:
        assert manager.queue_adapter.qcengine_local_options["scratch_directory"] == scratch_dir


@testing.using_rdkit
def test_adapter_error_message(managed_compute_server):
    client, server, manager = managed_compute_server
    reset_server_database(server)

    # HOOH without connectivity, RDKit should fail
    hooh = ptl.data.get_molecule("hooh.json").json_dict()
    del hooh["connectivity"]
    mol_ret = client.add_molecules([hooh])

    ret = client.add_compute("rdkit", "UFF", "", "energy", None, mol_ret)
    queue_id = ret.submitted[0]

    # Nothing should have happened yet
    assert len(manager.list_current_tasks()) == 0

    # Pull out a special iteration on the queue manager
    manager.update()
    assert len(manager.list_current_tasks()) == 1

    manager.await_results()
    assert len(manager.list_current_tasks()) == 0

    db = server.objects["storage_socket"]
    ret = db.get_queue(status="ERROR")["data"]

    assert len(ret) == 1
    assert "connectivity graph" in ret[0].error.error_message
    server.objects["storage_socket"].queue_mark_complete([queue_id])


@testing.using_rdkit
def test_adapter_raised_error(managed_compute_server):
    client, server, manager = managed_compute_server
    reset_server_database(server)

    # HOOH without connectivity, RDKit should fail
    hooh = ptl.data.get_molecule("hooh.json").json_dict()

    ret = client.add_compute("rdkit", "UFF", "", "hessian", None, hooh)
    queue_id = ret.submitted[0]

    manager.await_results()

    db = server.objects["storage_socket"]
    ret = db.get_queue(status="ERROR")["data"]

    assert len(ret) == 1
    assert "Error" in ret[0].error.error_message
    server.objects["storage_socket"].queue_mark_complete([queue_id])
