"""
Tests the database wrappers

All tests should be atomic, that is create and cleanup their data
"""

import pytest
import logging
from ...testing import load_procedure_data, caplog_handler_at_level
from ...storage_sockets.models import BaseResultORM

fake_manager_1 = {
    "cluster": "test_cluster",
    "hostname": "test_hostname",
    "username": "test_username",
    "uuid": "1234-4567-7890",
    "tag": "test_tag",
    "status": "ACTIVE",
}

fake_manager_2 = {
    "cluster": "test_cluster",
    "hostname": "test_hostname",
    "username": "test_username",
    "uuid": "1234-4567-7890",
    "tag": "test_tag",
    "status": "ACTIVE",
}


def test_procedure_basic(storage_socket):
    #
    # Tests basic workflow of creating computations and handling returns from managers
    #
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_peroxide_energy_wfn")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("psi4_methane_gradient_fail_iter")
    input_spec_4, molecule_4, result_data_4 = load_procedure_data("psi4_benzene_opt")
    input_spec_5, molecule_5, result_data_5 = load_procedure_data("psi4_peroxide_opt_fail_optiter")

    # set tags
    input_spec_1 = input_spec_1.copy(update={"tag": "for_manager_1"})
    input_spec_2 = input_spec_2.copy(update={"tag": "for_manager_1"})
    input_spec_3 = input_spec_3.copy(update={"tag": "for_manager_2"})
    input_spec_4 = input_spec_4.copy(update={"tag": "for_manager_2"})
    input_spec_5 = input_spec_5.copy(update={"tag": "for_manager_1"})

    _, ids1 = storage_socket.procedure.create([molecule_1], input_spec_1)
    _, ids2 = storage_socket.procedure.create([molecule_2], input_spec_2)
    _, ids3 = storage_socket.procedure.create([molecule_3], input_spec_3)
    _, ids4 = storage_socket.procedure.create([molecule_4], input_spec_4)
    _, ids5 = storage_socket.procedure.create([molecule_5], input_spec_5)
    all_ids = ids1 + ids2 + ids3 + ids4 + ids5

    # Should have created tasks
    meta, tasks = storage_socket.task.query(base_result=all_ids)
    assert meta.n_found == 5

    # Create the fake managers in the database
    assert storage_socket.manager.update(name="manager_1", **fake_manager_1)
    assert storage_socket.manager.update(name="manager_2", **fake_manager_2)

    # Managers claim the tasks
    storage_socket.task.claim("manager_1", ["psi4", "rdkit"], ["geometric"], 50, ["for_manager_1"])
    storage_socket.task.claim("manager_2", ["psi4", "rdkit"], ["geometric"], 50, ["for_manager_2"])

    # Tasks should be assigned correctly
    procs = storage_socket.procedure.get(all_ids, include_task=True)

    assert procs[0]["task_obj"]["manager"] == "manager_1"
    assert procs[1]["task_obj"]["manager"] == "manager_1"
    assert procs[2]["task_obj"]["manager"] == "manager_2"
    assert procs[3]["task_obj"]["manager"] == "manager_2"
    assert procs[4]["task_obj"]["manager"] == "manager_1"

    # Return results
    storage_socket.procedure.update_completed("manager_1", {ids1[0]: result_data_1})
    storage_socket.procedure.update_completed("manager_1", {ids2[0]: result_data_2})
    storage_socket.procedure.update_completed("manager_2", {ids3[0]: result_data_3})
    storage_socket.procedure.update_completed("manager_2", {ids4[0]: result_data_4})
    storage_socket.procedure.update_completed("manager_1", {ids5[0]: result_data_5})

    # Two tasks were failures. Those tasks should be the only ones remaining in the task queue
    meta, tasks = storage_socket.task.query(base_result=all_ids)
    assert meta.n_found == 2
    assert tasks[0]["manager"] == "manager_2"
    assert tasks[0]["base_result_id"] == ids3[0]
    assert tasks[1]["manager"] == "manager_1"
    assert tasks[1]["base_result_id"] == ids5[0]

    # Are the statuses, etc correct?
    procs = storage_socket.procedure.get(all_ids, include_task=True)
    assert procs[0]["status"] == "COMPLETE"
    assert procs[1]["status"] == "COMPLETE"
    assert procs[2]["status"] == "ERROR"
    assert procs[0]["task_obj"] is None
    assert procs[1]["task_obj"] is None
    assert procs[2]["task_obj"]["status"] == "ERROR"

    assert procs[0]["manager_name"] == "manager_1"
    assert procs[1]["manager_name"] == "manager_1"
    assert procs[2]["manager_name"] == "manager_2"


def test_procedure_wrong_manager_return(storage_socket, caplog):
    #
    # Manager that returns results is not the one it was assigned to
    # This also catches non-existent managers trying to return stuff
    #
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")

    _, ids = storage_socket.procedure.create([molecule_1], input_spec_1)

    # Create the fake managers in the database
    assert storage_socket.manager.update(name="manager_1", **fake_manager_1)
    assert storage_socket.manager.update(name="manager_2", **fake_manager_2)

    # Manager should claim the task
    claimed = storage_socket.task.claim("manager_1", ["psi4", "rdkit"], ["geometric"])
    assert len(claimed) == 1

    # The other manager returns the results
    with caplog_handler_at_level(caplog, logging.WARNING):
        storage_socket.procedure.update_completed("manager_2", {ids[0]: result_data_1})
        assert "belongs to manager_1, not manager manager_2" in caplog.text

    # The task should still be running, assigned to the other manager
    procs = storage_socket.procedure.get(ids, include_task=True)
    import pprint

    pprint.pprint(procs)

    assert len(procs) == 1
    assert procs[0]["task_obj"]["manager"] == "manager_1"
    assert procs[0]["task_obj"]["status"] == "RUNNING"


def test_procedure_nonexist_task(storage_socket, caplog):
    #
    # Manager returns data for a task that doesn't exist
    # This can happen if the task is deleted while running, for example
    #
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")

    # Create the fake manager in the database
    assert storage_socket.manager.update(name="manager_1", **fake_manager_1)

    # Try returning something
    with caplog_handler_at_level(caplog, logging.WARNING):
        storage_socket.procedure.update_completed("manager_1", {12345: result_data_1})
        assert "does not exist in the task queue" in caplog.text


def test_procedure_base_already_complete(storage_socket, caplog):
    #
    # Manager returns data for a task that exists, but for some reason the base result is complete
    # This check is for sanity and should not occur
    #

    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")
    _, ids = storage_socket.procedure.create([molecule_1], input_spec_1)

    # Create the fake manager in the database
    assert storage_socket.manager.update(name="manager_1", **fake_manager_1)

    # We don't expose this functionality for a reason...
    with storage_socket.session_scope() as session:
        session.query(BaseResultORM).update(dict(status="COMPLETE"))

    # Try returning something
    with caplog_handler_at_level(caplog, logging.ERROR):
        storage_socket.procedure.update_completed("manager_1", {ids[0]: result_data_1})
        assert "is already complete!" in caplog.text

    # The corresponding task should be deleted now
    procs = storage_socket.procedure.get(ids, include_task=True)
    assert procs[0]["task_obj"] is None
