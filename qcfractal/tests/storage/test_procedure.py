"""
Tests the database wrappers

All tests should be atomic, that is create and cleanup their data
"""

import logging
from ...testing import load_procedure_data, caplog_handler_at_level
from ...components.records.db_models import BaseResultORM
from qcfractal.interface.models import ObjectId, RecordStatusEnum, ManagerStatusEnum

fake_manager_1 = {
    "cluster": "test_cluster",
    "hostname": "test_hostname",
    "username": "test_username",
    "uuid": "1234-4567-7890",
    "tag": "test_tag",
    "status": ManagerStatusEnum.active,
}

fake_manager_2 = {
    "cluster": "test_cluster",
    "hostname": "test_hostname",
    "username": "test_username",
    "uuid": "1234-4567-7890",
    "tag": "test_tag",
    "status": ManagerStatusEnum.active,
}

fake_program_info = {"psi4": None, "rdkit": None, "geometric": None}


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

    _, ids1 = storage_socket.tasks.create([molecule_1], input_spec_1)
    _, ids2 = storage_socket.tasks.create([molecule_2], input_spec_2)
    _, ids3 = storage_socket.tasks.create([molecule_3], input_spec_3)
    _, ids4 = storage_socket.tasks.create([molecule_4], input_spec_4)
    _, ids5 = storage_socket.tasks.create([molecule_5], input_spec_5)
    all_ids = ids1 + ids2 + ids3 + ids4 + ids5

    procs = storage_socket.records.get(all_ids, include=["*", "task_obj"])
    task_ids = [x["task_obj"]["id"] for x in procs]

    # Should have created tasks
    tasks = storage_socket.tasks.get_tasks(task_ids)
    assert len(tasks) == 5
    assert all("psi4" in t["required_programs"] for t in tasks)
    assert all("geometric" in t["required_programs"] for t in tasks[3:])

    # Create the fake managers in the database
    assert storage_socket.managers.update(name="manager_1", **fake_manager_1)
    assert storage_socket.managers.update(name="manager_2", **fake_manager_2)

    # Managers claim the tasks
    storage_socket.tasks.claim_tasks("manager_1", fake_program_info, 50, ["for_manager_1"])
    storage_socket.tasks.claim_tasks("manager_2", fake_program_info, 50, ["for_manager_2"])

    # Tasks should be assigned correctly
    procs = storage_socket.records.get(all_ids, include=["*", "task_obj"])

    assert procs[0]["manager_name"] == "manager_1"
    assert procs[1]["manager_name"] == "manager_1"
    assert procs[2]["manager_name"] == "manager_2"
    assert procs[3]["manager_name"] == "manager_2"
    assert procs[4]["manager_name"] == "manager_1"
    assert procs[0]["status"] == RecordStatusEnum.running
    assert procs[1]["status"] == RecordStatusEnum.running
    assert procs[2]["status"] == RecordStatusEnum.running
    assert procs[3]["status"] == RecordStatusEnum.running
    assert procs[4]["status"] == RecordStatusEnum.running

    # Return results
    # The ids returned from create() are the result ids, but the managers return task ids
    storage_socket.tasks.update_completed("manager_1", {task_ids[0]: result_data_1})
    storage_socket.tasks.update_completed("manager_1", {task_ids[1]: result_data_2})
    storage_socket.tasks.update_completed("manager_2", {task_ids[2]: result_data_3})
    storage_socket.tasks.update_completed("manager_2", {task_ids[3]: result_data_4})
    storage_socket.tasks.update_completed("manager_1", {task_ids[4]: result_data_5})

    # Two tasks were failures. Those tasks should be the only ones remaining in the task queue
    tasks = storage_socket.tasks.get_tasks(task_ids, missing_ok=True)
    assert len(tasks) == 5
    assert tasks.count(None) == 3
    assert tasks[2]["base_result_id"] == ids3[0]
    assert tasks[4]["base_result_id"] == ids5[0]

    # Are the statuses, etc correct?
    procs = storage_socket.records.get(all_ids, include=["*", "task_obj"])
    assert procs[0]["status"] == RecordStatusEnum.complete
    assert procs[1]["status"] == RecordStatusEnum.complete
    assert procs[2]["status"] == RecordStatusEnum.error
    assert procs[0]["task_obj"] is None
    assert procs[1]["task_obj"] is None

    assert procs[0]["manager_name"] == "manager_1"
    assert procs[1]["manager_name"] == "manager_1"
    assert procs[2]["manager_name"] == "manager_2"


def test_procedure_wrong_manager_return(storage_socket, caplog):
    #
    # Manager that returns results is not the one it was assigned to
    # This also catches non-existent managers trying to return stuff
    #
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")

    _, ids = storage_socket.tasks.create([molecule_1], input_spec_1)

    # Create the fake managers in the database
    assert storage_socket.managers.update(name="manager_1", **fake_manager_1)
    assert storage_socket.managers.update(name="manager_2", **fake_manager_2)

    # Manager should claim the task
    claimed = storage_socket.tasks.claim_tasks("manager_1", fake_program_info)
    assert len(claimed) == 1

    # The other manager returns the results
    with caplog_handler_at_level(caplog, logging.WARNING):
        storage_socket.tasks.update_completed("manager_2", {ids[0]: result_data_1})
        assert "belongs to manager_1, not manager manager_2" in caplog.text

    # The task should still be running, assigned to the other manager
    procs = storage_socket.records.get(ids, include=["*", "task_obj"])

    assert len(procs) == 1
    assert procs[0]["manager_name"] == "manager_1"
    assert procs[0]["status"] == RecordStatusEnum.running


def test_procedure_nonexist_task(storage_socket, caplog):
    #
    # Manager returns data for a task that doesn't exist
    # This can happen if the task is deleted while running, for example
    #
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")

    # Create the fake manager in the database
    assert storage_socket.managers.update(name="manager_1", **fake_manager_1)

    # Try returning something
    with caplog_handler_at_level(caplog, logging.WARNING):
        storage_socket.tasks.update_completed("manager_1", {12345: result_data_1})
        assert "does not exist in the task queue" in caplog.text


def test_procedure_base_already_complete(storage_socket, caplog):
    #
    # Manager returns data for a task that exists, but for some reason the base result is complete
    # This check is for sanity and should not occur
    #

    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")
    _, ids = storage_socket.tasks.create([molecule_1], input_spec_1)

    # Create the fake manager in the database
    assert storage_socket.managers.update(name="manager_1", **fake_manager_1)

    # We don't expose this functionality for a reason...
    with storage_socket.session_scope() as session:
        session.query(BaseResultORM).update(dict(status=RecordStatusEnum.complete))

    # Try returning something
    with caplog_handler_at_level(caplog, logging.WARNING):
        storage_socket.tasks.update_completed("manager_1", {ids[0]: result_data_1})
        assert "is not in the running state" in caplog.text


def test_procedure_get(storage_socket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_benzene_opt")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("rdkit_water_energy")

    _, ids1 = storage_socket.tasks.create([molecule_1], input_spec_1)
    _, ids2 = storage_socket.tasks.create([molecule_2], input_spec_2)
    _, ids3 = storage_socket.tasks.create([molecule_3], input_spec_3)

    # notice the order
    procs = storage_socket.records.get(id=ids3 + ids1 + ids2)
    assert procs[0]["id"] == ObjectId(ids3[0])
    assert procs[1]["id"] == ObjectId(ids1[0])
    assert procs[2]["id"] == ObjectId(ids2[0])


def test_procedure_get_empty(storage_socket):
    assert storage_socket.records.get([]) == []

    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")
    _, ids1 = storage_socket.tasks.create([molecule_1], input_spec_1)

    assert storage_socket.records.get([]) == []


def test_procedure_query(storage_socket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_methane_gradient_fail_iter")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("psi4_benzene_opt")
    input_spec_4, molecule_4, result_data_4 = load_procedure_data("psi4_peroxide_opt_fail_optiter")
    input_spec_5, molecule_5, result_data_5 = load_procedure_data("rdkit_water_energy")

    # set tags
    input_spec_1 = input_spec_1.copy(update={"tag": "for_manager_1"})
    input_spec_2 = input_spec_2.copy(update={"tag": "for_manager_1"})
    input_spec_3 = input_spec_3.copy(update={"tag": "for_manager_2"})
    input_spec_4 = input_spec_4.copy(update={"tag": "for_manager_2"})
    input_spec_5 = input_spec_5.copy(update={"tag": "for_manager_3"})

    _, ids1 = storage_socket.tasks.create([molecule_1], input_spec_1)
    _, ids2 = storage_socket.tasks.create([molecule_2], input_spec_2)
    _, ids3 = storage_socket.tasks.create([molecule_3], input_spec_3)
    _, ids4 = storage_socket.tasks.create([molecule_4], input_spec_4)
    _, ids5 = storage_socket.tasks.create([molecule_5], input_spec_5)
    all_ids = ids1 + ids2 + ids3 + ids4 + ids5

    # Create the fake managers in the database
    assert storage_socket.managers.update(name="manager_1", **fake_manager_1)
    assert storage_socket.managers.update(name="manager_2", **fake_manager_2)

    # Managers claim some of the tasks
    storage_socket.tasks.claim_tasks("manager_1", fake_program_info, 50, ["for_manager_1"])
    storage_socket.tasks.claim_tasks("manager_2", fake_program_info, 50, ["for_manager_2"])

    # Return some of the results
    # The ids returned from create() are the result ids, but the managers return task ids
    procs = storage_socket.records.get(all_ids, include=["*", "task_obj"])
    task_ids = [x["task_obj"]["id"] for x in procs]

    storage_socket.tasks.update_completed("manager_1", {task_ids[0]: result_data_1})
    storage_socket.tasks.update_completed("manager_1", {task_ids[1]: result_data_2})
    storage_socket.tasks.update_completed("manager_2", {task_ids[2]: result_data_3})

    # Now finally test the queries
    meta, procs = storage_socket.records.query(id=ids1)
    assert meta.n_returned == 1
    assert procs[0]["procedure"] == "single"
    assert procs[0]["program"] == "psi4"

    meta, procs = storage_socket.records.query(procedure=["optimization"], status=[RecordStatusEnum.complete])
    assert meta.n_returned == 1
    assert {int(x["id"]) for x in procs} == {ids3[0]}

    meta, procs = storage_socket.records.query(status=[RecordStatusEnum.error])
    assert meta.n_returned == 1
    assert {int(x["id"]) for x in procs} == {ids2[0]}

    meta, procs = storage_socket.records.query(manager=["manager_1"])
    assert meta.n_returned == 2
    assert {int(x["id"]) for x in procs} == {ids1[0], ids2[0]}


def test_procedure_query_empty(storage_socket):
    meta, procs = storage_socket.records.query()
    assert meta.n_returned == 0

    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")
    _, ids1 = storage_socket.tasks.create([molecule_1], input_spec_1)

    meta, procs = storage_socket.records.query()
    assert meta.n_returned == 1

    meta, procs = storage_socket.records.query(id=[])
    assert meta.n_returned == 0

    meta, procs = storage_socket.records.query(status=[])
    assert meta.n_returned == 0


def test_procedure_create_existing(storage_socket):
    #
    # Tests re-creating procedures and deduplication
    #
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_benzene_opt")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("psi4_peroxide_opt_fail_optiter")
    input_spec_4, molecule_4, result_data_4 = load_procedure_data("psi4_peroxide_energy_wfn")

    _, ids1 = storage_socket.tasks.create([molecule_1], input_spec_1)
    _, ids2 = storage_socket.tasks.create([molecule_2], input_spec_2)
    _, ids3 = storage_socket.tasks.create([molecule_3], input_spec_3)
    all_ids = ids1 + ids2 + ids3

    # Should have created tasks
    meta, tasks = storage_socket.tasks.query_tasks(base_result_id=all_ids)
    assert meta.n_found == 3

    # Create the fake managers in the database
    assert storage_socket.managers.update(name="manager_1", **fake_manager_1)

    # Managers claim the tasks
    storage_socket.tasks.claim_tasks("manager_1", fake_program_info, 50)

    # Attempt to recreate. Should not do anything
    meta_1, new_ids_1 = storage_socket.tasks.create([molecule_1], input_spec_1)
    meta_2, new_ids_2 = storage_socket.tasks.create([molecule_2], input_spec_2)
    meta_3, new_ids_3 = storage_socket.tasks.create([molecule_3], input_spec_3)
    assert meta_1.n_existing == 1
    assert meta_2.n_existing == 1
    assert meta_3.n_existing == 1
    meta, tasks = storage_socket.tasks.query_tasks(base_result_id=all_ids)
    assert meta.n_found == 3

    # Return results
    # The ids returned from create() are the result ids, but the managers return task ids
    procs = storage_socket.records.get(all_ids, include=["*", "task_obj"])
    task_ids = [x["task_obj"]["id"] for x in procs]
    storage_socket.tasks.update_completed("manager_1", {task_ids[0]: result_data_1})
    storage_socket.tasks.update_completed("manager_1", {task_ids[1]: result_data_2})
    storage_socket.tasks.update_completed("manager_1", {task_ids[2]: result_data_3})

    # Tasks for the successful ones shouldn't exist now
    # One was a failure
    meta, tasks = storage_socket.tasks.query_tasks(base_result_id=all_ids)
    assert meta.n_found == 1

    # If I recreate the calculations, nothing should have changed
    # Add a new one as well
    meta_1, new_ids_1 = storage_socket.tasks.create([molecule_1], input_spec_1)
    meta_2, new_ids_2 = storage_socket.tasks.create([molecule_2], input_spec_2)
    meta_3, new_ids_3 = storage_socket.tasks.create([molecule_3], input_spec_3)
    meta_4, new_ids_4 = storage_socket.tasks.create([molecule_4], input_spec_4)
    assert meta_1.n_existing == 1
    assert meta_2.n_existing == 1
    assert meta_3.n_existing == 1
    assert meta_4.n_inserted == 1

    # Tasks for the completed computations should not have been recreated
    # (this query does not add the fourth calc we just added)
    meta, tasks = storage_socket.tasks.query_tasks(base_result_id=all_ids)
    assert meta.n_found == 1

    meta, tasks = storage_socket.tasks.query_tasks(base_result_id=[new_ids_4])
    assert meta.n_found == 1
