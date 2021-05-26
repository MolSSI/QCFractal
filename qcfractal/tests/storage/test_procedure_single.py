"""
Tests the database wrappers

All tests should be atomic, that is create and cleanup their data
"""

from datetime import datetime
from .test_procedure import load_procedure_data, fake_manager_1, fake_manager_2
from qcfractal.interface.models import RecordStatusEnum


def test_procedure_single_query(storage_socket):
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_methane_gradient_fail_iter")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("rdkit_water_energy")

    # set tags
    input_spec_1 = input_spec_1.copy(update={"tag": "for_manager_1"})
    input_spec_2 = input_spec_2.copy(update={"tag": "for_manager_1"})
    input_spec_3 = input_spec_3.copy(update={"tag": "for_manager_2"})

    _, ids1 = storage_socket.procedure.create([molecule_1], input_spec_1)
    _, ids2 = storage_socket.procedure.create([molecule_2], input_spec_2)
    _, ids3 = storage_socket.procedure.create([molecule_3], input_spec_3)
    all_ids = ids1 + ids2 + ids3

    # Create the fake managers in the database
    assert storage_socket.manager.update(name="manager_1", **fake_manager_1)
    assert storage_socket.manager.update(name="manager_2", **fake_manager_2)

    # Managers claim some of the tasks
    storage_socket.task_queue.claim("manager_1", ["psi4", "rdkit"], ["geometric"], 50, ["for_manager_1"])
    storage_socket.task_queue.claim("manager_2", ["psi4", "rdkit"], ["geometric"], 50, ["for_manager_2"])

    # Return some of the results
    # The ids returned from create() are the result ids, but the managers return task ids
    procs = storage_socket.procedure.single.get(all_ids, include=["*", "task_obj"])
    task_ids = [x["task_obj"]["id"] for x in procs]

    storage_socket.procedure.update_completed("manager_1", {task_ids[0]: result_data_1})

    # Now finally test the queries
    meta, procs = storage_socket.procedure.single.query(id=ids1)
    assert meta.n_returned == 1
    assert procs[0]["id"] == str(all_ids[0])

    # Manager is only assigned to the result on completion or error
    meta, procs = storage_socket.procedure.single.query(manager=["manager_1"])
    assert meta.n_returned == 1
    assert procs[0]["id"] == str(all_ids[0])

    meta, procs = storage_socket.procedure.single.query(
        created_before=datetime.utcnow(), status=[RecordStatusEnum.incomplete]
    )
    assert meta.n_returned == 2
    assert procs[0]["id"] == str(all_ids[1])
    assert procs[1]["id"] == str(all_ids[2])

    meta, procs = storage_socket.procedure.single.query(created_after=datetime.utcnow())
    assert meta.n_returned == 0

    meta, procs = storage_socket.procedure.single.query(status=[RecordStatusEnum.complete], include=["*", "stdout_obj"])
    assert meta.n_returned == 1
    assert len(procs[0]["stdout_obj"]) > 1
