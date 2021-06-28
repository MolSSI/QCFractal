"""
Tests the database wrappers

All tests should be atomic, that is create and cleanup their data
"""

from ...testing import load_procedure_data
from qcfractal.interface.models import ManagerStatusEnum, PriorityEnum

fake_manager_1 = {
    "cluster": "test_cluster",
    "hostname": "test_hostname",
    "username": "test_username",
    "uuid": "1234-4567-7890",
    "tag": "test_tag",
    "status": ManagerStatusEnum.active,
}


def test_task_nonexist_manager_claim(storage_socket):
    #
    # Manager that tries to claim some tasks does not exist
    #

    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")

    _, ids = storage_socket.procedure.create([molecule_1], input_spec_1)

    # Some random manager tries to claim the task
    claimed = storage_socket.procedure.claim_tasks("some_manager", {"psi4": None, "rdkit": None, "geometric": None})
    assert len(claimed) == 0

    #  Create it to make sure that it would actually claim the task
    assert storage_socket.manager.update(name="some_manager", **fake_manager_1)
    claimed = storage_socket.procedure.claim_tasks("some_manager", {"psi4": None, "rdkit": None, "geometric": None})
    assert len(claimed) == 1


def test_task_inactive_manager_claim(storage_socket):
    #
    # Manager that tries to claim some tasks exists but is inactive
    #

    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")

    _, ids = storage_socket.procedure.create([molecule_1], input_spec_1)

    assert storage_socket.manager.update(name="some_manager", **fake_manager_1)
    storage_socket.manager.deactivate(name=["some_manager"])

    claimed = storage_socket.procedure.claim_tasks("some_manager", {"psi4": None, "rdkit": None, "geometric": None})
    assert len(claimed) == 0

    # Manually set to active to make sure it can be claimed
    assert storage_socket.manager.update(name="some_manager", status=ManagerStatusEnum.active)
    claimed = storage_socket.procedure.claim_tasks("some_manager", {"psi4": None, "rdkit": None, "geometric": None})
    assert len(claimed) == 1


def test_task_ordering_time(storage_socket):
    #
    # Test ordering of tasks by creation time
    #

    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_peroxide_energy_wfn")

    _, ids_1 = storage_socket.procedure.create([molecule_1], input_spec_1)
    _, ids_2 = storage_socket.procedure.create([molecule_2], input_spec_2)

    assert storage_socket.manager.update(name="some_manager", **fake_manager_1)

    queue_id1 = storage_socket.procedure.claim_tasks("some_manager", {"psi4": None}, limit=1)[0]["base_result_id"]
    queue_id2 = storage_socket.procedure.claim_tasks("some_manager", {"psi4": None}, limit=1)[0]["base_result_id"]

    assert queue_id1 == int(ids_1[0])
    assert queue_id2 == int(ids_2[0])


def test_queue_ordering_priority(storage_socket):

    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_peroxide_energy_wfn")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("psi4_methane_gradient_fail_iter")

    input_spec_1 = input_spec_1.copy(update={"priority": PriorityEnum.normal})
    input_spec_2 = input_spec_2.copy(update={"priority": PriorityEnum.high})
    input_spec_3 = input_spec_3.copy(update={"priority": PriorityEnum.low})

    _, ids_1 = storage_socket.procedure.create([molecule_1], input_spec_1)
    _, ids_2 = storage_socket.procedure.create([molecule_2], input_spec_2)
    _, ids_3 = storage_socket.procedure.create([molecule_3], input_spec_3)

    assert storage_socket.manager.update(name="some_manager", **fake_manager_1)

    queue_id1 = storage_socket.procedure.claim_tasks("some_manager", {"psi4": None}, limit=1)[0]["base_result_id"]
    queue_id2 = storage_socket.procedure.claim_tasks("some_manager", {"psi4": None}, limit=1)[0]["base_result_id"]
    queue_id3 = storage_socket.procedure.claim_tasks("some_manager", {"psi4": None}, limit=1)[0]["base_result_id"]

    assert queue_id1 == int(ids_2[0])
    assert queue_id2 == int(ids_1[0])
    assert queue_id3 == int(ids_3[0])


def test_queue_order_procedure_priority(storage_socket):

    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")
    input_spec_2, molecule_2, result_data_2 = load_procedure_data("psi4_peroxide_energy_wfn")
    input_spec_3, molecule_3, result_data_3 = load_procedure_data("psi4_benzene_opt")

    input_spec_1 = input_spec_1.copy(update={"priority": PriorityEnum.normal})
    input_spec_2 = input_spec_2.copy(update={"priority": PriorityEnum.high})
    input_spec_3 = input_spec_3.copy(update={"priority": PriorityEnum.low})

    _, ids_1 = storage_socket.procedure.create([molecule_1], input_spec_1)
    _, ids_2 = storage_socket.procedure.create([molecule_2], input_spec_2)
    _, ids_3 = storage_socket.procedure.create([molecule_3], input_spec_3)

    assert storage_socket.manager.update(name="some_manager", **fake_manager_1)

    assert len(storage_socket.procedure.claim_tasks("some_manager", {"rdkit": None}, limit=1)) == 0
    assert len(storage_socket.procedure.claim_tasks("some_manager", {"rdkit": None, "geom": None}, limit=1)) == 0
    assert len(storage_socket.procedure.claim_tasks("some_manager", {"prog1": None, "geometric": None}, limit=1)) == 0

    queue_id1 = storage_socket.procedure.claim_tasks("some_manager", {"psi4": None, "geometric": None}, limit=1)[0][
        "base_result_id"
    ]
    queue_id2 = storage_socket.procedure.claim_tasks("some_manager", {"psi4": None, "geometric": None}, limit=1)[0][
        "base_result_id"
    ]
    queue_id3 = storage_socket.procedure.claim_tasks("some_manager", {"psi4": None, "geometric": None}, limit=1)[0][
        "base_result_id"
    ]

    assert queue_id1 == int(ids_2[0])
    assert queue_id2 == int(ids_1[0])
    assert queue_id3 == int(ids_3[0])
