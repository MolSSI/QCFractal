"""
Tests the database wrappers

All tests should be atomic, that is create and cleanup their data
"""

from ...testing import load_procedure_data
from qcfractal.interface.models import ManagerStatusEnum

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

    # Some random manager tries to claim something
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")

    _, ids = storage_socket.procedure.create([molecule_1], input_spec_1)

    # Some random manager tries to claim the task
    claimed = storage_socket.task.claim("some_manager", ["psi4", "rdkit"], ["geometric"])
    assert len(claimed) == 0

    #  Create it to make sure that it would actually claim the task
    assert storage_socket.manager.update(name="some_manager", **fake_manager_1)
    claimed = storage_socket.task.claim("some_manager", ["psi4", "rdkit"], ["geometric"])
    assert len(claimed) == 1


def test_task_inactive_manager_claim(storage_socket):
    #
    # Manager that tries to claim some tasks exists but is inactive
    #

    # Some random manager tries to claim something
    input_spec_1, molecule_1, result_data_1 = load_procedure_data("psi4_benzene_energy_1")

    _, ids = storage_socket.procedure.create([molecule_1], input_spec_1)

    assert storage_socket.manager.update(name="some_manager", **fake_manager_1)
    storage_socket.manager.deactivate(name=["some_manager"])

    claimed = storage_socket.task.claim("some_manager", ["psi4", "rdkit"], ["geometric"])
    assert len(claimed) == 0

    # Manually set to active to make sure it can be claimed
    assert storage_socket.manager.update(name="some_manager", status=ManagerStatusEnum.active)
    claimed = storage_socket.task.claim("some_manager", ["psi4", "rdkit"], ["geometric"])
    assert len(claimed) == 1
