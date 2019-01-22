"""
Explicit tests for queue manipulation.
"""

import time

import pytest

import qcfractal.interface as portal
from qcfractal import testing
from qcfractal.testing import reset_server_database, managed_compute_server


@testing.using_rdkit
def test_adapter_single(managed_compute_server):
    client, server, manager = managed_compute_server
    reset_server_database(server)

    # Add compute
    hooh = portal.data.get_molecule("hooh.json")
    ret = client.add_compute("rdkit", "UFF", "", "energy", None, [hooh.to_json()], tag="other")

    # Force manager compute and get results
    manager.await_results()
    ret = client.get_results()
    assert len(ret) == 1


@testing.using_rdkit
def test_adapter_error_message(managed_compute_server):
    client, server, manager = managed_compute_server
    reset_server_database(server)

    # HOOH without connectivity, RDKit should fail
    hooh = portal.data.get_molecule("hooh.json").to_json()
    del hooh["connectivity"]
    mol_ret = client.add_molecules({"hooh": hooh})

    ret = client.add_compute("rdkit", "UFF", "", "energy", None, mol_ret["hooh"])
    queue_id = ret["submitted"][0]

    # Nothing should have happened yet
    assert len(manager.list_current_tasks()) == 0

    # Pull out a special iteration on the queue manager
    manager.update()
    assert len(manager.list_current_tasks()) == 1

    manager.await_results()
    assert len(manager.list_current_tasks()) == 0

    db = server.objects["storage_socket"]
    ret = db.get_queue({"status": "ERROR"})["data"]

    assert len(ret) == 1
    assert "connectivity graph" in ret[0]["error"]
    server.objects["storage_socket"].queue_mark_complete([queue_id])


@testing.using_rdkit
def test_adapter_raised_error(managed_compute_server):
    client, server, manager = managed_compute_server
    reset_server_database(server)

    # HOOH without connectivity, RDKit should fail
    hooh = portal.data.get_molecule("hooh.json").to_json()
    del hooh["connectivity"]
    mol_ret = client.add_molecules({"hooh": hooh})

    ret = client.add_compute("something_bas", "UFF", "", "energy", None, mol_ret["hooh"])
    queue_id = ret["submitted"][0]

    manager.await_results()

    db = server.objects["storage_socket"]
    ret = db.get_queue({"status": "ERROR"})["data"]

    assert len(ret) == 1
    assert "QCEngine Call Error" in ret[0]["error"]
    server.objects["storage_socket"].queue_mark_complete([queue_id])