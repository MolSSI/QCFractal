"""
Explicit tests for queue manipulation.
"""

import logging
import pytest

import qcfractal.interface as portal
from qcfractal import testing, queue
from qcfractal.testing import test_server, reset_server_database


@pytest.fixture(scope="module")
def compute_manager_fixture(test_server):

    client = portal.FractalClient(test_server)

    # Build Fireworks test server and manager
    fireworks = pytest.importorskip("fireworks")
    logging.basicConfig(level=logging.CRITICAL, filename="/tmp/fireworks_logfile.txt")

    lpad = fireworks.LaunchPad(name="fw_testing_manager", logdir="/tmp/", strm_lvl="CRITICAL")
    lpad.reset(None, require_password=False)

    yield client, test_server, lpad

    # Cleanup and reset
    lpad.reset(None, require_password=False)
    logging.basicConfig(level=None, filename=None)


@testing.using_rdkit
def test_queue_manager_single(compute_manager_fixture):
    client, server, lpad = compute_manager_fixture
    reset_server_database(server)

    manager = queue.QueueManager(client, lpad)

    # Add compute
    hooh = portal.data.get_molecule("hooh.json")
    ret = client.add_compute("rdkit", "UFF", "", "energy", None, [hooh.to_json()], tag="other")

    # Force manager compute and get results
    manager.await_results()
    ret = client.get_results()
    assert len(ret) == 1


@testing.using_rdkit
def test_queue_manager_single_tags(compute_manager_fixture):
    client, server, lpad = compute_manager_fixture
    reset_server_database(server)

    manager_stuff = queue.QueueManager(client, lpad, queue_tag="stuff")
    manager_other = queue.QueueManager(client, lpad, queue_tag="other")

    # Add compute
    hooh = portal.data.get_molecule("hooh.json")
    ret = client.add_compute("rdkit", "UFF", "", "energy", None, [hooh.to_json()], tag="other")

    # Computer with the incorrect tag
    manager_stuff.await_results()
    ret = client.get_results()
    assert len(ret) == 0

    # Computer with the correct tag
    manager_other.await_results()
    ret = client.get_results()
    assert len(ret) == 1

    # Check the logs to make sure
    manager_logs = server.storage.get_managers({})["data"]
    assert len(manager_logs) == 2

    stuff_log = next(x for x in manager_logs if x["tag"] == "stuff")
    assert stuff_log["submitted"] == 0

    other_log = next(x for x in manager_logs if x["tag"] == "other")
    assert other_log["submitted"] == 1
    assert other_log["completed"] == 1


@testing.using_rdkit
def test_queue_manager_shutdown(compute_manager_fixture):
    """Tests to ensure tasks are returned to queue when the manager shuts down
    """
    client, server, lpad = compute_manager_fixture
    reset_server_database(server)

    manager = queue.QueueManager(client, lpad)

    hooh = portal.data.get_molecule("hooh.json")
    ret = client.add_compute("rdkit", "UFF", "", "energy", None, [hooh.to_json()], tag="other")

    # Pull job to manager and shutdown
    manager.update()
    assert len(manager.list_current_tasks()) == 1
    assert manager.shutdown()["nshutdown"] == 1

    # Boot new manager and await results
    manager = queue.QueueManager(client, lpad)
    manager.await_results()
    ret = client.get_results()
    assert len(ret) == 1