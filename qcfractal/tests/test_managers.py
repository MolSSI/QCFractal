"""
Explicit tests for queue manipulation.
"""

import time
from concurrent.futures import ProcessPoolExecutor

import pytest

import qcfractal.interface as ptl
from qcfractal import FractalServer, queue, testing
from qcfractal.testing import reset_server_database, test_server


@pytest.fixture(scope="module")
def compute_adapter_fixture(test_server):

    client = ptl.FractalClient(test_server)

    with ProcessPoolExecutor(max_workers=2) as adapter:

        yield client, test_server, adapter


@testing.using_rdkit
def test_queue_manager_single_tags(compute_adapter_fixture):
    client, server, adapter = compute_adapter_fixture
    reset_server_database(server)

    manager_stuff = queue.QueueManager(client, adapter, queue_tag="stuff")
    manager_other = queue.QueueManager(client, adapter, queue_tag="other")

    # Add compute
    hooh = ptl.data.get_molecule("hooh.json")
    ret = client.add_compute("rdkit", "UFF", "", "energy", None, [hooh.json_dict()], tag="other")

    # Computer with the incorrect tag
    manager_stuff.await_results()
    ret = client.query_results()
    assert len(ret) == 0

    # Computer with the correct tag
    manager_other.await_results()
    ret = client.query_results()
    assert len(ret) == 1

    # Check the logs to make sure
    manager_logs = server.storage.get_managers()["data"]
    assert len(manager_logs) == 2

    stuff_log = next(x for x in manager_logs if x["tag"] == "stuff")
    assert stuff_log["submitted"] == 0

    other_log = next(x for x in manager_logs if x["tag"] == "other")
    assert other_log["submitted"] == 1
    assert other_log["completed"] == 1


@testing.using_rdkit
def test_queue_manager_shutdown(compute_adapter_fixture):
    """Tests to ensure tasks are returned to queue when the manager shuts down
    """
    client, server, adapter = compute_adapter_fixture
    reset_server_database(server)

    manager = queue.QueueManager(client, adapter)

    hooh = ptl.data.get_molecule("hooh.json")
    client.add_compute("rdkit", "UFF", "", "energy", None, [hooh.json_dict()], tag="other")

    # Pull job to manager and shutdown
    manager.update()
    assert len(manager.list_current_tasks()) == 1
    assert manager.shutdown()["nshutdown"] == 1

    sman = server.list_managers(name=manager.name())
    assert len(sman) == 1
    assert sman[0]["status"] == "INACTIVE"

    # Boot new manager and await results
    manager = queue.QueueManager(client, adapter)
    manager.await_results()
    ret = client.query_results()
    assert len(ret) == 1

@testing.using_rdkit
def test_queue_manager_server_delay(compute_adapter_fixture):
    """Test to ensure interrupts to the server shutdown correctly"""
    client, server, adapter = compute_adapter_fixture
    reset_server_database(server)

    manager = queue.QueueManager(client, adapter, server_error_retries=1)

    hooh = ptl.data.get_molecule("hooh.json")
    client.add_compute("rdkit", "UFF", "", "energy", None, [hooh.json_dict()], tag="other")

    # Pull job to manager and shutdown
    manager.update()
    assert len(manager.list_current_tasks()) == 1

    # Mock a network error
    client._mock_network_error = True
    # Let the calculation finish
    manager.queue_adapter.await_results()
    # Try to push the changes through the network error
    manager.update()
    assert len(manager.list_current_tasks()) == 0
    assert len(manager._stale_payload_tracking) == 1
    assert manager.n_stale_jobs == 0

    # Try again to push the tracked attempts into stale
    manager.update()
    assert len(manager.list_current_tasks()) == 0
    assert len(manager._stale_payload_tracking) == 0
    assert manager.n_stale_jobs == 1
    # Update again to push jobs to stale
    manager.update()

    # Return the jobs to the server
    client._mock_network_error = False
    assert manager.shutdown()["nshutdown"] == 1

    # Once more, but this time restart the server in between
    manager = queue.QueueManager(client, adapter, server_error_retries=1)
    manager.update()
    assert len(manager.list_current_tasks()) == 1
    manager.queue_adapter.await_results()
    # Trigger our failure
    client._mock_network_error = True
    manager.update()
    assert len(manager.list_current_tasks()) == 0
    assert len(manager._stale_payload_tracking) == 1
    assert manager.n_stale_jobs == 0
    # Stop mocking a network error
    client._mock_network_error = False
    manager.update()
    assert len(manager.list_current_tasks()) == 0
    assert len(manager._stale_payload_tracking) == 0
    assert manager.n_stale_jobs == 0


def test_queue_manager_heartbeat(compute_adapter_fixture):
    """Tests to ensure tasks are returned to queue when the manager shuts down
    """

    client, _, adapter = compute_adapter_fixture

    with testing.loop_in_thread() as loop:

        # Build server, manually handle IOLoop (no start/stop needed)
        server = FractalServer(
            port=testing.find_open_port(),
            storage_project_name="qcf_heartbeat_checker_test",
            loop=loop,
            ssl_options=False,
            heartbeat_frequency=0.1)

        # Clean and re-init the database
        testing.reset_server_database(server)

        client = ptl.FractalClient(server)
        manager = queue.QueueManager(client, adapter)

        sman = server.list_managers(name=manager.name())
        assert len(sman) == 1
        assert sman[0]["status"] == "ACTIVE"

        # Make sure interval exceeds heartbeat time
        time.sleep(1)
        server.check_manager_heartbeats()

        sman = server.list_managers(name=manager.name())
        assert len(sman) == 1
        assert sman[0]["status"] == "INACTIVE"


def test_manager_max_tasks_limiter(compute_adapter_fixture):
    client, server, adapter = compute_adapter_fixture

    manager = queue.QueueManager(client, adapter, queue_tag="stuff", max_tasks=1.e9)
    assert manager.max_tasks < 1.e9


def test_queue_manager_testing():

    with ProcessPoolExecutor(max_workers=2) as adapter:
        manager = queue.QueueManager(None, adapter)

        assert manager.test()
