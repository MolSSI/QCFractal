"""
Explicit tests for queue manipulation.
"""

import datetime
import logging
import re
import time
from concurrent.futures import ProcessPoolExecutor

import pytest

import qcfractal.interface as ptl
from qcfractal.interface.models import TaskStatusEnum, RecordStatusEnum, ManagerStatusEnum
from qcfractal import qc_queue as queue, testing
from ..testing import caplog_handler_at_level


@pytest.fixture(scope="function")
def compute_adapter_fixture(fractal_test_server):

    client = fractal_test_server.client()

    with ProcessPoolExecutor(max_workers=2) as adapter:
        yield client, fractal_test_server, adapter


@testing.using_rdkit
def test_queue_manager_single_tags(compute_adapter_fixture):
    client, server, adapter = compute_adapter_fixture

    config = {"Hello": "World"}
    manager_stuff = queue.QueueManager(client, adapter, queue_tag="stuff", configuration=config)
    manager_other = queue.QueueManager(client, adapter, queue_tag="other", configuration=config)

    # Add compute
    hooh = ptl.data.get_molecule("hooh.json")
    ret = client.add_compute("rdkit", "UFF", "", "energy", None, [hooh], tag="other")

    # Computer with the incorrect tag
    manager_stuff.await_results()
    ret = client.query_results()
    assert len(ret) == 0

    # Computer with the correct tag
    manager_other.await_results()
    ret = client.query_results()
    assert len(ret) == 1

    # Check the logs to make sure
    managers = client.query_managers()
    assert len(managers) == 2

    test_results = {"stuff": 0, "other": 1}
    for manager in managers:
        value = test_results[manager["tag"]]
        assert manager["submitted"] == value
        assert manager["completed"] == value
        # TODO assert manager["username"] == CLIENT_USERNAME
        assert isinstance(manager["configuration"], dict)


@testing.using_rdkit
def test_queue_manager_multiple_tags(compute_adapter_fixture):
    client, server, adapter = compute_adapter_fixture

    config = {"Hello": "World"}
    base_molecule = ptl.data.get_molecule("butane.json")

    # Add compute
    molecules = [base_molecule.copy(update={"geometry": base_molecule.geometry + 0.1 * i}) for i in range(6)]
    tags = ["tag2", "tag1", "tag1", "tag2", "tag1", "tag3"]
    tasks = [
        client.add_compute("rdkit", "UFF", "", "energy", None, [mol], tag=tag).ids[0]
        for mol, tag in zip(molecules, tags)
    ]

    manager = queue.QueueManager(client, adapter, queue_tag=["tag1", "tag2"], configuration=config, max_tasks=2)

    # Check that tasks are pulled in the correct order
    manager.await_results()
    ret = client.query_results(tasks)
    ref_status = {
        tasks[0]: RecordStatusEnum.incomplete,
        tasks[1]: RecordStatusEnum.complete,
        tasks[2]: RecordStatusEnum.complete,
        tasks[3]: RecordStatusEnum.incomplete,
        tasks[4]: RecordStatusEnum.incomplete,
        tasks[5]: RecordStatusEnum.incomplete,
    }
    for result in ret:
        assert result.status == ref_status[result.id]
    manager.await_results()
    ret = client.query_results(tasks)
    ref_status = {
        tasks[0]: RecordStatusEnum.complete,
        tasks[1]: RecordStatusEnum.complete,
        tasks[2]: RecordStatusEnum.complete,
        tasks[3]: RecordStatusEnum.incomplete,
        tasks[4]: RecordStatusEnum.complete,
        tasks[5]: RecordStatusEnum.incomplete,
    }
    for result in ret:
        assert result.status == ref_status[result.id]

    manager.await_results()
    ret = client.query_results(tasks)
    ref_status = {
        tasks[0]: RecordStatusEnum.complete,
        tasks[1]: RecordStatusEnum.complete,
        tasks[2]: RecordStatusEnum.complete,
        tasks[3]: RecordStatusEnum.complete,
        tasks[4]: RecordStatusEnum.complete,
        tasks[5]: RecordStatusEnum.incomplete,
    }
    for result in ret:
        assert result.status == ref_status[result.id]

    # Check that tag list is correctly validated to not include None
    # This could be implemented, but would require greater sophistication
    # in SQLAlchemySocket.task.claim()
    with pytest.raises(TypeError):
        queue.QueueManager(client, adapter, queue_tag=["tag1", None])


@pytest.mark.slow
@testing.using_rdkit
def test_queue_manager_log_statistics(compute_adapter_fixture, caplog):
    """Test statistics are correctly generated"""
    client, server, adapter = compute_adapter_fixture

    manager = queue.QueueManager(client, adapter, cores_per_task=1, memory_per_task=1, verbose=True)

    hooh = ptl.data.get_molecule("hooh.json")
    client.add_compute("rdkit", "UFF", "", "energy", None, [hooh], tag="other")

    # Set capture level
    with caplog_handler_at_level(caplog, logging.INFO):
        # Pull jobs to manager
        manager.update()
        # Tasks should not have been started yet
        assert "Task statistics unavailable" in caplog.text
        assert "Task Stats: Processed" not in caplog.text
        manager.await_results()
        # Ensure text is at least generated
        assert "Task Stats: Processed" in caplog.text
        assert "Core Usage vs. Max Resources" in caplog.text
        # Ensure some kind of stats are being calculated seemingly correctly
        stats_re = re.search(r"Core Usage Efficiency: (\d+\.\d+)%", caplog.text)
        assert stats_re is not None and float(stats_re.group(1)) != 0.0

    # Clean up capture so it does not flood the output
    caplog.records.clear()
    caplog.handler.records.clear()

    # Record a heartbeat
    timestamp = datetime.datetime.utcnow()
    manager.heartbeat()

    storage_socket = server.get_storage_socket()
    manager_records = storage_socket.manager.get(name=[manager.name()])
    _, logs = storage_socket.manager.query_logs(manager_id=[manager_records[0]["id"]])

    # Grab just the last log
    meta, latest_log = storage_socket.manager.query_logs(manager_id=[manager_records[0]["id"]], after=timestamp)
    assert len(latest_log) >= 1
    assert meta.n_found >= 1
    assert meta.n_returned >= 1
    assert len(latest_log) < len(logs)
    state = latest_log[0]

    assert state["completed"] == 1
    assert state["total_task_walltime"] > 0
    assert state["total_worker_walltime"] > 0
    assert state["total_worker_walltime"] >= state["total_task_walltime"]
    assert state["active_memory"] > 0


@testing.using_rdkit
def test_queue_manager_shutdown(compute_adapter_fixture):
    """Tests to ensure tasks are returned to queue when the manager shuts down"""
    client, server, adapter = compute_adapter_fixture

    manager = queue.QueueManager(client, adapter)

    hooh = ptl.data.get_molecule("hooh.json")
    client.add_compute("rdkit", "UFF", "", "energy", None, [hooh], tag="other")

    # Pull job to manager and shutdown
    manager.update()
    assert len(manager.list_current_tasks()) == 1

    shutdown = manager.shutdown()
    assert shutdown["nshutdown"] == 1, shutdown["info"]

    sman = client.query_managers(name=manager.name(), status="INACTIVE")
    assert len(sman) == 1
    assert sman[0]["status"] == ManagerStatusEnum.inactive

    # Boot new manager and await results
    manager = queue.QueueManager(client, adapter)
    manager.await_results()
    ret = client.query_results()
    assert len(ret) == 1
    assert ret[0].status == RecordStatusEnum.complete


@testing.using_rdkit
def test_queue_manager_server_delay(compute_adapter_fixture):
    """Test to ensure interrupts to the server shutdown correctly"""
    client, server, adapter = compute_adapter_fixture

    manager = queue.QueueManager(client, adapter, server_error_retries=1)

    hooh = ptl.data.get_molecule("hooh.json")
    client.add_compute("rdkit", "UFF", "", "energy", None, [hooh], tag="other")

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


@testing.using_rdkit
def test_queue_manager_heartbeat(compute_adapter_fixture):
    """Tests to ensure managers are marked as inactive when heartbeat times are exceeded"""

    client, server, adapter = compute_adapter_fixture
    server.start_periodics()

    hooh = ptl.data.get_molecule("hooh.json")
    ret = client.add_compute("rdkit", "UFF", "", "energy", None, [hooh], tag="other")

    manager = queue.QueueManager(client, adapter, queue_tag="other")

    manager.update()
    cur_tasks = manager.list_current_tasks()
    assert len(cur_tasks) > 0

    tasks = client.query_tasks(status="WAITING")
    assert len(tasks) == 0

    # By default, the manager isn't contacting the server with heartbeats
    # So just wait out the right amount of time and the server should mark it as inactive
    heartbeat_freq = server._qcf_config.heartbeat_frequency
    max_missed = server._qcf_config.heartbeat_max_missed
    wait_time = heartbeat_freq * (max_missed + 1) + 2
    time.sleep(wait_time)

    sman = client.query_managers(name=manager.name(), status="INACTIVE")
    assert len(sman) == 1
    assert sman[0]["status"] == ManagerStatusEnum.inactive

    tasks = client.query_tasks(status="WAITING")
    assert len(tasks) == 1


def test_manager_max_tasks_limiter(compute_adapter_fixture):
    client, server, adapter = compute_adapter_fixture

    manager = queue.QueueManager(client, adapter, queue_tag="stuff", max_tasks=1.0e9)
    assert manager.max_tasks < 1.0e9


def test_queue_manager_testing():

    with Pool(processes=2, initializer=_initialize_signals_process_pool) as adapter:
        manager = queue.QueueManager(None, adapter)

        assert manager.test()


def test_node_parallel(compute_adapter_fixture):
    """Test functionality related to note parallel jobs"""
    client, server, adapter = compute_adapter_fixture

    manager = queue.QueueManager(client, adapter, nodes_per_task=2, cores_per_rank=2)
    assert manager.queue_adapter.qcengine_local_options["nnodes"] == 2
    assert manager.queue_adapter.qcengine_local_options["cores_per_rank"] == 2
