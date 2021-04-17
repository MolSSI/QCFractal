"""
Tests the manager subsocket
"""

import pytest
from datetime import datetime, timedelta


def test_manager_basic(storage_socket):

    manager_info = {
        "cluster": "test_cluster",
        "hostname": "test_hostname",
        "username": "test_username",
        "uuid": "1234-4567-7890",
        "tag": "test_tag",
        "status": "ACTIVE",
    }
    assert storage_socket.manager.update(name="first_manager", **manager_info)
    ret = storage_socket.manager.get(name=["first_manager"])
    assert all(ret[0][k] == v for k, v in manager_info.items())

    # Updating with submitted,completed,failures should add, rather than replace
    assert storage_socket.manager.update(name="first_manager", submitted=100, completed=200, failures=300, log=True)
    assert storage_socket.manager.update(name="first_manager", submitted=50, completed=100, failures=150, log=True)
    ret = storage_socket.manager.get(name=["first_manager"], include_logs=True)
    assert ret[0]["submitted"] == 150
    assert ret[0]["completed"] == 300
    assert ret[0]["failures"] == 450
    assert len(ret[0]["logs"]) == 2

    # If we don't want log, they shouldn't be there
    ret = storage_socket.manager.get(name=["first_manager"])
    assert "logs" not in ret[0]


def test_manager_get_nonexist(storage_socket):

    ret = storage_socket.manager.get(name=["manager_does_not_exist"], missing_ok=True)
    assert ret == [None]

    with pytest.raises(RuntimeError, match=r"Could not find all requested manager records"):
        storage_socket.manager.get(name=["manager_does_not_exist"])


def test_manager_deactivate_name(storage_socket):

    manager_info = {
        "cluster": "test_cluster",
        "hostname": "test_hostname",
        "username": "test_username",
        "uuid": "1234-4567-7890",
        "tag": "test_tag",
        "status": "ACTIVE",
    }
    assert storage_socket.manager.update(name="first_manager", **manager_info)
    ret = storage_socket.manager.get(name=["first_manager"])
    assert ret[0]["status"] == "ACTIVE"

    # Deactivate nothing?
    n = storage_socket.manager.deactivate(name=["first_manager_nonexist"])
    assert n == []
    ret = storage_socket.manager.get(name=["first_manager"])
    assert ret[0]["status"] == "ACTIVE"

    # Now deactivate a real manager
    n = storage_socket.manager.deactivate(name=["first_manager"])
    assert n == ["first_manager"]

    ret = storage_socket.manager.get(name=["first_manager"])
    assert ret[0]["status"] == "INACTIVE"


def test_manager_deactivate_time(storage_socket):

    manager_info = {
        "cluster": "test_cluster",
        "hostname": "test_hostname",
        "username": "test_username",
        "uuid": "1234-4567-7890",
        "tag": "test_tag",
        "status": "ACTIVE",
    }
    assert storage_socket.manager.update(name="first_manager", **manager_info)
    ret = storage_socket.manager.get(name=["first_manager"])
    assert ret[0]["status"] == "ACTIVE"

    # Deactivate nothing (modified_before is an hour ago)
    n = storage_socket.manager.deactivate(modified_before=datetime.utcnow() - timedelta(seconds=3600))
    assert n == []
    ret = storage_socket.manager.get(name=["first_manager"])
    assert ret[0]["status"] == "ACTIVE"

    # Now deactivate real managers
    n = storage_socket.manager.deactivate(modified_before=datetime.utcnow())
    assert n == ["first_manager"]
    ret = storage_socket.manager.get(name=["first_manager"])
    assert ret[0]["status"] == "INACTIVE"


def test_manager_query(storage_socket):

    manager_info = [
        {
            "cluster": "test_cluster",
            "hostname": "test_hostname" + str(i % 3),
            "username": "test_username",
            "uuid": "1234-4567-789" + str(i),
            "tag": "test_tag",
            "status": "ACTIVE",
        }
        for i in range(10)
    ]

    for i, m in enumerate(manager_info):
        assert storage_socket.manager.update(name=f"first_manager{i}", **m)

    meta, ret = storage_socket.manager.query(name=["first_manager0", "first_manager1"])
    assert meta.success
    assert meta.n_returned == 2
    assert len(ret) == 2

    meta, ret = storage_socket.manager.query(name=["first_manager0"], include=["cluster"])
    assert meta.success
    assert meta.n_returned == 1
    assert len(ret) == 1
    assert ret[0] == {"cluster": "test_cluster"}  # We only returned the cluster

    meta, ret = storage_socket.manager.query(
        hostname=["test_hostname0"], include=["name", "uuid"], modified_before=datetime.utcnow()
    )
    assert meta.success
    assert meta.n_returned == 4
    assert ret[0]["uuid"] == "1234-4567-7890"
    assert ret[1]["uuid"] == "1234-4567-7893"
    assert ret[2]["uuid"] == "1234-4567-7896"
    assert ret[3]["uuid"] == "1234-4567-7899"
