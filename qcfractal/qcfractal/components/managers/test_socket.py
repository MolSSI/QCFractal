from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcportal.exceptions import ComputeManagerError
from qcportal.managers import ManagerName, ManagerStatusEnum

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def test_manager_socket_deactivate_before_notasks(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")

    time_0 = datetime.utcnow()

    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    time_1 = datetime.utcnow()
    storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    name1 = mname1.fullname
    name2 = mname2.fullname

    deactivated = storage_socket.managers.deactivate(modified_before=time_0)
    assert deactivated == []

    deactivated = storage_socket.managers.deactivate(modified_before=time_1)
    assert deactivated == [name1]

    manager = storage_socket.managers.get([name1, name2])
    assert manager[0]["status"] == ManagerStatusEnum.inactive
    assert manager[1]["status"] == ManagerStatusEnum.active

    deactivated = storage_socket.managers.deactivate(modified_before=datetime.utcnow())
    assert deactivated == [name2]

    manager = storage_socket.managers.get([name1, name2])
    assert manager[0]["status"] == ManagerStatusEnum.inactive
    assert manager[1]["status"] == ManagerStatusEnum.inactive


def test_manager_socket_get_proj(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")

    storage_socket.managers.activate(
        mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": None, "qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    name1 = mname1.fullname
    name2 = mname2.fullname

    # Logs not included by default
    manager = storage_socket.managers.get([name1, name2])
    assert "log" not in manager[0]
    assert "log" not in manager[1]

    manager = storage_socket.managers.get([name1, name2], include=["cluster"])
    assert manager[0]["name"] == name1
    assert manager[1]["name"] == name2
    assert set(manager[0].keys()) == {"id", "name", "cluster"}  # name & id always returned
    assert set(manager[1].keys()) == {"id", "name", "cluster"}

    manager = storage_socket.managers.get([name1, name2], include=["name", "log"])
    assert set(manager[0].keys()) == {"id", "name", "log"}
    assert set(manager[1].keys()) == {"id", "name", "log"}

    # cannot exclude name
    manager = storage_socket.managers.get([name1, name2], exclude=["cluster", "name"])
    assert "name" in manager[0]
    assert "name" in manager[1]
    assert "cluster" not in manager[0]
    assert "cluster" not in manager[1]


def test_manager_socket_update_resource_stats_nonexist(storage_socket: SQLAlchemySocket):
    with pytest.raises(ComputeManagerError, match=r"does not exist"):
        storage_socket.managers.update_resource_stats(
            "no_manager_name",
            total_worker_walltime=1.234,
            total_task_walltime=5.678,
            active_tasks=3,
            active_cores=10,
            active_memory=3.45,
        )
