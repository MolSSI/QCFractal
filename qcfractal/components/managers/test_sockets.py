"""
Tests the managers subsocket
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest

from qcfractal.portal.exceptions import ComputeManagerError, MissingDataError
from qcfractal.portal.managers import ManagerName, ManagerStatusEnum

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket


def test_manager_socket_activate_get(storage_socket: SQLAlchemySocket):
    time_0 = datetime.utcnow()

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")

    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    time_1 = datetime.utcnow()
    storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1"],
    )
    time_2 = datetime.utcnow()

    name1 = mname1.fullname
    name2 = mname2.fullname

    # Test getting duplicates
    manager = storage_socket.managers.get([name2, name1, name1, name2])
    assert len(manager) == 4
    assert manager[1]["name"] == name1
    assert manager[1]["cluster"] == "test_cluster"
    assert manager[1]["hostname"] == "a_host"
    assert manager[1]["username"] == "bill"
    assert manager[1]["tags"] == ["tag1", "tag2"]
    assert manager[1]["status"] == ManagerStatusEnum.active
    assert manager[1]["created_on"] > time_0
    assert manager[1]["modified_on"] > time_0
    assert manager[1]["created_on"] < time_1
    assert manager[1]["modified_on"] < time_1

    assert manager[0]["name"] == name2
    assert manager[0]["tags"] == ["tag1"]
    assert manager[0]["status"] == ManagerStatusEnum.active
    assert manager[0]["created_on"] > time_1
    assert manager[0]["modified_on"] > time_1
    assert manager[0]["created_on"] < time_2
    assert manager[0]["modified_on"] < time_2

    assert manager[2] == manager[1]
    assert manager[3] == manager[0]


def test_manager_socket_activate_normalize(storage_socket: SQLAlchemySocket):
    # Activation where tags & programs are mixed case
    # Also, duplicate tags are specified
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"program1": "v3.0", "PROgRam2": "v4.0"},
        tags=["tag1", "taG3", "tAg2", "TAG3", "TAG1"],
    )

    manager = storage_socket.managers.get([mname1.fullname])
    assert len(manager) == 1

    assert manager[0]["tags"] == ["tag1", "tag3", "tag2"]
    assert manager[0]["programs"] == {"program1": "v3.0", "program2": "v4.0"}


def test_manager_socket_activate_notags_1(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    with pytest.raises(ComputeManagerError, match=r"does not have any tags assigned") as error:
        storage_socket.managers.activate(
            name_data=mname1,
            manager_version="v2.0",
            qcengine_version="v1.0",
            username="bill",
            programs={"qcprog": None, "qcprog2": "v3.0"},
            tags=[],
        )

    # The server should be telling this manager to give up and shut down
    assert error.value.shutdown is True


def test_manager_socket_activate_notags_2(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    with pytest.raises(ComputeManagerError, match=r"does not have any tags assigned") as error:
        storage_socket.managers.activate(
            name_data=mname1,
            manager_version="v2.0",
            qcengine_version="v1.0",
            username="bill",
            programs={"qcprog": None, "qcprog2": "v3.0"},
            tags=[""],
        )

    # The server should be telling this manager to give up and shut down
    assert error.value.shutdown is True


def test_manager_socket_activate_noprogs_1(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    with pytest.raises(ComputeManagerError, match=r"does not have any programs available") as error:
        storage_socket.managers.activate(
            name_data=mname1,
            manager_version="v2.0",
            qcengine_version="v1.0",
            username="bill",
            programs={},
            tags=["tag1"],
        )

    # The server should be telling this manager to give up and shut down
    assert error.value.shutdown is True


def test_manager_socket_activate_noprogs_2(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    with pytest.raises(ComputeManagerError, match=r"does not have any programs available") as error:
        storage_socket.managers.activate(
            name_data=mname1,
            manager_version="v2.0",
            qcengine_version="v1.0",
            username="bill",
            programs={"": None},
            tags=["tag1"],
        )

    # The server should be telling this manager to give up and shut down
    assert error.value.shutdown is True


def test_manager_socket_activate_duplicate(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    # Same hostname, cluster, uuid
    with pytest.raises(ComputeManagerError, match=r"already exists") as error:
        storage_socket.managers.activate(
            name_data=mname1,
            manager_version="v2.0",
            qcengine_version="v1.0",
            username="bill",
            programs={"qcprog": None, "qcprog2": "v3.0"},
            tags=["*"],
        )

    # The server should be telling this manager to give up and shut down
    assert error.value.shutdown is True


def test_manager_socket_deactivate_byname_notasks(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")

    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    name1 = mname1.fullname
    name2 = mname2.fullname

    deactivated = storage_socket.managers.deactivate([name1])
    assert deactivated == [name1]

    manager = storage_socket.managers.get([name1, name2])
    assert manager[0]["name"] == name1
    assert manager[0]["status"] == ManagerStatusEnum.inactive
    assert manager[1]["name"] == name2
    assert manager[1]["status"] == ManagerStatusEnum.active


def test_manager_socket_deactivate_before_notasks(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")

    time_0 = datetime.utcnow()

    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    time_1 = datetime.utcnow()
    storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
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


def test_manager_socket_deactivate_deactivated(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")

    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    name1 = mname1.fullname
    name2 = mname2.fullname

    deactivated = storage_socket.managers.deactivate([name1])
    assert deactivated == [name1]

    # deactivation will only work on active managers
    # so only name2 will be deactivated here
    deactivated = storage_socket.managers.deactivate([name1, name2])
    assert deactivated == [name2]


def test_manager_socket_get_nonexist(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    manager = storage_socket.managers.get(["noname"], missing_ok=True)
    assert manager == [None]

    with pytest.raises(MissingDataError):
        storage_socket.managers.get(["noname", mname1.fullname], missing_ok=False)


def test_manager_socket_get_empty(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    manager = storage_socket.managers.get([])
    assert manager == []


def test_manager_socket_get_proj(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")

    storage_socket.managers.activate(
        mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
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


def test_manager_socket_update_resource_stats(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    time_0 = datetime.utcnow()
    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    time_1 = datetime.utcnow()

    name1 = mname1.fullname

    # Logs should be empty
    manager = storage_socket.managers.get([name1], include=["*", "log"])
    assert manager[0]["log"] == []

    # Now update the stats
    storage_socket.managers.update_resource_stats(
        name1,
        total_worker_walltime=1.234,
        total_task_walltime=5.678,
        active_tasks=3,
        active_cores=10,
        active_memory=3.45,
    )

    time_2 = datetime.utcnow()

    # Should be something in the logs now
    manager = storage_socket.managers.get([name1], include=["*", "log"])
    assert len(manager[0]["log"]) == 1

    # Was the data stored in the manager
    assert manager[0]["total_worker_walltime"] == 1.234
    assert manager[0]["total_task_walltime"] == 5.678
    assert manager[0]["active_tasks"] == 3
    assert manager[0]["active_cores"] == 10
    assert manager[0]["active_memory"] == 3.45
    assert manager[0]["modified_on"] > time_1
    assert manager[0]["modified_on"] < time_2

    # and the log
    log = manager[0]["log"][0]
    assert log["total_worker_walltime"] == 1.234
    assert log["total_task_walltime"] == 5.678
    assert log["active_tasks"] == 3
    assert log["active_cores"] == 10
    assert log["active_memory"] == 3.45
    assert log["timestamp"] == manager[0]["modified_on"]

    # Add another
    storage_socket.managers.update_resource_stats(
        name1,
        total_worker_walltime=2 * 1.234,
        total_task_walltime=2 * 5.678,
        active_tasks=2 * 3,
        active_cores=2 * 10,
        active_memory=2 * 3.45,
    )

    manager = storage_socket.managers.get([name1], include=["*", "log"])

    # Was the data stored in the manager
    assert manager[0]["total_worker_walltime"] == 2 * 1.234
    assert manager[0]["total_task_walltime"] == 2 * 5.678
    assert manager[0]["active_tasks"] == 2 * 3
    assert manager[0]["active_cores"] == 2 * 10
    assert manager[0]["active_memory"] == 2 * 3.45

    # and the log
    # logs should be newest first
    log = manager[0]["log"][0]
    assert log["total_worker_walltime"] == 2 * 1.234
    assert log["total_task_walltime"] == 2 * 5.678
    assert log["active_tasks"] == 2 * 3
    assert log["active_cores"] == 2 * 10
    assert log["active_memory"] == 2 * 3.45
    assert log["timestamp"] == manager[0]["modified_on"]
    assert manager[0]["log"][0]["timestamp"] > manager[0]["log"][1]["timestamp"]


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


def test_manager_socket_update_inactive(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    storage_socket.managers.deactivate([mname1.fullname])

    with pytest.raises(ComputeManagerError, match=r"is not active"):
        storage_socket.managers.update_resource_stats(
            mname1.fullname,
            total_worker_walltime=1.234,
            total_task_walltime=5.678,
            active_tasks=3,
            active_cores=10,
            active_memory=3.45,
        )


def test_manager_socket_query(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    mname2 = ManagerName(cluster="test_cluster_2", hostname="a_host_2", uuid="1234-5678-1234-5679")

    time_0 = datetime.utcnow()
    storage_socket.managers.activate(
        mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    time_1 = datetime.utcnow()
    storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    time_2 = datetime.utcnow()
    name1 = mname1.fullname
    name2 = mname2.fullname

    storage_socket.managers.deactivate([name2])

    # Logs not included by default
    meta, managers = storage_socket.managers.query(name=[name1, name2, name1, name2])
    assert len(managers) == 2
    assert "log" not in managers[0]
    assert "log" not in managers[1]

    meta, managers = storage_socket.managers.query(hostname=["a_host"])
    assert meta.n_found == 1
    assert managers[0]["hostname"] == "a_host"

    meta, managers = storage_socket.managers.query(cluster=["test_cluster_2"])
    assert meta.n_found == 1
    assert managers[0]["cluster"] == "test_cluster_2"

    meta, managers = storage_socket.managers.query(modified_before=time_0)
    assert meta.n_found == 0

    meta, managers = storage_socket.managers.query(modified_before=time_1)
    assert meta.n_found == 1
    assert managers[0]["hostname"] == "a_host"

    meta, managers = storage_socket.managers.query(modified_after=time_1)
    assert meta.n_found == 1
    assert managers[0]["hostname"] == "a_host_2"

    meta, managers = storage_socket.managers.query(status=[ManagerStatusEnum.active])
    assert meta.n_found == 1
    assert managers[0]["hostname"] == "a_host"

    meta, managers = storage_socket.managers.query(status=[ManagerStatusEnum.active, ManagerStatusEnum.inactive])
    assert meta.n_found == 2

    meta, managers = storage_socket.managers.query(
        status=[ManagerStatusEnum.active, ManagerStatusEnum.inactive], limit=1
    )
    assert meta.n_found == 2
    assert meta.n_returned == 1

    meta, managers = storage_socket.managers.query(
        status=[ManagerStatusEnum.active, ManagerStatusEnum.inactive], skip=1
    )
    assert meta.n_found == 2
    assert meta.n_returned == 1


def test_manager_socket_query_proj(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    mname2 = ManagerName(cluster="test_cluster_2", hostname="a_host_2", uuid="1234-5678-1234-5679")

    time_0 = datetime.utcnow()
    storage_socket.managers.activate(
        mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    time_1 = datetime.utcnow()
    storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    time_2 = datetime.utcnow()
    name1 = mname1.fullname
    name2 = mname2.fullname

    storage_socket.managers.deactivate([name2])

    # Logs not included by default
    meta, managers = storage_socket.managers.query(name=[name1, name2, name1, name2], include=["*", "log"])
    assert len(managers) == 2
    assert "log" in managers[0]
    assert "log" in managers[1]

    meta, managers = storage_socket.managers.query(name=[name1, name2, name1, name2], include=["claimed"])
    assert set(managers[0].keys()) == {"id", "claimed"}

    meta, managers = storage_socket.managers.query(name=[name1, name2, name1, name2], exclude=["claimed", "hostname"])
    assert "claimed" not in managers[0]
    assert "hostname" not in managers[0]
    assert "claimed" not in managers[1]
    assert "hostname" not in managers[1]
    assert "failures" in managers[0]
    assert "failures" in managers[1]
