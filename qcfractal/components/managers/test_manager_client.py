from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from qcportal import PortalRequestError
from qcportal.managers import ManagerName, ManagerStatusEnum

if TYPE_CHECKING:
    from qcfractal.testing_helpers import TestingSnowflake


def test_manager_mclient_activate(snowflake: TestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")

    mclient1 = snowflake.manager_client(mname1)
    mclient2 = snowflake.manager_client(mname2)

    time_0 = datetime.utcnow()
    mclient1.activate(
        manager_version="v2.0",
        qcengine_version="v1.0",
        programs={"program1": "v3.0"},
        tags=["tag1", "tag2"],
    )

    time_1 = datetime.utcnow()
    mclient2.activate(
        manager_version="v2.0",
        qcengine_version="v1.0",
        programs={"program1": "v3.0"},
        tags=["tag1"],
    )
    time_2 = datetime.utcnow()

    name1 = mname1.fullname
    name2 = mname2.fullname

    storage_socket = snowflake.get_storage_socket()
    manager = storage_socket.managers.get([name2, name1])
    assert len(manager) == 2

    assert manager[0]["name"] == name2
    assert manager[0]["tags"] == ["tag1"]
    assert manager[0]["status"] == ManagerStatusEnum.active
    assert manager[0]["created_on"] > time_1
    assert manager[0]["modified_on"] > time_1
    assert manager[0]["created_on"] < time_2
    assert manager[0]["modified_on"] < time_2

    assert manager[1]["name"] == name1
    assert manager[1]["cluster"] == "test_cluster"
    assert manager[1]["hostname"] == "a_host"
    assert manager[1]["tags"] == ["tag1", "tag2"]
    assert manager[1]["status"] == ManagerStatusEnum.active
    assert manager[1]["created_on"] > time_0
    assert manager[1]["modified_on"] > time_0
    assert manager[1]["created_on"] < time_1
    assert manager[1]["modified_on"] < time_1


def test_manager_mclient_activate_normalize(snowflake: TestingSnowflake):
    # Activation where tags & programs are mixed case
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)
    client = snowflake.client()

    mclient1.activate(
        manager_version="v2.0",
        qcengine_version="v1.0",
        programs={"program1": "v3.0", "PROgRam2": "v4.0"},
        tags=["tag1", "taG3", "tAg2", "TAG3", "TAG1"],
    )

    manager = client.get_managers([mname1.fullname])
    assert manager[0].tags == ["tag1", "tag3", "tag2"]
    assert manager[0].programs == {"program1": "v3.0", "program2": "v4.0"}


def test_manager_mclient_activate_notags_1(snowflake: TestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)

    with pytest.raises(ValidationError, match=r"field contains no non-zero-length tags") as err:
        mclient1.activate(
            manager_version="v2.0",
            qcengine_version="v1.0",
            programs={"program1": "v3.0"},
            tags=[],
        )


def test_manager_mclient_activate_notags_2(snowflake: TestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)

    with pytest.raises(ValidationError, match=r"field contains no non-zero-length tags") as err:
        mclient1.activate(
            manager_version="v2.0",
            qcengine_version="v1.0",
            programs={"program1": "v3.0"},
            tags=[""],
        )


def test_manager_mclient_activate_noprogs_1(snowflake: TestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)

    with pytest.raises(ValidationError, match=r"field contains no non-zero-length programs") as err:
        mclient1.activate(
            manager_version="v2.0",
            qcengine_version="v1.0",
            programs={},
            tags=["tag1"],
        )


def test_manager_mclient_activate_noprogs_2(snowflake: TestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)

    with pytest.raises(ValidationError, match=r"field contains no non-zero-length programs") as err:
        mclient1.activate(
            manager_version="v2.0",
            qcengine_version="v1.0",
            programs={"": None},
            tags=["tag1"],
        )


def test_manager_mclient_activate_duplicate(snowflake: TestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)
    mclient1.activate(
        manager_version="v2.0",
        qcengine_version="v1.0",
        programs={"program1": "v3.0"},
        tags=["tag1"],
    )

    with pytest.raises(PortalRequestError, match=r"already exists") as err:
        mclient1.activate(
            manager_version="v2.0",
            qcengine_version="v1.0",
            programs={"program1": "v3.0"},
            tags=["tag1"],
        )


def test_manager_mclient_deactivate_notasks(snowflake: TestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")

    mclient1 = snowflake.manager_client(mname1)
    mclient2 = snowflake.manager_client(mname2)

    time_0 = datetime.utcnow()
    mclient1.activate(
        manager_version="v2.0",
        qcengine_version="v1.0",
        programs={"program1": "v3.0"},
        tags=["tag1", "tag2"],
    )

    time_1 = datetime.utcnow()
    mclient2.activate(
        manager_version="v2.0",
        qcengine_version="v1.0",
        programs={"program1": "v3.0"},
        tags=["tag1"],
    )
    time_2 = datetime.utcnow()

    name1 = mname1.fullname
    name2 = mname2.fullname

    mclient2.deactivate(
        total_worker_walltime=1.0, total_task_walltime=2.0, active_tasks=1, active_cores=2, active_memory=7.0
    )

    storage_socket = snowflake.get_storage_socket()
    manager = storage_socket.managers.get([name1, name2])
    assert manager[0]["name"] == name1
    assert manager[0]["status"] == ManagerStatusEnum.active
    assert manager[1]["name"] == name2
    assert manager[1]["status"] == ManagerStatusEnum.inactive
    assert manager[1]["total_worker_walltime"] == 1.0
    assert manager[1]["total_task_walltime"] == 2.0
    assert manager[1]["active_tasks"] == 1
    assert manager[1]["active_cores"] == 2
    assert manager[1]["active_memory"] == 7.0


def test_manager_mclient_deactivate_deactivated(snowflake: TestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)

    time_0 = datetime.utcnow()
    mclient1.activate(
        manager_version="v2.0",
        qcengine_version="v1.0",
        programs={"program1": "v3.0"},
        tags=["tag1", "tag2"],
    )

    mclient1.deactivate(
        total_worker_walltime=1.0, total_task_walltime=2.0, active_tasks=1, active_cores=2, active_memory=7.0
    )

    with pytest.raises(PortalRequestError, match=r"is not active") as err:
        mclient1.deactivate(
            total_worker_walltime=1.0, total_task_walltime=2.0, active_tasks=1, active_cores=2, active_memory=7.0
        )


def test_manager_mclient_heartbeat(snowflake: TestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)

    time_0 = datetime.utcnow()
    mclient1.activate(
        manager_version="v2.0",
        qcengine_version="v1.0",
        programs={"program1": "v3.0"},
        tags=["tag1", "tag2"],
    )

    time_1 = datetime.utcnow()

    name1 = mname1.fullname

    storage_socket = snowflake.get_storage_socket()
    manager = storage_socket.managers.get([name1], include=["*", "log"])
    assert len(manager[0]["log"]) == 0

    # Now do a heartbeat
    mclient1.heartbeat(
        total_worker_walltime=1.234, total_task_walltime=5.678, active_tasks=3, active_cores=10, active_memory=3.45
    )

    time_2 = datetime.utcnow()

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


def test_manager_mclient_heartbeat_deactivated(snowflake: TestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)

    mclient1.activate(
        manager_version="v2.0",
        qcengine_version="v1.0",
        programs={"program1": "v3.0"},
        tags=["tag1", "tag2"],
    )

    mclient1.deactivate(
        total_worker_walltime=1.0, total_task_walltime=2.0, active_tasks=1, active_cores=2, active_memory=7.0
    )

    with pytest.raises(PortalRequestError, match=r"is not active") as err:
        mclient1.heartbeat(
            total_worker_walltime=1.234, total_task_walltime=5.678, active_tasks=3, active_cores=10, active_memory=3.45
        )
