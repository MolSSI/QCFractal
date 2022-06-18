from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError

from qcfractal.components.records.singlepoint.testing_helpers import submit_test_data
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

    client = snowflake.client()
    manager = client.get_managers([name2, name1])
    assert len(manager) == 2

    assert manager[0].name == name2
    assert manager[0].tags == ["tag1"]
    assert manager[0].status == ManagerStatusEnum.active
    assert manager[0].created_on > time_1
    assert manager[0].modified_on > time_1
    assert manager[0].created_on < time_2
    assert manager[0].modified_on < time_2

    assert manager[1].name == name1
    assert manager[1].cluster == "test_cluster"
    assert manager[1].hostname == "a_host"
    assert manager[1].tags == ["tag1", "tag2"]
    assert manager[1].status == ManagerStatusEnum.active
    assert manager[1].created_on > time_0
    assert manager[1].modified_on > time_0
    assert manager[1].created_on < time_1
    assert manager[1].modified_on < time_1


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

    manager = client.get_managers(mname1.fullname)
    assert manager.tags == ["tag1", "tag3", "tag2"]
    assert manager.programs == {"program1": "v3.0", "program2": "v4.0"}


def test_manager_mclient_activate_notags(snowflake: TestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)

    with pytest.raises(ValidationError, match=r"field contains no non-zero-length tags") as err:
        mclient1.activate(
            manager_version="v2.0",
            qcengine_version="v1.0",
            programs={"program1": "v3.0"},
            tags=[],
        )

    with pytest.raises(ValidationError, match=r"field contains no non-zero-length tags") as err:
        mclient1.activate(
            manager_version="v2.0",
            qcengine_version="v1.0",
            programs={"program1": "v3.0"},
            tags=[""],
        )


def test_manager_mclient_activate_noprogs(snowflake: TestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)

    with pytest.raises(ValidationError, match=r"field contains no non-zero-length programs") as err:
        mclient1.activate(
            manager_version="v2.0",
            qcengine_version="v1.0",
            programs={},
            tags=["tag1"],
        )

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


def test_manager_mclient_deactivate(snowflake: TestingSnowflake):
    client = snowflake.client()

    id1, _ = submit_test_data(snowflake.get_storage_socket(), "sp_psi4_benzene_energy_1", "tag1")

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")

    mclient1 = snowflake.manager_client(mname1)
    mclient2 = snowflake.manager_client(mname2)

    mclient1.activate(
        manager_version="v2.0",
        qcengine_version="v1.0",
        programs={"psi4": "v3.0"},
        tags=["tag1", "tag2"],
    )

    mclient2.activate(
        manager_version="v2.0",
        qcengine_version="v1.0",
        programs={"psi4": "v3.0"},
        tags=["tag1"],
    )

    name1 = mname1.fullname
    name2 = mname2.fullname

    # client2 claims tasks
    mclient2.claim(1)
    mclient2.deactivate(
        total_worker_walltime=1.0, total_task_walltime=2.0, active_tasks=1, active_cores=2, active_memory=7.0
    )

    manager = client.get_managers([name1, name2])
    assert manager[0].name == name1
    assert manager[0].status == ManagerStatusEnum.active
    assert manager[1].name == name2
    assert manager[1].status == ManagerStatusEnum.inactive
    assert manager[1].total_worker_walltime == 1.0
    assert manager[1].total_task_walltime == 2.0
    assert manager[1].active_tasks == 1
    assert manager[1].active_cores == 2
    assert manager[1].active_memory == 7.0
    assert manager[1].claimed == 1

    record = client.get_records(id1)
    assert record.raw_data.status == "waiting"
    assert record.raw_data.manager_name is None


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

    client = snowflake.client()
    manager = client.get_managers(name1, include=["log"])
    assert len(manager.log) == 0

    # Now do a heartbeat
    mclient1.heartbeat(
        total_worker_walltime=1.234, total_task_walltime=5.678, active_tasks=3, active_cores=10, active_memory=3.45
    )

    time_2 = datetime.utcnow()

    manager = client.get_managers(name1, include=["log"])
    assert len(manager.log) == 1

    # Was the data stored in the manager
    assert manager.total_worker_walltime == 1.234
    assert manager.total_task_walltime == 5.678
    assert manager.active_tasks == 3
    assert manager.active_cores == 10
    assert manager.active_memory == 3.45
    assert manager.modified_on > time_1
    assert manager.modified_on < time_2

    # and the log
    log = manager.log[0]
    assert log.total_worker_walltime == 1.234
    assert log.total_task_walltime == 5.678
    assert log.active_tasks == 3
    assert log.active_cores == 10
    assert log.active_memory == 3.45
    assert log.timestamp == manager.modified_on

    # Now do another heartbeat
    mclient1.heartbeat(
        total_worker_walltime=2 * 1.234,
        total_task_walltime=2 * 5.678,
        active_tasks=2 * 3,
        active_cores=2 * 10,
        active_memory=2 * 3.45,
    )

    time_3 = datetime.utcnow()

    manager = client.get_managers(name1, include=["log"])
    assert len(manager.log) == 2

    # Was the data stored in the manager
    assert manager.total_worker_walltime == 2 * 1.234
    assert manager.total_task_walltime == 2 * 5.678
    assert manager.active_tasks == 2 * 3
    assert manager.active_cores == 2 * 10
    assert manager.active_memory == 2 * 3.45
    assert manager.modified_on > time_2
    assert manager.modified_on < time_3

    # and the log
    log = manager.log[0]
    assert log.total_worker_walltime == 2 * 1.234
    assert log.total_task_walltime == 2 * 5.678
    assert log.active_tasks == 2 * 3
    assert log.active_cores == 2 * 10
    assert log.active_memory == 2 * 3.45
    assert log.timestamp == manager.modified_on


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
