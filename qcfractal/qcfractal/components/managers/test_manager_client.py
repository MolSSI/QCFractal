from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

try:
    from pydantic.v1 import ValidationError
except ImportError:
    from pydantic import ValidationError

from qcfractal.components.singlepoint.testing_helpers import submit_test_data
from qcportal import PortalRequestError
from qcportal.managers import ManagerName, ManagerStatusEnum
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcarchivetesting.testing_classes import QCATestingSnowflake


def test_manager_mclient_activate(snowflake: QCATestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")

    mclient1 = snowflake.manager_client(mname1)
    mclient2 = snowflake.manager_client(mname2)

    time_0 = now_at_utc()
    mclient1.activate(
        manager_version="v2.0",
        programs={"qcengine": ["unknown"], "program1": ["v3.0"]},
        compute_tags=["tag1", "tag2"],
    )

    time_1 = now_at_utc()
    mclient2.activate(
        manager_version="v2.0",
        programs={"qcengine": ["unknown"], "program1": ["v3.0"]},
        compute_tags=["tag1"],
    )
    time_2 = now_at_utc()

    name1 = mname1.fullname
    name2 = mname2.fullname

    client = snowflake.client()
    manager = client.get_managers([name2, name1])
    assert len(manager) == 2

    assert manager[0].name == name2
    assert manager[0].compute_tags == ["tag1"]
    assert manager[0].status == ManagerStatusEnum.active
    assert manager[0].created_on > time_1
    assert manager[0].modified_on > time_1
    assert manager[0].created_on < time_2
    assert manager[0].modified_on < time_2

    assert manager[1].name == name1
    assert manager[1].cluster == "test_cluster"
    assert manager[1].hostname == "a_host"
    assert manager[1].compute_tags == ["tag1", "tag2"]
    assert manager[1].status == ManagerStatusEnum.active
    assert manager[1].created_on > time_0
    assert manager[1].modified_on > time_0
    assert manager[1].created_on < time_1
    assert manager[1].modified_on < time_1


def test_manager_mclient_activate_normalize(snowflake: QCATestingSnowflake):
    # Activation where tags & programs are mixed case
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)
    client = snowflake.client()

    mclient1.activate(
        manager_version="v2.0",
        programs={"qcengine": ["unknown"], "program1": ["v3.0"], "PROgRam2": ["v4.0"], "PROGRAM4": ["v5.0-AB"]},
        compute_tags=["tag1", "taG3", "tAg2", "TAG3", "TAG1"],
    )

    manager = client.get_managers(mname1.fullname)
    assert manager.compute_tags == ["tag1", "tag3", "tag2"]
    assert manager.programs == {
        "qcengine": ["unknown"],
        "program1": ["v3.0"],
        "program2": ["v4.0"],
        "program4": ["v5.0-ab"],
    }


def test_manager_mclient_activate_notags(snowflake: QCATestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)

    with pytest.raises(ValidationError, match=r"field contains no non-zero-length tags") as err:
        mclient1.activate(
            manager_version="v2.0",
            programs={"qcengine": ["unknown"], "program1": ["v3.0"]},
            compute_tags=[],
        )

    with pytest.raises(ValidationError, match=r"field contains no non-zero-length tags") as err:
        mclient1.activate(
            manager_version="v2.0",
            programs={"qcengine": ["unknown"], "program1": ["v3.0"]},
            compute_tags=[""],
        )


def test_manager_mclient_activate_noprogs(snowflake: QCATestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)

    with pytest.raises(ValidationError, match=r"field contains no non-zero-length programs") as err:
        mclient1.activate(
            manager_version="v2.0",
            programs={},
            compute_tags=["tag1"],
        )

    with pytest.raises(ValidationError, match=r"field contains no non-zero-length programs") as err:
        mclient1.activate(
            manager_version="v2.0",
            programs={"": ["unknown"]},
            compute_tags=["tag1"],
        )


def test_manager_mclient_activate_duplicate(snowflake: QCATestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)
    mclient1.activate(
        manager_version="v2.0",
        programs={"qcengine": ["unknown"], "program1": ["v3.0"]},
        compute_tags=["tag1"],
    )

    with pytest.raises(PortalRequestError, match=r"already exists") as err:
        mclient1.activate(
            manager_version="v2.0",
            programs={"qcengine": ["unknown"], "program1": ["v3.0"]},
            compute_tags=["tag1"],
        )


def test_manager_mclient_deactivate(snowflake: QCATestingSnowflake):
    client = snowflake.client()

    id1, _ = submit_test_data(snowflake.get_storage_socket(), "sp_psi4_benzene_energy_1", "tag1")

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")
    mprog1 = {"qcengine": ["unknown"], "psi4": ["v3.0"]}

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")
    mprog2 = {"qcengine": ["unknown"], "psi4": ["v3.0"]}

    mclient1 = snowflake.manager_client(mname1)
    mclient2 = snowflake.manager_client(mname2)

    mclient1.activate(
        manager_version="v2.0",
        programs=mprog1,
        compute_tags=["tag1", "tag2"],
    )

    mclient2.activate(
        manager_version="v2.0",
        programs=mprog2,
        compute_tags=["tag1"],
    )

    name1 = mname1.fullname
    name2 = mname2.fullname

    # client2 claims tasks
    mclient2.claim(mprog2, ["tag1"], 1)
    mclient2.deactivate(total_cpu_hours=1.0, active_tasks=1, active_cores=2, active_memory=7.0)

    manager = client.get_managers([name1, name2])
    assert manager[0].name == name1
    assert manager[0].status == ManagerStatusEnum.active
    assert manager[1].name == name2
    assert manager[1].status == ManagerStatusEnum.inactive
    assert manager[1].total_cpu_hours == 1.0
    assert manager[1].active_tasks == 1
    assert manager[1].active_cores == 2
    assert manager[1].active_memory == 7.0
    assert manager[1].claimed == 1

    record = client.get_records(id1)
    assert record.status == "waiting"
    assert record.manager_name is None


def test_manager_mclient_deactivate_deactivated(snowflake: QCATestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)

    time_0 = now_at_utc()
    mclient1.activate(
        manager_version="v2.0",
        programs={"qcengine": ["unkonwn"], "program1": ["v3.0"]},
        compute_tags=["tag1", "tag2"],
    )

    mclient1.deactivate(total_cpu_hours=2.0, active_tasks=1, active_cores=2, active_memory=7.0)

    with pytest.raises(PortalRequestError, match=r"is not active") as err:
        mclient1.deactivate(total_cpu_hours=2.0, active_tasks=1, active_cores=2, active_memory=7.0)


def test_manager_mclient_heartbeat(snowflake: QCATestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)

    time_0 = now_at_utc()
    mclient1.activate(
        manager_version="v2.0",
        programs={"qcengine": ["unknown"], "program1": ["v3.0"]},
        compute_tags=["tag1", "tag2"],
    )

    time_1 = now_at_utc()

    name1 = mname1.fullname

    client = snowflake.client()
    manager = client.get_managers(name1)

    # Now do a heartbeat
    mclient1.heartbeat(total_cpu_hours=5.678, active_tasks=3, active_cores=10, active_memory=3.45)

    time_2 = now_at_utc()

    manager = client.get_managers(name1)

    # Was the data stored in the manager
    assert manager.total_cpu_hours == 5.678
    assert manager.active_tasks == 3
    assert manager.active_cores == 10
    assert manager.active_memory == 3.45
    assert manager.modified_on > time_1
    assert manager.modified_on < time_2

    # Now do another heartbeat
    mclient1.heartbeat(
        total_cpu_hours=2 * 5.678,
        active_tasks=2 * 3,
        active_cores=2 * 10,
        active_memory=2 * 3.45,
    )

    time_3 = now_at_utc()

    manager = client.get_managers(name1)

    # Was the data stored in the manager
    assert manager.total_cpu_hours == 2 * 5.678
    assert manager.active_tasks == 2 * 3
    assert manager.active_cores == 2 * 10
    assert manager.active_memory == 2 * 3.45
    assert manager.modified_on > time_2
    assert manager.modified_on < time_3


def test_manager_mclient_heartbeat_deactivated(snowflake: QCATestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    mclient1 = snowflake.manager_client(mname1)

    mclient1.activate(
        manager_version="v2.0",
        programs={"qcengine": ["unknown"], "program1": ["v3.0"]},
        compute_tags=["tag1", "tag2"],
    )

    mclient1.deactivate(total_cpu_hours=2.0, active_tasks=1, active_cores=2, active_memory=7.0)

    with pytest.raises(PortalRequestError, match=r"is not active") as err:
        mclient1.heartbeat(total_cpu_hours=5.678, active_tasks=3, active_cores=10, active_memory=3.45)
