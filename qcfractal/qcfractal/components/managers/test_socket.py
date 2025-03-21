from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qcfractal.components.managers.db_models import ComputeManagerORM
from qcportal.exceptions import ComputeManagerError
from qcportal.managers import ManagerName, ManagerStatusEnum
from qcportal.utils import now_at_utc

if TYPE_CHECKING:
    from qcfractal.db_socket import SQLAlchemySocket
    from sqlalchemy.orm.session import Session


def test_manager_socket_deactivate_before_notasks(storage_socket: SQLAlchemySocket, session: Session):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")

    time_0 = now_at_utc()

    mid1 = storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": ["unknown"], "qcprog": ["unknown"], "qcprog2": ["v3.0"]},
        compute_tags=["tag1", "tag2"],
    )

    time_1 = now_at_utc()
    mid2 = storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": ["unknown"], "qcprog": ["unknown"], "qcprog2": ["v3.0"]},
        compute_tags=["tag1", "tag2"],
    )

    name1 = mname1.fullname
    name2 = mname2.fullname

    deactivated = storage_socket.managers.deactivate(modified_before=time_0)
    assert deactivated == []

    deactivated = storage_socket.managers.deactivate(modified_before=time_1)
    assert deactivated == [name1]

    manager1 = session.get(ComputeManagerORM, mid1)
    manager2 = session.get(ComputeManagerORM, mid2)
    assert manager1.status == ManagerStatusEnum.inactive
    assert manager2.status == ManagerStatusEnum.active

    deactivated = storage_socket.managers.deactivate(modified_before=now_at_utc())
    assert deactivated == [name2]

    session.expire_all()
    manager1 = session.get(ComputeManagerORM, mid1)
    manager2 = session.get(ComputeManagerORM, mid2)
    assert manager1.status == ManagerStatusEnum.inactive
    assert manager2.status == ManagerStatusEnum.inactive


def test_manager_socket_get_proj(storage_socket: SQLAlchemySocket):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")

    storage_socket.managers.activate(
        mname1,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": ["unknown"], "qcprog": ["unknown"], "qcprog2": ["v3.0"]},
        compute_tags=["tag1", "tag2"],
    )

    storage_socket.managers.activate(
        name_data=mname2,
        manager_version="v2.0",
        username="bill",
        programs={"qcengine": ["unknown"], "qcprog": ["unknown"], "qcprog2": ["v3.0"]},
        compute_tags=["tag1", "tag2"],
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

    manager = storage_socket.managers.get([name1, name2], include=["name"])
    assert set(manager[0].keys()) == {"id", "name"}
    assert set(manager[1].keys()) == {"id", "name"}

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
            total_cpu_hours=1.234,
            active_tasks=3,
            active_cores=10,
            active_memory=3.45,
        )
