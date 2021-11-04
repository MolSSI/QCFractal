"""
Tests the managers client
"""

from datetime import datetime

import pytest

from qcfractal.portal.client import PortalRequestError
from qcfractal.portal.components.managers import ManagerName, ManagerStatusEnum
from qcfractal.testing import TestingSnowflake


def test_manager_client_get(snowflake: TestingSnowflake):
    time_0 = datetime.utcnow()

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")

    storage_socket = snowflake.get_storage_socket()
    client = snowflake.client()

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
    manager = client.get_managers([name2, name1, name1, name2])
    assert len(manager) == 4
    assert manager[1].name == name1
    assert manager[1].cluster == "test_cluster"
    assert manager[1].hostname == "a_host"
    assert manager[1].username == "bill"
    assert manager[1].tags == ["tag1", "tag2"]
    assert manager[1].status == ManagerStatusEnum.active
    assert manager[1].created_on > time_0
    assert manager[1].modified_on > time_0
    assert manager[1].created_on < time_1
    assert manager[1].modified_on < time_1

    assert manager[0].name == name2
    assert manager[0].tags == ["tag1"]
    assert manager[0].status == ManagerStatusEnum.active
    assert manager[0].created_on > time_1
    assert manager[0].modified_on > time_1
    assert manager[0].created_on < time_2
    assert manager[0].modified_on < time_2

    assert manager[2] == manager[1]
    assert manager[3] == manager[0]


def test_manager_client_get_nonexist(snowflake: TestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    storage_socket = snowflake.get_storage_socket()
    client = snowflake.client()

    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    manager = client.get_managers(["noname"], missing_ok=True)
    assert manager == [None]

    with pytest.raises(PortalRequestError):
        client.get_managers(["noname", mname1.fullname], missing_ok=False)


def test_manager_client_get_empty(snowflake: TestingSnowflake):
    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    storage_socket = snowflake.get_storage_socket()
    client = snowflake.client()

    storage_socket.managers.activate(
        name_data=mname1,
        manager_version="v2.0",
        qcengine_version="v1.0",
        username="bill",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    manager = client.get_managers([], missing_ok=True)
    assert manager == []


def test_manager_client_query(snowflake: TestingSnowflake):
    storage_socket = snowflake.get_storage_socket()
    client = snowflake.client()

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
    meta, managers = client.query_managers(name=[name1, name2, name1, name2])
    assert len(managers) == 2
    assert managers[0].log is None
    assert managers[1].log is None

    meta, managers = client.query_managers(hostname=["a_host"])
    assert meta.n_found == 1
    assert managers[0].hostname == "a_host"

    meta, managers = client.query_managers(cluster=["test_cluster_2"])
    assert meta.n_found == 1
    assert managers[0].cluster == "test_cluster_2"

    meta, managers = client.query_managers(modified_before=time_0)
    assert meta.n_found == 0

    meta, managers = client.query_managers(modified_before=time_1)
    assert meta.n_found == 1
    assert managers[0].hostname == "a_host"

    meta, managers = client.query_managers(modified_after=time_1)
    assert meta.n_found == 1
    assert managers[0].hostname == "a_host_2"

    meta, managers = client.query_managers(status=[ManagerStatusEnum.active])
    assert meta.n_found == 1
    assert managers[0].hostname == "a_host"

    meta, managers = client.query_managers(status=[ManagerStatusEnum.active, ManagerStatusEnum.inactive])
    assert meta.n_found == 2

    meta, managers = client.query_managers(status=[ManagerStatusEnum.active, ManagerStatusEnum.inactive], limit=1)
    assert meta.n_found == 2
    assert meta.n_returned == 1

    meta, managers = client.query_managers(status=[ManagerStatusEnum.active, ManagerStatusEnum.inactive], skip=1)
    assert meta.n_found == 2
    assert meta.n_returned == 1

    # Empty query
    meta, managers = client.query_managers()
    assert len(managers) == 2
