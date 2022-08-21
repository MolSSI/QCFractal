from __future__ import annotations

from datetime import datetime

import pytest

from qcfractal.testing_helpers import TestingSnowflake
from qcportal import PortalRequestError
from qcportal.managers import ManagerName, ManagerStatusEnum


def test_manager_client_get(snowflake: TestingSnowflake):
    time_0 = datetime.utcnow()

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")

    client = snowflake.client()

    mclient1 = snowflake.manager_client(mname1, username="bill")
    mclient2 = snowflake.manager_client(mname2, username="bill")

    time_0 = datetime.utcnow()
    mclient1.activate(
        manager_version="v2.0",
        programs={"qcengine": None, "qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    time_1 = datetime.utcnow()
    mclient2.activate(
        manager_version="v2.0",
        programs={"qcengine": None, "qcprog": None, "qcprog2": "v3.0"},
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
    assert manager[1].log is None

    assert manager[0].name == name2
    assert manager[0].tags == ["tag1"]
    assert manager[0].status == ManagerStatusEnum.active
    assert manager[0].created_on > time_1
    assert manager[0].modified_on > time_1
    assert manager[0].created_on < time_2
    assert manager[0].modified_on < time_2
    assert manager[0].log is None

    assert manager[2] == manager[1]
    assert manager[3] == manager[0]

    # Including logs
    manager = client.get_managers(name1, include=["log"])
    assert manager.log is not None


def test_manager_client_get_nonexist(snowflake: TestingSnowflake, activated_manager_name: ManagerName):
    client = snowflake.client()
    manager = client.get_managers(["noname", activated_manager_name.fullname], missing_ok=True)
    assert manager[0] is None
    assert manager[1] is not None

    with pytest.raises(PortalRequestError):
        client.get_managers(["noname", activated_manager_name.fullname], missing_ok=False)


def test_manager_client_get_empty(snowflake: TestingSnowflake, activated_manager_name: ManagerName):
    # include activated_manager_name so that there is something in the db
    client = snowflake.client()
    manager = client.get_managers([], missing_ok=True)
    assert manager == []
