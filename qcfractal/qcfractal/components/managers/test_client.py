from __future__ import annotations

import pytest

from qcarchivetesting.testing_classes import QCATestingSnowflake
from qcportal import PortalRequestError
from qcportal.managers import ManagerName, ManagerStatusEnum
from qcportal.utils import now_at_utc


def test_manager_client_get(snowflake: QCATestingSnowflake):
    time_0 = now_at_utc()

    mname1 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5678")

    # UUID is different
    mname2 = ManagerName(cluster="test_cluster", hostname="a_host", uuid="1234-5678-1234-5679")

    client = snowflake.client()

    mclient1 = snowflake.manager_client(mname1, username="bill")
    mclient2 = snowflake.manager_client(mname2, username="bill")

    time_0 = now_at_utc()
    mclient1.activate(
        manager_version="v2.0",
        programs={"qcengine": ["unknown"], "qcprog": ["unknown"], "qcprog2": ["v3.0"]},
        compute_tags=["tag1", "tag2"],
    )

    time_1 = now_at_utc()
    mclient2.activate(
        manager_version="v2.0",
        programs={"qcengine": ["unknown"], "qcprog": ["unknown"], "qcprog2": ["v3.0"]},
        compute_tags=["tag1"],
    )
    time_2 = now_at_utc()

    name1 = mname1.fullname
    name2 = mname2.fullname

    # Test getting duplicates
    manager = client.get_managers([name2, name1, name1, name2])
    assert len(manager) == 4
    assert manager[1].name == name1
    assert manager[1].cluster == "test_cluster"
    assert manager[1].hostname == "a_host"
    assert manager[1].username == "bill"
    assert manager[1].compute_tags == ["tag1", "tag2"]
    assert manager[1].status == ManagerStatusEnum.active
    assert manager[1].created_on > time_0
    assert manager[1].modified_on > time_0
    assert manager[1].created_on < time_1
    assert manager[1].modified_on < time_1

    assert manager[0].name == name2
    assert manager[0].compute_tags == ["tag1"]
    assert manager[0].status == ManagerStatusEnum.active
    assert manager[0].created_on > time_1
    assert manager[0].modified_on > time_1
    assert manager[0].created_on < time_2
    assert manager[0].modified_on < time_2

    assert manager[2].id == manager[1].id
    assert manager[3].id == manager[0].id


def test_manager_client_get_nonexist(snowflake: QCATestingSnowflake):
    activated_manager_name, _ = snowflake.activate_manager()
    client = snowflake.client()
    manager = client.get_managers(["noname", activated_manager_name.fullname], missing_ok=True)
    assert manager[0] is None
    assert manager[1] is not None

    with pytest.raises(PortalRequestError):
        client.get_managers(["noname", activated_manager_name.fullname], missing_ok=False)


def test_manager_client_get_empty(snowflake: QCATestingSnowflake, activated_manager_name: ManagerName):
    # include activated_manager_name so that there is something in the db
    client = snowflake.client()
    manager = client.get_managers([], missing_ok=True)
    assert manager == []
