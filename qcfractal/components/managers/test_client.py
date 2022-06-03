from __future__ import annotations

from datetime import datetime

import pytest

from qcfractal.testing_helpers import TestingSnowflake
from qcportal import PortalRequestError, PortalClient
from qcportal.managers import ManagerName, ManagerStatusEnum


@pytest.fixture(scope="module")
def queryable_managers_client(module_temporary_database):
    db_config = module_temporary_database.config
    with TestingSnowflake(db_config, encoding="application/json") as server:

        for cluster_i in range(4):
            for host_i in range(10):
                for uuid_i in range(3):
                    mname = ManagerName(
                        cluster=f"test_cluster_{cluster_i}",
                        hostname=f"test_host_{host_i}",
                        uuid=f"1234-5678-1234-567{uuid_i}",
                    )

                    mclient = server.manager_client(mname)
                    mclient.activate(
                        manager_version="v2.0",
                        qcengine_version="v1.0",
                        programs={"qcprog": None, "qcprog2": "v3.0"},
                        tags=[f"tag_{cluster_i}", "tag2"],
                    )

                    if uuid_i == 0:
                        mclient.deactivate(1.0, 1.0, 1, 1, 1.0)

        yield server.client()


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
        qcengine_version="v1.0",
        programs={"qcprog": None, "qcprog2": "v3.0"},
        tags=["tag1", "tag2"],
    )

    time_1 = datetime.utcnow()
    mclient2.activate(
        manager_version="v2.0",
        qcengine_version="v1.0",
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


def test_manager_client_query(queryable_managers_client: PortalClient):
    query_res = queryable_managers_client.query_managers(status="active")
    assert query_res.current_meta.n_found == 80

    query_res = queryable_managers_client.query_managers(hostname="test_host_1")
    assert query_res.current_meta.n_found == 12

    query_res = queryable_managers_client.query_managers(hostname="test_host_1", status="inactive")
    assert query_res.current_meta.n_found == 4

    query_res = queryable_managers_client.query_managers(cluster="test_cluster_2", status="active")
    assert query_res.current_meta.n_found == 20

    query_res = queryable_managers_client.query_managers(
        name=["test_cluster_2-test_host_1-1234-5678-1234-5672", "test_cluster_1-test_host_2-1234-5678-1234-5671"]
    )
    assert query_res.current_meta.n_found == 2

    managers = list(query_res)
    query_res = queryable_managers_client.query_managers(manager_ids=[managers[0].id, managers[1].id])
    assert query_res.current_meta.n_found == 2
    assert all(x.log is None for x in query_res)

    query_res = queryable_managers_client.query_managers(manager_ids=[managers[0].id, managers[1].id], include=["log"])
    assert query_res.current_meta.n_found == 2
    assert all(x.log is not None for x in query_res)


def test_manager_client_query_empty_iter(queryable_managers_client: PortalClient):

    query_res = queryable_managers_client.query_managers()
    assert len(query_res.current_batch) < queryable_managers_client.api_limits["get_managers"]

    managers = list(query_res)
    assert len(managers) == 120


def test_manager_client_query_limit(queryable_managers_client: PortalClient):

    query_res = queryable_managers_client.query_managers(limit=19)
    assert query_res.current_meta.n_found == 120
    assert len(query_res.current_batch) < queryable_managers_client.api_limits["get_managers"]

    managers = list(query_res)
    assert len(managers) == 19

    query_res = queryable_managers_client.query_managers(hostname="test_host_1", limit=11)
    assert query_res.current_meta.n_found == 12

    managers = list(query_res)
    assert len(managers) == 11
