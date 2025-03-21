from __future__ import annotations

import pytest

from qcportal import PortalClient
from qcportal.managers import ManagerName


@pytest.fixture(scope="module")
def queryable_managers_client(session_snowflake):
    for cluster_i in range(4):
        for host_i in range(10):
            for uuid_i in range(3):
                mname = ManagerName(
                    cluster=f"test_cluster_{cluster_i}",
                    hostname=f"test_host_{host_i}",
                    uuid=f"1234-5678-1234-567{uuid_i}",
                )

                mclient = session_snowflake.manager_client(mname)
                mclient.activate(
                    manager_version="v2.0",
                    programs={"qcengine": ["unknown"], "qcprog": ["unknown"], "qcprog2": ["v3.0"]},
                    compute_tags=[f"tag_{cluster_i}", "tag2"],
                )

                if uuid_i == 0:
                    mclient.deactivate(1, 1, 1.0, 1.0)

    yield session_snowflake.client()
    session_snowflake.reset()


def test_manager_client_query(queryable_managers_client: PortalClient):
    query_res = queryable_managers_client.query_managers(status="active")
    query_res_l = list(query_res)
    assert len(query_res_l) == 80

    query_res = queryable_managers_client.query_managers(hostname="test_host_1")
    query_res_l = list(query_res)
    assert len(query_res_l) == 12

    query_res = queryable_managers_client.query_managers(hostname="test_host_1", status="inactive")
    query_res_l = list(query_res)
    assert len(query_res_l) == 4

    query_res = queryable_managers_client.query_managers(cluster="test_cluster_2", status="active")
    query_res_l = list(query_res)
    assert len(query_res_l) == 20

    query_res = queryable_managers_client.query_managers(
        name=["test_cluster_2-test_host_1-1234-5678-1234-5672", "test_cluster_1-test_host_2-1234-5678-1234-5671"]
    )
    managers = list(query_res)
    assert len(managers) == 2

    query_res = queryable_managers_client.query_managers(manager_id=[managers[0].id, managers[1].id])
    query_res_l = list(query_res)
    assert len(query_res_l) == 2


def test_manager_client_query_empty_iter(queryable_managers_client: PortalClient):
    query_res = queryable_managers_client.query_managers()
    assert len(query_res._current_batch) < queryable_managers_client.api_limits["get_managers"]

    managers = list(query_res)
    assert len(managers) == 120


def test_manager_client_query_limit(queryable_managers_client: PortalClient):
    query_res = queryable_managers_client.query_managers(limit=19)
    query_res_l = list(query_res)
    assert len(query_res_l) == 19
    assert len(query_res._current_batch) < queryable_managers_client.api_limits["get_managers"]

    query_res = queryable_managers_client.query_managers(hostname="test_host_1", limit=11)
    query_res_l = list(query_res)
    assert len(query_res_l) == 11
