import time

import jwt
import pytest

from qcarchivetesting import test_users
from qcarchivetesting.testing_classes import (
    QCATestingSnowflake,
)
from qcfractal.components.auth.role_socket import default_roles
from qcportal.exceptions import AuthenticationFailure


@pytest.mark.slow
def test_jwt_refresh(secure_snowflake):
    client = secure_snowflake.client("submit_user", password=test_users["submit_user"]["pw"])
    time.sleep(client._jwt_access_exp - time.time() + 1)
    client.list_datasets()


@pytest.mark.slow
def test_jwt_refresh_user_newrole(secure_snowflake):
    admin_client = secure_snowflake.client("admin_user", password=test_users["admin_user"]["pw"])
    client = secure_snowflake.client("submit_user", password=test_users["submit_user"]["pw"])

    submit_info = client.get_user()
    assert submit_info.role == "submit"
    decoded_access = jwt.decode(client._jwt_access_token, algorithms=["HS256"], options={"verify_signature": False})
    assert decoded_access["role"] == "submit"
    assert decoded_access["permissions"] == default_roles["submit"]

    uinfo = admin_client.get_user("submit_user")
    uinfo.role = "read"
    admin_client.modify_user(uinfo)

    time.sleep(client._jwt_access_exp - time.time() + 1)

    client.list_datasets()  # will refresh token as needed
    decoded_access = jwt.decode(client._jwt_access_token, algorithms=["HS256"], options={"verify_signature": False})
    assert decoded_access["role"] == "read"
    assert decoded_access["permissions"] == default_roles["read"]


@pytest.mark.slow
def test_jwt_refresh_user_role_modified(secure_snowflake):
    admin_client = secure_snowflake.client("admin_user", password=test_users["admin_user"]["pw"])
    client = secure_snowflake.client("read_user", password=test_users["read_user"]["pw"])

    read_info = client.get_user()
    assert read_info.role == "read"
    decoded_access = jwt.decode(client._jwt_access_token, algorithms=["HS256"], options={"verify_signature": False})
    assert decoded_access["role"] == "read"
    assert decoded_access["permissions"] == default_roles["read"]

    rinfo = admin_client.get_role("read")
    rinfo.permissions.Statement.append(
        {"Effect": "Allow", "Action": "WRITE", "Resource": ["/api/v1/user", "/api/v1/role"]}
    )
    admin_client.modify_role(rinfo)

    time.sleep(client._jwt_access_exp - time.time() + 1)

    client.list_datasets()  # will refresh token as needed
    decoded_access = jwt.decode(client._jwt_access_token, algorithms=["HS256"], options={"verify_signature": False})
    assert decoded_access["role"] == "read"
    assert decoded_access["permissions"] == rinfo.permissions.dict()


@pytest.mark.slow
def test_jwt_refresh_user_disabled(secure_snowflake):
    admin_client = secure_snowflake.client("admin_user", password=test_users["admin_user"]["pw"])
    client = secure_snowflake.client("submit_user", password=test_users["submit_user"]["pw"])

    uinfo = admin_client.get_user("submit_user")
    uinfo.enabled = False
    admin_client.modify_user(uinfo)

    time.sleep(client._jwt_access_exp - time.time() + 1)

    with pytest.raises(AuthenticationFailure, match="User account has been disabled"):
        client.list_datasets()


@pytest.mark.slow
def test_jwt_refresh_user_deleted(postgres_server, pytestconfig):
    # Need its own snowflake because we need logging disabled
    # Otherwise, the user cannot be deleted because it is referenced in the access log table

    pg_harness = postgres_server.get_new_harness("jwt_user_deleted")
    encoding = pytestconfig.getoption("--client-encoding")
    with QCATestingSnowflake(
        pg_harness,
        encoding=encoding,
        create_users=True,
        enable_security=True,
        allow_unauthenticated_read=False,
        log_access=False,
    ) as snowflake:
        admin_client = snowflake.client("admin_user", password=test_users["admin_user"]["pw"])
        client = snowflake.client("submit_user", password=test_users["submit_user"]["pw"])

        admin_client.delete_user("submit_user")
        time.sleep(client._jwt_access_exp - time.time() + 1)

        with pytest.raises(AuthenticationFailure, match="User account no longer exists"):
            client.list_datasets()
