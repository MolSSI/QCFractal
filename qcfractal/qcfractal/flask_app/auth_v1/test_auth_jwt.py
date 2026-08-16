import time

import jwt
import pytest

from qcarchivetesting.testing_classes import (
    QCATestingSnowflake,
)
from qcportal import PortalRequestError
from qcportal.exceptions import AuthenticationFailure


@pytest.mark.slow
def test_jwt_refresh(secure_snowflake):
    client = secure_snowflake.user_client("submit_user")
    time.sleep(client._jwt_access_exp - time.time() + 1)
    client.list_datasets()


@pytest.mark.slow
def test_jwt_refresh_user_newrole(secure_snowflake):
    admin_client = secure_snowflake.user_client("admin_user")
    client = secure_snowflake.user_client("submit_user")

    submit_info = client.get_user()
    assert submit_info.role == "submit"
    decoded_access = jwt.decode(client._jwt_access_token, algorithms=["HS256"], options={"verify_signature": False})
    assert decoded_access["role"] == "submit"

    uinfo = admin_client.get_user("submit_user")
    uinfo.role = "read"
    admin_client.modify_user(uinfo)

    time.sleep(client._jwt_access_exp - time.time() + 1)

    client.list_datasets()  # will refresh token as needed
    decoded_access = jwt.decode(client._jwt_access_token, algorithms=["HS256"], options={"verify_signature": False})
    assert decoded_access["role"] == "read"


@pytest.mark.slow
def test_jwt_refresh_user_disabled(secure_snowflake):
    admin_client = secure_snowflake.user_client("admin_user")
    client = secure_snowflake.user_client("submit_user")

    uinfo = admin_client.get_user("submit_user")
    uinfo.enabled = False
    admin_client.modify_user(uinfo)

    time.sleep(client._jwt_access_exp - time.time() + 1)

    with pytest.raises(AuthenticationFailure, match="User account has been disabled"):
        client.list_datasets()


@pytest.mark.slow
def test_jwt_disabled_before_refresh(postgres_server, client_encoding):
    # Disabling an account must take effect within the server-side re-verify cache lifetime (~5s),
    # not only after the access token expires and is refreshed. Use a long access-token lifetime
    # so the token is still valid (and no refresh happens) when we make the post-disable request.
    pg_harness = postgres_server.get_new_harness("jwt_disabled_before_refresh")
    with QCATestingSnowflake(
        pg_harness,
        encoding=client_encoding,
        create_users=True,
        enable_security=True,
        allow_unauthenticated_read=False,
        extra_config={"api": {"jwt_access_token_expires": 60}},
    ) as snowflake:
        admin_client = snowflake.user_client("admin_user")
        client = snowflake.user_client("submit_user")

        # Works to start with
        client.list_datasets()

        uinfo = admin_client.get_user("submit_user")
        uinfo.enabled = False
        admin_client.modify_user(uinfo)

        # Wait past the re-verify cache lifetime. The access token is still valid, so the client
        # does not refresh -- the server must reject the request based on the disabled account.
        time.sleep(7)
        assert client._jwt_access_exp - time.time() > 5  # token still valid; this is not the refresh path

        with pytest.raises(PortalRequestError, match="is disabled"):
            client.list_datasets()


@pytest.mark.slow
def test_jwt_role_downgrade_before_refresh(postgres_server, client_encoding):
    # Downgrading a user's role must take effect within the re-verify cache lifetime, even though
    # the still-valid access token carries the old (higher) role in its claims.
    pg_harness = postgres_server.get_new_harness("jwt_role_downgrade_before_refresh")
    with QCATestingSnowflake(
        pg_harness,
        encoding=client_encoding,
        create_users=True,
        enable_security=True,
        allow_unauthenticated_read=False,
        extra_config={"api": {"jwt_access_token_expires": 60}},
    ) as snowflake:
        admin_client = snowflake.user_client("admin_user")
        client = snowflake.user_client("submit_user")

        # The submit role can add datasets
        client.add_dataset("singlepoint", "ds_before_downgrade")

        uinfo = admin_client.get_user("submit_user")
        uinfo.role = "read"
        admin_client.modify_user(uinfo)

        # The token still carries role=submit
        decoded = jwt.decode(client._jwt_access_token, algorithms=["HS256"], options={"verify_signature": False})
        assert decoded["role"] == "submit"

        # Wait past the re-verify cache lifetime without letting the token expire
        time.sleep(7)
        assert client._jwt_access_exp - time.time() > 5  # token still valid; this is not the refresh path

        # The read role cannot add datasets, so the downgrade must be enforced despite the stale claim
        with pytest.raises(PortalRequestError, match="Forbidden"):
            client.add_dataset("singlepoint", "ds_after_downgrade")


@pytest.mark.slow
def test_jwt_refresh_user_deleted(postgres_server, client_encoding):
    # Need its own snowflake because we need logging disabled
    # Otherwise, the user cannot be deleted because it is referenced in the access log table

    pg_harness = postgres_server.get_new_harness("jwt_user_deleted")
    with QCATestingSnowflake(
        pg_harness,
        encoding=client_encoding,
        create_users=True,
        enable_security=True,
        allow_unauthenticated_read=False,
        log_access=False,
    ) as snowflake:
        admin_client = snowflake.user_client("admin_user")
        client = snowflake.user_client("submit_user")

        admin_client.delete_user("submit_user")
        time.sleep(client._jwt_access_exp - time.time() + 1)

        with pytest.raises(AuthenticationFailure, match="User account no longer exists"):
            client.list_datasets()
