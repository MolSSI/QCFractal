"""
Client-level tests for API tokens, driven through PortalClient against a running server
"""

import datetime

import pytest

from qcportal import PortalClient
from qcportal.auth import API_TOKEN_PREFIX
from qcportal.client_base import PortalRequestError
from qcportal.exceptions import AuthenticationFailure
from qcportal.utils import now_at_utc


def test_api_token_client_crud_me(secure_snowflake):
    client = secure_snowflake.user_client("admin_user")

    new_token = client.create_api_token(name="laptop")
    assert new_token.token.startswith(API_TOKEN_PREFIX)
    assert new_token.info.name == "laptop"

    listed = client.list_api_tokens()
    assert len(listed) == 1
    assert listed[0].id == new_token.info.id
    # The secret is never in a listing
    assert not hasattr(listed[0], "token")

    client.delete_api_token(new_token.info.id)
    assert client.list_api_tokens() == []


def test_api_token_client_connect(secure_snowflake):
    admin = secure_snowflake.user_client("admin_user")
    new_token = admin.create_api_token(name="for connecting")

    # Build a brand-new client authenticated only by the token
    token_client = secure_snowflake.client(api_token=new_token.token)
    assert token_client.username == "admin_user"
    assert token_client.user_id is not None

    # And it can actually make requests
    assert token_client.list_api_tokens() is not None


def test_api_token_client_role_inherited(secure_snowflake):
    admin = secure_snowflake.user_client("admin_user")
    read_user_id = admin.get_user("read_user").id
    new_token = admin.create_api_token(name="ro", username_or_id="read_user")

    token_client = secure_snowflake.client(api_token=new_token.token)
    # read role cannot list users
    with pytest.raises(PortalRequestError, match="not authorized|Forbidden"):
        token_client.list_users()


def test_api_token_client_admin_for_other_user(secure_snowflake):
    admin = secure_snowflake.user_client("admin_user")

    new_token = admin.create_api_token(name="on behalf", username_or_id="read_user")
    assert new_token.info.user_id == admin.get_user("read_user").id

    listed = admin.list_api_tokens("read_user")
    assert len(listed) == 1

    admin.delete_api_token(new_token.info.id, username_or_id="read_user")
    assert admin.list_api_tokens("read_user") == []


def test_api_token_client_nonadmin_cannot_manage_others(secure_snowflake):
    read_client = secure_snowflake.user_client("read_user")
    with pytest.raises(PortalRequestError, match="not authorized|Forbidden"):
        read_client.create_api_token(name="x", username_or_id="admin_user")


def test_api_token_client_maintain_can_list_not_mint(secure_snowflake):
    maintain = secure_snowflake.user_client("maintain_user")

    # maintain has users:read, so can list another user's tokens
    assert maintain.list_api_tokens("read_user") == []

    # ... but not create them (users:modify is admin-only)
    with pytest.raises(PortalRequestError, match="not authorized|Forbidden"):
        maintain.create_api_token(name="x", username_or_id="read_user")


def test_api_token_client_cannot_delete_others_via_me(secure_snowflake):
    admin = secure_snowflake.user_client("admin_user")
    other = admin.create_api_token(name="admins token")

    read_client = secure_snowflake.user_client("read_user")
    # read_user tries to delete admin's token id through their own /me endpoint
    with pytest.raises(PortalRequestError, match="not found"):
        read_client.delete_api_token(other.info.id)

    # admin's token still works
    token_client = secure_snowflake.client(api_token=other.token)
    assert token_client.user_id is not None


def test_api_token_client_revocation(secure_snowflake):
    admin = secure_snowflake.user_client("admin_user")
    new_token = admin.create_api_token("revoke_test")

    token_client = secure_snowflake.client(api_token=new_token.token)
    assert token_client.list_api_tokens() is not None

    admin.delete_api_token(new_token.info.id)

    # Token verification is briefly cached; clear it to stand in for the cache expiring
    from qcfractal.flask_app.flask_app import token_verifier

    token_verifier.reset_all()

    with pytest.raises(PortalRequestError):
        token_client.list_api_tokens()


def test_api_token_client_mutual_exclusion(secure_snowflake):
    with pytest.raises(ValueError, match="both an api_token and a username"):
        secure_snowflake.client(username="admin_user", password="something123", api_token="qcf_x")


def test_api_token_client_bad_prefix(secure_snowflake):
    with pytest.raises(ValueError, match="does not look like a valid API token"):
        secure_snowflake.client(api_token="not-a-token")


def test_api_token_client_invalid_token_fails_fast(secure_snowflake):
    # A well-formed-but-unknown token: /me returns 401, which is fatal at construction
    with pytest.raises((AuthenticationFailure, PortalRequestError)):
        secure_snowflake.client(api_token="qcf_thisisnotarealtoken")


def test_api_token_client_datetime_roundtrip(secure_snowflake):
    admin = secure_snowflake.user_client("admin_user")
    expires = now_at_utc() + datetime.timedelta(days=1)
    new_token = admin.create_api_token(name="expiring", expires_at=expires)

    assert new_token.info.expires_at is not None
    # Survives with timezone info
    assert new_token.info.expires_at.tzinfo is not None
    assert abs((new_token.info.expires_at - expires).total_seconds()) < 5


def test_api_token_client_scope_roundtrip(secure_snowflake):
    # The scope placeholder survives the wire in both directions
    admin = secure_snowflake.user_client("admin_user")

    new_token = admin.create_api_token("scoped", scope="unlimited")
    assert new_token.info.scope == "unlimited"
    assert admin.list_api_tokens()[0].scope == "unlimited"

    # Requesting a scope the server does not know is rejected client-side by the strict body model
    with pytest.raises(Exception, match="unlimited|validation"):
        admin.create_api_token("bad_scope", scope="read_only")
