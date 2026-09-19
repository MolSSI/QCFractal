"""
HTTP-level tests for authenticating with a long-lived API token

These drive raw requests against a running server (secure_snowflake), the way a third-party client
that only knows how to set a static Authorization header would.
"""

import pytest
import requests

from qcarchivetesting import test_users
from qcfractal.components.auth.db_models import UserAPITokenORM
from sqlalchemy import select


def _mint_token(snowflake, username, **kwargs):
    """Create a token for a user directly through the storage socket, returning the plaintext"""
    socket = snowflake.get_storage_socket()
    user_id = socket.users.get(username)["id"]
    raw, info = socket.auth.create_api_token(user_id, **kwargs)
    return raw, info


def _auth(token):
    return {"Authorization": f"Bearer {token}"}


def _n_internal_errors(snowflake) -> int:
    from qcfractal.components.serverinfo.db_models import InternalErrorLogORM

    socket = snowflake.get_storage_socket()
    with socket.session_scope(True) as session:
        return len(session.execute(select(InternalErrorLogORM.id)).all())


def test_api_token_auth_works(secure_snowflake):
    uri = secure_snowflake.get_uri()
    raw, _ = _mint_token(secure_snowflake, "admin_user")

    r = requests.get(f"{uri}/api/v1/information")
    assert r.status_code == 401  # no credentials

    r = requests.get(f"{uri}/api/v1/information", headers=_auth(raw))
    assert r.status_code == 200


def test_api_token_auth_role_inherited(secure_snowflake):
    uri = secure_snowflake.get_uri()
    raw, _ = _mint_token(secure_snowflake, "read_user")

    # A read token can read
    r = requests.get(f"{uri}/api/v1/information", headers=_auth(raw))
    assert r.status_code == 200

    # ... but not manage users (read role)
    r = requests.get(f"{uri}/api/v1/users", headers=_auth(raw))
    assert r.status_code == 403


@pytest.mark.parametrize(
    "header",
    [
        "Bearer qcf_garbagegarbagegarbage",
        "Bearer qcf_",
        "Bearer ",
        "Bearer not-a-jwt-at-all",
        "Basic qcf_x",
        "bearer qcf_alsogarbage",  # lowercase scheme, still 401
    ],
)
def test_api_token_auth_bad_header_is_401_not_500(secure_snowflake, header):
    uri = secure_snowflake.get_uri()
    before = _n_internal_errors(secure_snowflake)

    r = requests.get(f"{uri}/api/v1/information", headers={"Authorization": header})
    assert r.status_code == 401

    # Crucially, a bad token must not produce an internal server error
    after = _n_internal_errors(secure_snowflake)
    assert after == before


def test_api_token_auth_bad_token_not_silently_anonymous(secure_snowflake_allow_read):
    # On a server that allows unauthenticated read, a *bad* token must still be a 401 - never a
    # silent downgrade to anonymous access
    uri = secure_snowflake_allow_read.get_uri()

    # No header at all -> anonymous read works
    r = requests.get(f"{uri}/api/v1/information")
    assert r.status_code == 200

    # A bad token -> 401, not a degraded anonymous 200
    r = requests.get(f"{uri}/api/v1/information", headers=_auth("qcf_garbage"))
    assert r.status_code == 401


def test_api_token_auth_revocation_immediate(secure_snowflake):
    uri = secure_snowflake.get_uri()
    raw, info = _mint_token(secure_snowflake, "admin_user")

    r = requests.get(f"{uri}/api/v1/information", headers=_auth(raw))
    assert r.status_code == 200

    # Revoke and immediately try again - no waiting for a cache
    socket = secure_snowflake.get_storage_socket()
    user_id = socket.users.get("admin_user")["id"]
    socket.auth.delete_api_token(info["id"], user_id)

    r = requests.get(f"{uri}/api/v1/information", headers=_auth(raw))
    assert r.status_code == 401


def test_api_token_auth_expired(secure_snowflake):
    import datetime
    from qcportal.utils import now_at_utc

    uri = secure_snowflake.get_uri()
    raw, info = _mint_token(secure_snowflake, "admin_user")

    socket = secure_snowflake.get_storage_socket()
    with socket.session_scope() as session:
        orm = session.execute(select(UserAPITokenORM).where(UserAPITokenORM.id == info["id"])).scalar_one()
        orm.expires_at = now_at_utc() - datetime.timedelta(seconds=1)

    r = requests.get(f"{uri}/api/v1/information", headers=_auth(raw))
    assert r.status_code == 401


def test_api_token_auth_session_cookie_precedence(secure_snowflake):
    # An Authorization header always wins over a session cookie
    from qcfractal.flask_app.csrf import CSRF_HEADER

    uri = secure_snowflake.get_uri()

    # Log admin_user in via a browser session
    sess = requests.Session()
    sess.headers.update({CSRF_HEADER: "XMLHttpRequest"})
    r = sess.post(
        f"{uri}/auth/v1/session_login",
        json={"username": "admin_user", "password": test_users["admin_user"]["pw"]},
    )
    assert r.status_code == 200

    # A bad token in the header must not be ignored in favor of the cookie
    r = sess.get(f"{uri}/api/v1/information", headers=_auth("qcf_garbage"))
    assert r.status_code == 401


def test_api_token_auth_compute_endpoint(secure_snowflake):
    uri = secure_snowflake.get_uri()
    raw, _ = _mint_token(secure_snowflake, "compute_user")

    r = requests.get(f"{uri}/compute/v1/information", headers=_auth(raw))
    assert r.status_code == 200


def test_api_token_auth_security_disabled(snowflake):
    # With security disabled, token routes require security and a token is not usable to manage them
    uri = snowflake.get_uri()

    r = requests.get(f"{uri}/api/v1/information", headers=_auth("qcf_anything"))
    # Non-security endpoints are open, but a garbage token is still rejected before that
    assert r.status_code == 401

    # Token management requires security
    r = requests.get(f"{uri}/api/v1/me/tokens", headers=_auth("qcf_anything"))
    assert r.status_code == 401


def test_api_token_auth_jwt_still_works(secure_snowflake):
    # A regression smoke test: username/password JWT login is unaffected
    uri = secure_snowflake.get_uri()
    r = requests.post(
        f"{uri}/auth/v1/login",
        json={"username": "admin_user", "password": test_users["admin_user"]["pw"]},
    )
    assert r.status_code == 200
    access_token = r.json()["access_token"]

    r = requests.get(f"{uri}/api/v1/information", headers=_auth(access_token))
    assert r.status_code == 200


def test_api_token_client_security_disabled_bootstrap(snowflake):
    # A token client against a security-disabled server: /me returns 401, but that must not fail
    # client construction. The client just cannot learn its own identity.
    from qcportal import PortalClient

    # Mint a token directly (security is off, but the table and socket still work)
    socket = snowflake.get_storage_socket()
    # With security disabled there are no real users; create one to own the token
    from qcportal.auth import UserInfo

    socket.users.add(UserInfo(username="tok_owner", role="admin", enabled=True), password="a_password_123")
    user_id = socket.users.get("tok_owner")["id"]
    raw, _ = socket.auth.create_api_token(user_id)

    # Construction must succeed even though /me is unavailable
    client = PortalClient(snowflake.get_uri(), api_token=raw)
    assert client.user_id is None  # identity could not be determined, but no error
