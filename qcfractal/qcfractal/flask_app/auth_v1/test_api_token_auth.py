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


_mint_counter = [0]


def _mint_token(snowflake, username, name=None, **kwargs):
    """Create a token for a user directly through the storage socket, returning the plaintext"""
    socket = snowflake.get_storage_socket()
    user_id = socket.users.get(username)["id"]
    if name is None:
        _mint_counter[0] += 1
        name = f"token_{_mint_counter[0]}"
    raw, info = socket.auth.create_api_token(user_id, name, **kwargs)
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


def test_api_token_auth_revocation(secure_snowflake):
    # Token verification is briefly cached, so revocation takes effect within the cache lifetime.
    # (The socket-level test covers that the underlying lookup is immediate.) Here we clear the
    # cache to stand in for its expiry and confirm the revoked token is then rejected.
    from qcfractal.flask_app.flask_app import token_verifier

    uri = secure_snowflake.get_uri()
    raw, info = _mint_token(secure_snowflake, "admin_user")

    r = requests.get(f"{uri}/api/v1/information", headers=_auth(raw))
    assert r.status_code == 200

    socket = secure_snowflake.get_storage_socket()
    user_id = socket.users.get("admin_user")["id"]
    socket.auth.delete_api_token(info["id"], user_id)
    token_verifier.reset_all()  # stand in for the cache expiring

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
    raw, _ = socket.auth.create_api_token(user_id, "bootstrap_test")

    # Construction must succeed even though /me is unavailable
    client = PortalClient(snowflake.get_uri(), api_token=raw)
    assert client.user_id is None  # identity could not be determined, but no error


def test_api_token_auth_empty_header_is_401(secure_snowflake):
    # A present-but-empty Authorization header must be a 401, not a fall back to anonymous
    uri = secure_snowflake.get_uri()
    before = _n_internal_errors(secure_snowflake)

    r = requests.get(f"{uri}/api/v1/information", headers={"Authorization": ""})
    assert r.status_code == 401
    assert _n_internal_errors(secure_snowflake) == before


def test_api_token_auth_empty_header_ignores_cookie(secure_snowflake):
    # An empty Authorization header must not be ignored in favor of a valid session cookie
    from qcfractal.flask_app.csrf import CSRF_HEADER

    uri = secure_snowflake.get_uri()
    sess = requests.Session()
    sess.headers.update({CSRF_HEADER: "XMLHttpRequest"})
    r = sess.post(
        f"{uri}/auth/v1/session_login",
        json={"username": "admin_user", "password": test_users["admin_user"]["pw"]},
    )
    assert r.status_code == 200

    # Cookie is valid, but the explicit (empty) Authorization header takes precedence -> 401
    r = sess.get(f"{uri}/api/v1/information", headers={"Authorization": ""})
    assert r.status_code == 401


def test_api_token_auth_recorded_in_access_log(secure_snowflake):
    # A token-authenticated request records which token made it, for attribution
    from qcportal.serverinfo.models import AccessLogQueryFilters

    uri = secure_snowflake.get_uri()
    raw, info = _mint_token(secure_snowflake, "admin_user")

    r = requests.get(f"{uri}/api/v1/information", headers=_auth(raw))
    assert r.status_code == 200

    socket = secure_snowflake.get_storage_socket()
    accesses = socket.serverinfo.query_access_log(AccessLogQueryFilters())
    # Find the information request made with the token
    token_accesses = [a for a in accesses if a["api_token_id"] == info["id"]]
    assert len(token_accesses) >= 1

    # A password/JWT request has no api_token_id
    r = requests.post(
        f"{uri}/auth/v1/login",
        json={"username": "admin_user", "password": test_users["admin_user"]["pw"]},
    )
    jwt = r.json()["access_token"]
    requests.get(f"{uri}/api/v1/information", headers=_auth(jwt))

    accesses = socket.serverinfo.query_access_log(AccessLogQueryFilters())
    info_via_jwt = [
        a for a in accesses if a["full_uri"].endswith("/information") and a["api_token_id"] is None
    ]
    assert len(info_via_jwt) >= 1


def test_api_token_cannot_create_token(secure_snowflake):
    # A token-authenticated request may not mint another token (would defeat revocation)
    uri = secure_snowflake.get_uri()
    raw, _ = _mint_token(secure_snowflake, "admin_user")

    r = requests.post(
        f"{uri}/api/v1/me/tokens",
        headers={**_auth(raw), "Content-Type": "application/json"},
        data='{"name": "sneaky"}',
    )
    assert r.status_code == 403
    assert "API token" in r.json()["msg"]

    # An admin token also cannot create a token for another user
    r = requests.post(
        f"{uri}/api/v1/users/read_user/tokens",
        headers={**_auth(raw), "Content-Type": "application/json"},
        data='{"name": "sneaky"}',
    )
    assert r.status_code == 403


def test_api_token_cannot_change_password(secure_snowflake):
    # A token-authenticated request may not change a password (persistence / lockout vector)
    uri = secure_snowflake.get_uri()
    raw, _ = _mint_token(secure_snowflake, "admin_user")

    r = requests.put(
        f"{uri}/api/v1/me/password",
        headers={**_auth(raw), "Content-Type": "application/json"},
        data="null",
    )
    assert r.status_code == 403


def test_api_token_can_list_but_not_delete(secure_snowflake):
    # Under token auth, listing tokens is allowed but creating and deleting are not
    uri = secure_snowflake.get_uri()
    raw, info = _mint_token(secure_snowflake, "admin_user")

    r = requests.get(f"{uri}/api/v1/me/tokens", headers=_auth(raw))
    assert r.status_code == 200

    r = requests.delete(f"{uri}/api/v1/me/tokens/{info['id']}", headers=_auth(raw))
    assert r.status_code == 403

    # An admin token also cannot delete another user's token
    other_raw, other_info = _mint_token(secure_snowflake, "read_user")
    r = requests.delete(
        f"{uri}/api/v1/users/read_user/tokens/{other_info['id']}", headers=_auth(raw)
    )
    assert r.status_code == 403


def test_password_auth_can_still_create_token(secure_snowflake):
    # The block is specific to token auth: a JWT (password) client creates tokens normally
    uri = secure_snowflake.get_uri()
    r = requests.post(
        f"{uri}/auth/v1/login",
        json={"username": "admin_user", "password": test_users["admin_user"]["pw"]},
    )
    jwt = r.json()["access_token"]

    r = requests.post(
        f"{uri}/api/v1/me/tokens",
        headers={"Authorization": f"Bearer {jwt}", "Content-Type": "application/json"},
        data='{"name": "from_jwt"}',
    )
    assert r.status_code == 200


def test_api_token_cannot_administer_users(secure_snowflake):
    # An admin token must not be able to create/modify/delete user accounts - that would be a
    # persistence path around token revocation (mint a new admin with a known password)
    import json

    uri = secure_snowflake.get_uri()
    raw, _ = _mint_token(secure_snowflake, "admin_user")
    h = {**_auth(raw), "Content-Type": "application/json"}

    # Create a user (body is a (UserInfo, password) tuple on the wire)
    new_user = {
        "username": "sneaky_admin",
        "role": "admin",
        "enabled": True,
        "fullname": "",
        "organization": "",
        "email": "",
    }
    r = requests.post(f"{uri}/api/v1/users", headers=h, data=json.dumps([new_user, "a_password_123"]))
    assert r.status_code == 403

    # Modify a user
    read_user = requests.get(f"{uri}/api/v1/users/read_user", headers=_auth(raw)).json()
    r = requests.patch(f"{uri}/api/v1/users", headers=h, data=json.dumps(read_user))
    assert r.status_code == 403

    # Delete a user
    r = requests.delete(f"{uri}/api/v1/users/read_user", headers=_auth(raw))
    assert r.status_code == 403

    # ... and the sneaky account was never created
    assert requests.get(f"{uri}/api/v1/users/sneaky_admin", headers=_auth(raw)).status_code in (400, 404)


def test_password_auth_can_still_administer_users(secure_snowflake):
    # The block is specific to token auth: an admin JWT can still manage users
    import json

    uri = secure_snowflake.get_uri()
    jwt = requests.post(
        f"{uri}/auth/v1/login",
        json={"username": "admin_user", "password": test_users["admin_user"]["pw"]},
    ).json()["access_token"]
    h = {"Authorization": f"Bearer {jwt}", "Content-Type": "application/json"}

    new_user = {
        "username": "legit_new_user",
        "role": "read",
        "enabled": True,
        "fullname": "",
        "organization": "",
        "email": "",
    }
    r = requests.post(f"{uri}/api/v1/users", headers=h, data=json.dumps([new_user, "a_password_123"]))
    assert r.status_code == 200
