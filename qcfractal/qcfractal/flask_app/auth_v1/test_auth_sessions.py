import time

import pytest
import requests

from qcarchivetesting import test_users
from qcportal.auth import UserInfo


def get_qcf_cookie(cookies):
    for c in cookies:
        if c.name == "qcf_session":
            return c
    return None


def cookies_has_qcf_cookie(cookies):
    return get_qcf_cookie(cookies) is not None


@pytest.mark.parametrize("use_forms", [True, False])
def test_auth_session_login_logout(secure_snowflake, use_forms):
    username = "admin_user"
    password = test_users["admin_user"]["pw"]
    uri = secure_snowflake.get_uri()

    sess = requests.Session()  # will store cookies automatically

    # First, not logged in = unauthorized
    r = sess.get(f"{uri}/api/v1/information")
    assert r.status_code == 401  # unauthorized

    # Now go through the browser login. This should set a cookie
    if use_forms:
        r = sess.post(f"{uri}/auth/v1/session_login", data={"username": username, "password": password})
    else:
        r = sess.post(f"{uri}/auth/v1/session_login", json={"username": username, "password": password})

    assert r.status_code == 200
    assert cookies_has_qcf_cookie(sess.cookies)

    # Can get to protected endpoint
    r = sess.get(f"{uri}/api/v1/information")
    assert r.status_code == 200

    # Now logout - cookie is removed
    r = sess.post(f"{uri}/auth/v1/session_logout")
    assert r.status_code == 200
    assert not cookies_has_qcf_cookie(sess.cookies)

    # Not logged in anymore
    r = sess.get(f"{uri}/api/v1/information")
    assert r.status_code == 401  # unauthorized


def test_auth_session_user_disabled(secure_snowflake):
    username = "submit_user"
    password = test_users["submit_user"]["pw"]
    uri = secure_snowflake.get_uri()

    sess = requests.Session()  # will store cookies automatically

    # Now go through the browser login. This should set a cookie
    r = sess.post(f"{uri}/auth/v1/session_login", json={"username": username, "password": password})

    assert r.status_code == 200
    assert cookies_has_qcf_cookie(sess.cookies)

    # Disable the user
    storage_socket = secure_snowflake.get_storage_socket()
    uinfo = storage_socket.users.get("submit_user")
    uinfo["enabled"] = False

    storage_socket.users.modify(UserInfo(**uinfo), as_admin=True)
    assert not get_qcf_cookie(sess.cookies).is_expired()

    # Not logged in anymore
    time.sleep(3.1)  # wait for server verification cache to expire
    r = sess.get(f"{uri}/api/v1/information")
    assert r.status_code == 401  # unauthorized
    assert "is disabled" in r.text


def test_auth_session_expires(secure_snowflake):
    username = "admin_user"
    password = test_users["admin_user"]["pw"]
    uri = secure_snowflake.get_uri()

    sess = requests.Session()  # will store cookies automatically

    # First, not logged in = unauthorized
    r = sess.get(f"{uri}/api/v1/information")
    assert r.status_code == 401  # unauthorized

    # Now go through the browser login. This should set a cookie
    r = sess.post(f"{uri}/auth/v1/session_login", json={"username": username, "password": password})

    assert r.status_code == 200
    assert cookies_has_qcf_cookie(sess.cookies)

    # Wait for expiration
    time.sleep(secure_snowflake._qcf_config.api.user_session_max_age + 1)
    assert get_qcf_cookie(sess.cookies).is_expired()

    # Not logged in anymore
    r = sess.get(f"{uri}/api/v1/information")
    assert r.status_code == 401  # unauthorized

    sess.cookies.clear_expired_cookies()
    assert not cookies_has_qcf_cookie(sess.cookies)
    r = sess.get(f"{uri}/api/v1/information")
    assert r.status_code == 401  # unauthorized


def test_auth_session_forged_expires(secure_snowflake):
    username = "admin_user"
    password = test_users["admin_user"]["pw"]
    uri = secure_snowflake.get_uri()

    sess = requests.Session()  # will store cookies automatically

    # First, not logged in = unauthorized
    r = sess.get(f"{uri}/api/v1/information")
    assert r.status_code == 401  # unauthorized

    # Now go through the browser login. This should set a cookie
    r = sess.post(f"{uri}/auth/v1/session_login", json={"username": username, "password": password})

    assert r.status_code == 200
    assert cookies_has_qcf_cookie(sess.cookies)

    # Wait for expiration
    time.sleep(secure_snowflake._qcf_config.api.user_session_max_age + 1)

    assert get_qcf_cookie(sess.cookies).is_expired()

    # Make it not expired
    c = get_qcf_cookie(sess.cookies)
    c.expires = c.expires + 100000

    # Still not logged in
    r = sess.get(f"{uri}/api/v1/information")
    assert r.status_code == 401  # unauthorized


@pytest.mark.slow
def test_auth_session_extension(secure_snowflake):
    username = "admin_user"
    password = test_users["admin_user"]["pw"]
    uri = secure_snowflake.get_uri()

    sess = requests.Session()  # will store cookies automatically

    # First, not logged in = unauthorized
    r = sess.get(f"{uri}/api/v1/information")
    assert r.status_code == 401  # unauthorized

    # Now go through the browser login. This should set a cookie
    r = sess.post(f"{uri}/auth/v1/session_login", json={"username": username, "password": password})

    assert r.status_code == 200
    assert cookies_has_qcf_cookie(sess.cookies)
    session_id = get_qcf_cookie(sess.cookies).value
    last_exp = get_qcf_cookie(sess.cookies).expires

    max_wait = 2 * int(secure_snowflake._qcf_config.api.user_session_max_age)
    for i in range(max_wait):
        time.sleep(2)
        r = sess.get(f"{uri}/api/v1/information")

        c = get_qcf_cookie(sess.cookies)

        # All session_ids should be the same
        assert not c.is_expired()
        assert c.value == session_id
        assert c.expires > last_exp
        last_exp = c.expires

        assert r.status_code == 200


def _session_login(sess: requests.Session, uri: str, username: str) -> str:
    """Logs in with a browser session, returning the session key from the cookie"""
    r = sess.post(f"{uri}/auth/v1/session_login", json={"username": username, "password": test_users[username]["pw"]})
    assert r.status_code == 200

    # The key issued by this response (the session jar may contain other cookies)
    return r.cookies["qcf_session"]


def test_auth_session_rotates_on_login(secure_snowflake):
    uri = secure_snowflake.get_uri()
    storage_socket = secure_snowflake.get_storage_socket()
    admin_id = storage_socket.users.get("admin_user")["id"]

    sess = requests.Session()
    key1 = _session_login(sess, uri, "admin_user")
    assert storage_socket.auth.load_user_session(key1) is not None

    # Logging in again (already logged in) must issue a new key and revoke the old one
    key2 = _session_login(sess, uri, "admin_user")
    assert key2 != key1
    assert storage_socket.auth.load_user_session(key1) is None

    user_id, session_data, _ = storage_socket.auth.load_user_session(key2)
    assert user_id == admin_id
    assert session_data["user_id"] == str(admin_id)

    # The old key is useless, the new one works
    r = requests.get(f"{uri}/api/v1/information", cookies={"qcf_session": key1})
    assert r.status_code == 401
    r = requests.get(f"{uri}/api/v1/information", cookies={"qcf_session": key2})
    assert r.status_code == 200


def test_auth_session_fixation(secure_snowflake):
    # An attacker obtains a valid session key, plants it in the victim's browser, and the victim logs in.
    # The planted key must not become the victim's session
    uri = secure_snowflake.get_uri()
    storage_socket = secure_snowflake.get_storage_socket()
    admin_id = storage_socket.users.get("admin_user")["id"]
    read_id = storage_socket.users.get("read_user")["id"]

    attacker = requests.Session()
    planted_key = _session_login(attacker, uri, "read_user")
    assert storage_socket.auth.load_user_session(planted_key)[0] == read_id

    victim = requests.Session()
    victim.cookies.set("qcf_session", planted_key)
    victim_key = _session_login(victim, uri, "admin_user")

    assert victim_key != planted_key

    # The planted key is gone from the database and no longer authenticates anyone
    assert storage_socket.auth.load_user_session(planted_key) is None
    r = requests.get(f"{uri}/api/v1/me", cookies={"qcf_session": planted_key})
    assert r.status_code == 401
    r = attacker.get(f"{uri}/api/v1/me")
    assert r.status_code == 401

    # The victim's session is owned by the victim (both in the relational column and the data)
    user_id, session_data, _ = storage_socket.auth.load_user_session(victim_key)
    assert user_id == admin_id
    assert session_data["user_id"] == str(admin_id)

    r = requests.get(f"{uri}/api/v1/me", cookies={"qcf_session": victim_key})
    assert r.status_code == 200
    assert r.json()["username"] == "admin_user"


def test_auth_session_logout_revokes(secure_snowflake):
    uri = secure_snowflake.get_uri()
    storage_socket = secure_snowflake.get_storage_socket()

    sess = requests.Session()
    key = _session_login(sess, uri, "admin_user")

    r = sess.post(f"{uri}/auth/v1/session_logout")
    assert r.status_code == 200
    assert not cookies_has_qcf_cookie(sess.cookies)

    # Row is gone from the database, and the key cannot be reused even if the browser kept it
    assert storage_socket.auth.load_user_session(key) is None
    r = requests.get(f"{uri}/api/v1/information", cookies={"qcf_session": key})
    assert r.status_code == 401
    assert storage_socket.auth.load_user_session(key) is None


def test_auth_session_failed_login_keeps_session(secure_snowflake):
    uri = secure_snowflake.get_uri()
    storage_socket = secure_snowflake.get_storage_socket()

    sess = requests.Session()
    key = _session_login(sess, uri, "admin_user")

    # A failed login attempt must not disturb the existing session
    r = sess.post(f"{uri}/auth/v1/session_login", json={"username": "admin_user", "password": "definitely wrong"})
    assert r.status_code == 401

    assert get_qcf_cookie(sess.cookies).value == key
    assert storage_socket.auth.load_user_session(key) is not None
    r = sess.get(f"{uri}/api/v1/information")
    assert r.status_code == 200


def test_auth_session_revoked_not_resurrected(secure_snowflake):
    uri = secure_snowflake.get_uri()
    storage_socket = secure_snowflake.get_storage_socket()
    admin_id = storage_socket.users.get("admin_user")["id"]

    sess = requests.Session()
    key = _session_login(sess, uri, "admin_user")

    # Revoke the session out-of-band (administrative action, expiry cleanup, concurrent logout)
    storage_socket.auth.delete_user_session(user_session_key=key)

    # A request carrying the old cookie is unauthenticated and must not re-create the row
    r = sess.get(f"{uri}/api/v1/information")
    assert r.status_code == 401
    assert storage_socket.auth.load_user_session(key) is None

    # The socket-level guarantee behind this: saving an existing session never inserts
    assert storage_socket.auth.update_user_session(admin_id, key, {"user_id": str(admin_id)}) is False
    assert storage_socket.auth.load_user_session(key) is None


def test_auth_session_owner_mismatch(secure_snowflake):
    # If the session data somehow disagrees with the relational owner of the session, it is invalid
    from sqlalchemy import select
    from qcfractal.components.auth.db_models import UserSessionORM

    uri = secure_snowflake.get_uri()
    storage_socket = secure_snowflake.get_storage_socket()
    read_id = storage_socket.users.get("read_user")["id"]

    sess = requests.Session()
    key = _session_login(sess, uri, "admin_user")

    with storage_socket.session_scope() as s:
        row = s.execute(select(UserSessionORM).where(UserSessionORM.session_key == key)).scalar_one()
        row.session_data = {**row.session_data, "user_id": str(read_id)}

    r = sess.get(f"{uri}/api/v1/me")
    assert r.status_code == 401
    assert storage_socket.auth.load_user_session(key) is None
