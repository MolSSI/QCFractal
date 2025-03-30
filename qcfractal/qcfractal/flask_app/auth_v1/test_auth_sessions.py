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
