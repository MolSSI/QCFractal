import time

import pytest
import requests

from qcarchivetesting import test_users
from qcfractal.flask_app.csrf import CSRF_HEADER
from qcportal.auth import UserInfo

# Header that every state-changing request authenticated by a session cookie must carry (CSRF protection)
CSRF_HEADERS = {CSRF_HEADER: "XMLHttpRequest"}


def _browser_session() -> requests.Session:
    """
    A requests session that behaves like the web portal: stores cookies and sends the CSRF header
    """
    sess = requests.Session()
    sess.headers.update(CSRF_HEADERS)
    return sess


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

    sess = _browser_session()

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

    sess = _browser_session()

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

    sess = _browser_session()

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

    sess = _browser_session()

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

    sess = _browser_session()

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

    sess = _browser_session()
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

    attacker = _browser_session()
    planted_key = _session_login(attacker, uri, "read_user")
    assert storage_socket.auth.load_user_session(planted_key)[0] == read_id

    victim = _browser_session()
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

    sess = _browser_session()
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

    sess = _browser_session()
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

    sess = _browser_session()
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

    sess = _browser_session()
    key = _session_login(sess, uri, "admin_user")

    with storage_socket.session_scope() as s:
        row = s.execute(select(UserSessionORM).where(UserSessionORM.session_key == key)).scalar_one()
        row.session_data = {**row.session_data, "user_id": str(read_id)}

    r = sess.get(f"{uri}/api/v1/me")
    assert r.status_code == 401
    assert storage_socket.auth.load_user_session(key) is None


def test_auth_session_csrf_login_logout(secure_snowflake):
    uri = secure_snowflake.get_uri()
    storage_socket = secure_snowflake.get_storage_socket()
    creds = {"username": "admin_user", "password": test_users["admin_user"]["pw"]}

    # Login without the header is rejected (login CSRF), whether json or a plain form post
    sess = requests.Session()
    r = sess.post(f"{uri}/auth/v1/session_login", json=creds)
    assert r.status_code == 403
    assert not cookies_has_qcf_cookie(sess.cookies)

    r = sess.post(f"{uri}/auth/v1/session_login", data=creds)
    assert r.status_code == 403
    assert not cookies_has_qcf_cookie(sess.cookies)

    # Logout without the header is rejected and leaves the session intact
    sess = _browser_session()
    key = _session_login(sess, uri, "admin_user")

    r = sess.post(f"{uri}/auth/v1/session_logout", headers={CSRF_HEADER: None})
    assert r.status_code == 403
    assert storage_socket.auth.load_user_session(key) is not None
    r = sess.get(f"{uri}/api/v1/information")
    assert r.status_code == 200

    # And from an untrusted origin
    r = sess.post(f"{uri}/auth/v1/session_logout", headers={"Origin": "https://evil.example"})
    assert r.status_code == 403
    assert storage_socket.auth.load_user_session(key) is not None


def test_auth_session_csrf_unsafe_methods(secure_snowflake):
    uri = secure_snowflake.get_uri()
    sess = _browser_session()
    _session_login(sess, uri, "admin_user")

    body = {"ids": []}

    # With the header: fine
    r = sess.post(f"{uri}/api/v1/molecules/bulkGet", json=body)
    assert r.status_code == 200

    # Without the header: rejected. Safe methods are unaffected
    r = sess.post(f"{uri}/api/v1/molecules/bulkGet", json=body, headers={CSRF_HEADER: None})
    assert r.status_code == 403
    r = sess.get(f"{uri}/api/v1/information", headers={CSRF_HEADER: None})
    assert r.status_code == 200

    # Multipart posts carrying a json part are how a hostile site would bypass a
    # content-type based check without a CORS preflight. The header is still required
    files = {"body_data": ("body.json", b'{"ids": []}', "application/json")}
    r = sess.post(f"{uri}/api/v1/molecules/bulkGet", files=files, headers={CSRF_HEADER: None})
    assert r.status_code == 403
    r = sess.post(f"{uri}/api/v1/molecules/bulkGet", files=files)
    assert r.status_code == 200

    # Origin checks: the server itself is trusted, anything else (including "null") is not
    host = uri.split("://", 1)[1]
    r = sess.post(f"{uri}/api/v1/molecules/bulkGet", json=body, headers={"Origin": f"http://{host}"})
    assert r.status_code == 200
    r = sess.post(f"{uri}/api/v1/molecules/bulkGet", json=body, headers={"Origin": f"https://{host}"})
    assert r.status_code == 200
    r = sess.post(f"{uri}/api/v1/molecules/bulkGet", json=body, headers={"Origin": "https://evil.example"})
    assert r.status_code == 403
    r = sess.post(f"{uri}/api/v1/molecules/bulkGet", json=body, headers={"Origin": "null"})
    assert r.status_code == 403


def test_auth_session_csrf_jwt_unaffected(secure_snowflake):
    # Bearer-token requests are not subject to CSRF checks (a hostile site cannot add the header)
    uri = secure_snowflake.get_uri()
    creds = {"username": "admin_user", "password": test_users["admin_user"]["pw"]}

    r = requests.post(f"{uri}/auth/v1/login", json=creds)
    assert r.status_code == 200
    auth = {"Authorization": f"Bearer {r.json()['access_token']}"}

    r = requests.post(f"{uri}/api/v1/molecules/bulkGet", json={"ids": []}, headers=auth)
    assert r.status_code == 200
    r = requests.post(
        f"{uri}/api/v1/molecules/bulkGet", json={"ids": []}, headers={**auth, "Origin": "https://evil.example"}
    )
    assert r.status_code == 200


def test_auth_session_bearer_precedence(secure_snowflake):
    # A request with both a session cookie and an Authorization header is authenticated by the header
    uri = secure_snowflake.get_uri()

    sess = _browser_session()
    _session_login(sess, uri, "read_user")
    r = sess.get(f"{uri}/api/v1/me")
    assert r.json()["username"] == "read_user"

    creds = {"username": "admin_user", "password": test_users["admin_user"]["pw"]}
    r = requests.post(f"{uri}/auth/v1/login", json=creds)
    auth = {"Authorization": f"Bearer {r.json()['access_token']}"}

    r = sess.get(f"{uri}/api/v1/me", headers=auth)
    assert r.status_code == 200
    assert r.json()["username"] == "admin_user"

    # A bad token is an error, not a fall back to the cookie
    r = sess.get(f"{uri}/api/v1/me", headers={"Authorization": "Bearer not.a.token"})
    assert r.status_code == 401
    r = sess.get(f"{uri}/api/v1/me", headers={"Authorization": "Basic abc"})
    assert r.status_code == 401


def test_auth_session_cookie_attributes(secure_snowflake):
    uri = secure_snowflake.get_uri()
    creds = {"username": "admin_user", "password": test_users["admin_user"]["pw"]}

    sess = _browser_session()
    r = sess.post(f"{uri}/auth/v1/session_login", json=creds)
    assert r.status_code == 200

    set_cookie = r.headers["Set-Cookie"]
    attrs = {a.strip().split("=")[0].lower() for a in set_cookie.split(";")[1:]}
    assert "httponly" in attrs
    assert "samesite=lax" in set_cookie.lower()

    # The snowflake is served over plain http and explicitly disables the Secure flag
    assert secure_snowflake._qcf_config.api.user_session_cookie_secure is False
    assert "secure" not in attrs

    # Logout deletes the cookie with the same attributes
    r = sess.post(f"{uri}/auth/v1/session_logout")
    assert r.status_code == 200
    set_cookie = r.headers["Set-Cookie"]
    assert set_cookie.startswith("qcf_session=;")
    assert "httponly" in set_cookie.lower()
    assert "samesite=lax" in set_cookie.lower()


def test_auth_session_throttled_refresh(secure_snowflake):
    # An unmodified session is only written back (and the cookie re-sent) once its last access
    # is older than a tenth of the lifetime. Modifications are always written
    uri = secure_snowflake.get_uri()
    storage_socket = secure_snowflake.get_storage_socket()
    threshold = secure_snowflake._qcf_config.api.user_session_max_age / 10

    sess = _browser_session()
    key = _session_login(sess, uri, "admin_user")
    _, _, accessed_1 = storage_socket.auth.load_user_session(key)

    # Immediately again: nothing written, no new cookie, but the response varies on the cookie
    r = sess.get(f"{uri}/api/v1/information")
    assert r.status_code == 200
    assert "Set-Cookie" not in r.headers
    assert "Cookie" in r.headers.get("Vary", "")
    _, _, accessed_2 = storage_socket.auth.load_user_session(key)
    assert accessed_2 == accessed_1

    # A change to the session data (here, the client user agent) is written immediately
    r = sess.get(f"{uri}/api/v1/information", headers={"User-Agent": "something else"})
    assert r.status_code == 200
    assert "Set-Cookie" in r.headers
    _, data, accessed_3 = storage_socket.auth.load_user_session(key)
    assert data["user_agent"] == "something else"
    assert accessed_3 > accessed_2

    # After the threshold, the session is touched again (sliding expiration)
    time.sleep(threshold + 0.5)
    r = sess.get(f"{uri}/api/v1/information", headers={"User-Agent": "something else"})
    assert r.status_code == 200
    assert "Set-Cookie" in r.headers
    _, _, accessed_4 = storage_socket.auth.load_user_session(key)
    assert accessed_4 > accessed_3
