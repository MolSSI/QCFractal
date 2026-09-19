"""
Tests for the global request handlers (error logging redaction)
"""

from flask import Flask

from qcfractal.flask_app.handlers import redacted_request_headers, redacted_request_body


def _ctx(path: str, data: bytes, headers=None, content_type="application/json"):
    app = Flask(__name__)
    return app.test_request_context(path, method="POST", data=data, headers=headers or {}, content_type=content_type)


def test_handlers_redact_credential_headers():
    headers = {
        "Authorization": "Bearer abc.def.ghi",
        "Cookie": "qcf_session=supersecretkey",
        "X-Api-Key": "key123",
        "User-Agent": "pytest",
    }
    with _ctx("/api/v1/molecules", b"{}", headers=headers):
        red = redacted_request_headers()

    assert red["Authorization"] == "<redacted>"
    assert red["Cookie"] == "<redacted>"
    assert red["X-Api-Key"] == "<redacted>"
    assert red["User-Agent"] == "pytest"

    joined = str(red)
    assert "supersecretkey" not in joined
    assert "abc.def.ghi" not in joined
    assert "key123" not in joined


def test_handlers_redact_auth_body():
    body = b'{"username": "someone", "password": "hunter2hunter2"}'

    # Auth endpoints are always redacted
    with _ctx("/auth/v1/login", body):
        red = redacted_request_body()
    assert "hunter2hunter2" not in red
    assert "someone" not in red
    assert str(len(body)) in red

    # Password change endpoints are always redacted, even if the body has no "password" key
    with _ctx("/api/v1/me/password", b'"hunter2hunter2"'):
        red = redacted_request_body()
    assert "hunter2hunter2" not in red

    # Any body that looks like it carries a password is redacted (user creation, etc)
    with _ctx("/api/v1/users", b'{"user_info": {"username": "someone"}, "Password": "hunter2hunter2"}'):
        red = redacted_request_body()
    assert "hunter2hunter2" not in red


def test_handlers_ordinary_body_kept():
    body = b'{"ids": [1, 2, 3]}'
    with _ctx("/api/v1/molecules/bulkGet", body):
        red = redacted_request_body()
    assert red == str(body)

    with _ctx("/api/v1/molecules/bulkGet", b""):
        assert redacted_request_body() == ""

    # Long bodies are truncated
    with _ctx("/api/v1/molecules/bulkGet", b"x" * 20000):
        assert len(redacted_request_body()) <= 8192
