"""Tests for the error-message extraction helper, which must never raise"""

from types import SimpleNamespace

import pytest

from qcportal.client_base import _response_msg


class _FakeResponse:
    def __init__(self, json_value, raises=False, reason="Some Reason"):
        self._json_value = json_value
        self._raises = raises
        self.reason = reason

    def json(self):
        if self._raises:
            raise ValueError("not json")
        return self._json_value


@pytest.mark.parametrize(
    "resp,expected",
    [
        (_FakeResponse({"msg": "a real message"}), "a real message"),
        (_FakeResponse({"msg": None}), "Some Reason"),  # non-string msg falls back
        (_FakeResponse({"msg": 123}), "Some Reason"),
        (_FakeResponse({"other": "x"}), "Some Reason"),  # no msg key
        (_FakeResponse([1, 2, 3]), "Some Reason"),  # non-dict json
        (_FakeResponse(None), "Some Reason"),  # json null
        (_FakeResponse(None, raises=True), "Some Reason"),  # not json at all
    ],
)
def test_response_msg_always_string(resp, expected):
    result = _response_msg(resp)
    assert isinstance(result, str)
    assert result == expected
    # And a substring check (as callers do) never raises
    assert "Token has expired" not in result
