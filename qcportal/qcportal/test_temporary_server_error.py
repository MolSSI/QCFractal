import pytest
import requests

from qcportal import PortalRequestError, is_temporary_server_error


@pytest.mark.parametrize(
    "ex",
    [
        ConnectionRefusedError("Could not connect"),
        requests.exceptions.ConnectionError("Connection refused"),
        requests.exceptions.ReadTimeout("Read timed out"),
        requests.exceptions.ChunkedEncodingError("Connection broken"),
        PortalRequestError("Internal Server Error", 500, {}),
        PortalRequestError("Bad Gateway", 502, {}),
        PortalRequestError("Service Unavailable", 503, {}),
        PortalRequestError("Gateway Timeout", 504, {}),
        PortalRequestError("Request Timeout", 408, {}),
        PortalRequestError("Too Many Requests", 429, {}),
    ],
)
def test_is_temporary_server_error(ex):
    assert is_temporary_server_error(ex) is True


@pytest.mark.parametrize(
    "ex",
    [
        PortalRequestError("Bad Request", 400, {}),
        PortalRequestError("Unauthorized", 401, {}),
        PortalRequestError("Forbidden", 403, {}),
        PortalRequestError("Not Found", 404, {}),
        ValueError("not a server error"),
        RuntimeError("Redirection is not allowed"),
    ],
)
def test_is_not_temporary_server_error(ex):
    assert is_temporary_server_error(ex) is False
