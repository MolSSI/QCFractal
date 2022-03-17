from __future__ import annotations

from typing import (
    Any,
    Dict,
    Optional,
    Union,
    TypeVar,
    Type,
)

import pydantic
import requests
from pkg_resources import parse_version

from . import __version__
from .exceptions import AuthenticationFailure
from .serialization import serialize, deserialize

_T = TypeVar("_T")
_U = TypeVar("_U")
_V = TypeVar("_V")


_ssl_error_msg = (
    "\n\nSSL handshake failed. This is likely caused by a failure to retrieve 3rd party SSL certificates.\n"
    "If you trust the server you are connecting to, try 'PortalClient(... verify=False)'"
)
_connection_error_msg = "\n\nCould not connect to server {}, please check the address and try again."


class PortalRequestError(Exception):
    def __init__(self, msg: str, status_code: int, details: Dict[str, Any]):
        Exception.__init__(self, msg)
        self.msg = msg
        self.status_code = status_code
        self.details = details

    def __str__(self):
        return f"Portal request error: {self.msg} (HTTP status {self.status_code})"


class PortalClientBase:
    def __init__(
        self,
        address: str = "api.qcarchive.molssi.org:443",
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify: bool = True,
    ) -> None:
        """Initializes a PortalClient instance from an address and verification information.

        Parameters
        ----------
        address
            The IP and port of the FractalServer instance ("192.168.1.1:8888")
        username
            The username to authenticate with.
        password
            The password to authenticate with.
        verify
            Verifies the SSL connection with a third party server. This may be False if a
            FractalServer was not provided a SSL certificate and defaults back to self-signed
            SSL keys.
        """

        if not address.startswith("http://") and not address.startswith("https://"):
            address = "https://" + address

        # If we are `http`, ignore all SSL directives
        if not address.startswith("https"):
            self._verify = True

        if not address.endswith("/"):
            address += "/"

        self.address = address
        self.username = username
        self._verify = verify

        self._headers: Dict[str, str] = {}
        self._headers["User-Agent"] = f"qcportal/{__version__}"
        self._timeout = 60
        self.encoding = "application/json"

        # Mode toggle for network error testing, not public facing
        self._mock_network_error = False

        # If no 3rd party verification, quiet urllib
        if self._verify is False:
            from urllib3.exceptions import InsecureRequestWarning

            requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

        if username is not None and password is not None:
            self._get_JWT_token(username, password)

        ### Define all attributes before this line

        # Try to connect and pull the server info
        self.server_info = self.get_server_information()
        self.server_name = self.server_info["name"]
        self.api_limits = self.server_info["api_limits"]

        client_version_lower_limit = parse_version(self.server_info["client_version_lower_limit"])
        client_version_upper_limit = parse_version(self.server_info["client_version_upper_limit"])

        client_version = parse_version(__version__)

        if not client_version_lower_limit <= client_version <= client_version_upper_limit:
            raise RuntimeError(
                f"This client version {str(client_version)} does not fall within the server's allowed "
                f"client versions of [{str(client_version_lower_limit)}, {str(client_version_upper_limit)}]."
                f"You may need to upgrade or downgrade"
            )

    @property
    def encoding(self) -> str:
        return self._encoding

    @encoding.setter
    def encoding(self, encoding: str):
        self._encoding = encoding
        self._headers["Content-Type"] = encoding
        self._headers["Accept"] = encoding

    def _get_JWT_token(self, username: str, password: str) -> None:

        try:
            ret = requests.post(
                self.address + "v1/login", json={"username": username, "password": password}, verify=self._verify
            )
        except requests.exceptions.SSLError:
            raise ConnectionRefusedError(_ssl_error_msg) from None
        except requests.exceptions.ConnectionError:
            raise ConnectionRefusedError(_connection_error_msg.format(self.address)) from None

        if ret.status_code == 200:
            self.refresh_token = ret.json()["refresh_token"]
            self._headers["Authorization"] = f'Bearer {ret.json()["access_token"]}'
        else:
            raise AuthenticationFailure(ret.json()["msg"])

    def _refresh_JWT_token(self) -> None:

        ret = requests.post(
            self.address + "v1/refresh", headers={"Authorization": f"Bearer {self.refresh_token}"}, verify=self._verify
        )

        if ret.status_code == 200:
            self._headers["Authorization"] = f'Bearer {ret.json()["access_token"]}'
        else:  # shouldn't happen unless user is blacklisted
            raise ConnectionRefusedError("Unable to refresh JWT authorization token! " "This is a server issue!!")

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        body: Optional[Union[bytes, str]] = None,
        url_params: Optional[Dict[str, Any]] = None,
        retry: Optional[bool] = True,
    ) -> requests.Response:

        addr = self.address + endpoint
        kwargs = {"data": body, "headers": self._headers, "verify": self._verify, "timeout": self._timeout}

        if url_params:
            kwargs["params"] = url_params

        try:
            if method == "get":
                r = requests.get(addr, **kwargs)
            elif method == "post":
                r = requests.post(addr, **kwargs)
            elif method == "put":
                r = requests.put(addr, **kwargs)
            elif method == "patch":
                r = requests.patch(addr, **kwargs)
            elif method == "delete":
                r = requests.delete(addr, **kwargs)
            else:
                raise KeyError("Method not understood: '{}'".format(method))
        except requests.exceptions.SSLError:
            raise ConnectionRefusedError(_ssl_error_msg) from None
        except requests.exceptions.ConnectionError:
            raise ConnectionRefusedError(_connection_error_msg.format(self.address)) from None

        # If JWT token expired, automatically renew it and retry once
        if retry and (r.status_code == 401) and "Token has expired" in r.json()["msg"]:
            self._refresh_JWT_token()
            return self._request(method, endpoint, body=body, retry=False)

        if r.status_code != 200:
            try:
                # For many errors returned by our code, the error details are returned as json
                # with the error message stored under "msg"
                details = r.json()
            except:
                # If this error comes from, ie, the web server or something else, then
                # we have to use 'reason'
                details = {"msg": r.reason}

            raise PortalRequestError(f"Request failed: {details['msg']}", r.status_code, details)

        return r

    def _auto_request(
        self,
        method: str,
        endpoint: str,
        body_model: Optional[Type[_T]],
        url_params_model: Optional[Type[_U]],
        response_model: Optional[Type[_V]],
        body: Optional[Union[_T, Dict[str, Any]]] = None,
        url_params: Optional[Union[_U, Dict[str, Any]]] = None,
    ) -> _V:

        if body_model is None and body is not None:
            raise RuntimeError("Body data not specified, but required")

        if url_params_model is None and url_params is not None:
            raise RuntimeError("Query parameters not specified, but required")

        serialized_body = None
        if body_model is not None:
            parsed_body = pydantic.parse_obj_as(body_model, body)
            serialized_body = serialize(parsed_body, self.encoding)

        parsed_url_params = None
        if url_params_model is not None:
            parsed_url_params = pydantic.parse_obj_as(url_params_model, url_params).dict()

        r = self._request(method, endpoint, body=serialized_body, url_params=parsed_url_params)
        d = deserialize(r.content, r.headers["Content-Type"])

        if response_model is None:
            return None
        else:
            return pydantic.parse_obj_as(response_model, d)

    def get_server_information(self) -> Dict[str, Any]:
        """Request general information about the server

        Returns
        -------
        :
            Server information.
        """

        # Request the info, and store here for later use
        return self._auto_request("get", "v1/information", None, None, Dict[str, Any], None, None)
