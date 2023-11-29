from __future__ import annotations

import logging
import os
import random
import time
from typing import (
    Any,
    Dict,
    Optional,
    Union,
    TypeVar,
    Type,
)

import jwt

try:
    import pydantic.v1 as pydantic
except ImportError:
    import pydantic
import requests
import yaml
from packaging.version import parse as parse_version

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


def pretty_print_request(req):
    print("----------------------")
    print(f"{req.method} {req.url}")
    print("\n".join(f"{k}: {v}" for k, v in req.headers.items()))
    print("----------------------")


def pretty_print_response(res):
    print("----------------------")
    print(f"RESPONSE {res.url} -> {res.status_code}")
    print(f"Content: {len(res.content)} bytes")
    print("\n".join(f"{k}: {v}" for k, v in res.headers.items()))
    print("----------------------")


class PortalRequestError(Exception):
    def __init__(self, msg: str, status_code: int, details: Dict[str, Any]):
        Exception.__init__(self, msg)
        self.msg = msg
        self.status_code = status_code
        self.details = details

    def __str__(self):
        return f"{self.msg} (HTTP status {self.status_code})"


class PortalClientBase:
    def __init__(
        self,
        address: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify: bool = True,
        show_motd: bool = True,
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
        show_motd
            If a Message-of-the-Day is available, display it
        """

        self._logger = logging.getLogger("PortalClientBase")

        # For developer use and debugging
        self.debug_requests = False

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

        # A persistent session
        # This results in significant speedup (~65% faster in my test)
        # https://docs.python-requests.org/en/master/user/advanced/#session-objects
        self._req_session = requests.Session()

        self._req_session.headers.update({"User-Agent": f"qcportal/{__version__}"})

        self.encoding = "application/json"

        self.timeout = 60

        # Handling retries of requests
        self.retry_max = 5
        self.retry_delay = 0.5
        self.retry_backoff = 2
        self.retry_jitter_fraction = 0.05

        # If no 3rd party verification, quiet urllib
        if self._verify is False:
            from urllib3.exceptions import InsecureRequestWarning

            requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

        if username is not None and password is not None:
            self._username = username
            self._password = password
            self._get_JWT_token()
        else:
            self._username = None
            self._password = None
            self._jwt_access_exp = None
            self._jwt_refresh_exp = None

        # Try to connect and pull the server info
        self.server_info = self.get_server_information()
        self.server_name = self.server_info["name"]
        self.api_limits = self.server_info["api_limits"]

        server_version = parse_version(self.server_info["version"])
        client_version = parse_version(__version__)

        if client_version > server_version:
            self._logger.warning(
                "WARNING: This client version is newer than the server version. This may work if the "
                "versions are close, but expect exceptions and errors if attempting things the server "
                "does not support. "
                f"client version: {str(__version__)}, server version: {str(self.server_info['version'])}"
            )

        motd = self.server_info.get("motd", "")
        if show_motd and motd:
            print("*" * 10 + "- Message-of-the-Day from the server -" + "*" * 10)
            print()
            print(motd)
            print()
            print("*" * 14 + "- End of Message-of-the-Day -" + "*" * 15)

    @classmethod
    def from_file(cls, server_name: Optional[str] = None, config_path: Optional[str] = None):
        """Creates a new client given information in a file.

        If no path is passed in, the current working directory and finally ~/.qca
        are searched for "qcportal_config.yaml"

        Parameters
        ----------
        server_name
            Name/alias of the server in the yaml file
        config_path
            Full path to a configuration file, or a directory containing "qcportal_config.yaml".
        """

        # Search canonical paths
        if config_path is None:
            test_paths = [os.getcwd(), os.path.join(os.path.expanduser("~"), ".qca")]

            for path in test_paths:
                local_path = os.path.join(path, "qcportal_config.yaml")
                if os.path.exists(local_path):
                    config_path = local_path
                    break

            if config_path is None:
                raise FileNotFoundError(
                    "Could not find `qcportal_config.yaml` in the following paths:\n    {}".format(
                        ", ".join(test_paths)
                    )
                )

        else:
            config_path = os.path.join(os.path.expanduser(config_path))

            # Gave folder, not file
            if os.path.isdir(config_path):
                config_path = os.path.join(config_path, "qcportal_config.yaml")

        with open(config_path, "r") as handle:
            data = yaml.load(handle, Loader=yaml.SafeLoader)

        if server_name is not None:
            data = data.get(server_name)
            if data is None:
                raise RuntimeError(f"Server '{server_name}' does not exist in the configuration file")

        if "address" not in data:
            raise KeyError("Config file must at least contain an address field.")

        return cls(**data)

    @property
    def encoding(self) -> str:
        return self._encoding

    @encoding.setter
    def encoding(self, encoding: str):
        self._encoding = encoding
        enc_headers = {"Content-Type": encoding, "Accept": encoding}
        self._req_session.headers.update(enc_headers)

    def _get_JWT_token(self) -> None:
        try:
            ret = self._req_session.post(
                self.address + "auth/v1/login",
                json={"username": self._username, "password": self._password},
                verify=self._verify,
            )
        except requests.exceptions.SSLError:
            raise ConnectionRefusedError(_ssl_error_msg) from None
        except requests.exceptions.ConnectionError:
            raise ConnectionRefusedError(_connection_error_msg.format(self.address)) from None

        if ret.status_code == 200:
            ret_json = ret.json()
            self._jwt_refresh_token = ret_json["refresh_token"]
            self._jwt_access_token = ret_json["access_token"]
            self._req_session.headers.update({"Authorization": f"Bearer {self._jwt_access_token}"})

            # Store the expiration time of the access and refresh tokens
            # (these are unix epoch timestamps)
            decoded_access_token = jwt.decode(
                self._jwt_access_token, algorithms=["HS256"], options={"verify_signature": False}
            )
            decoded_refresh_token = jwt.decode(
                self._jwt_refresh_token, algorithms=["HS256"], options={"verify_signature": False}
            )
            self._jwt_access_exp = decoded_access_token["exp"]
            self._jwt_refresh_exp = decoded_refresh_token["exp"]
        else:
            try:
                msg = ret.json()["msg"]
            except:
                msg = ret.reason
            raise AuthenticationFailure(msg)

    def _refresh_JWT_token(self) -> None:
        ret = self._req_session.post(
            self.address + "auth/v1/refresh",
            headers={"Authorization": f"Bearer {self._jwt_refresh_token}"},
            verify=self._verify,
        )

        if ret.status_code == 200:
            ret_json = ret.json()
            self._jwt_access_token = ret_json["access_token"]
            self._req_session.headers.update({"Authorization": f"Bearer {self._jwt_access_token}"})

            # Store the expiration time of the access and refresh tokens
            # (these are unix epoch timestamps)
            decoded_access_token = jwt.decode(
                self._jwt_access_token, algorithms=["HS256"], options={"verify_signature": False}
            )
            self._jwt_access_exp = decoded_access_token["exp"]

        elif ret.status_code == 401 and "Token has expired" in ret.json()["msg"]:
            # If the refresh token has expired, try to log in again
            self._get_JWT_token()
        elif ret.status_code == 401 and f" is disabled" in ret.json()["msg"]:
            raise AuthenticationFailure("User account has been disabled")
        elif ret.status_code == 401 and f" does not exist" in ret.json()["msg"]:
            raise AuthenticationFailure("User account no longer exists")
        else:  # shouldn't happen unless user is blacklisted or something
            print(ret, ret.text)
            raise ConnectionRefusedError("Unable to refresh JWT authorization token! This is a server issue!!")

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        body: Optional[Union[bytes, str]] = None,
        url_params: Optional[Dict[str, Any]] = None,
        internal_retry: Optional[bool] = True,
        allow_retries: bool = True,
    ) -> requests.Response:
        # If refresh token has expired, log in again
        if self._jwt_refresh_exp and self._jwt_refresh_exp < time.time():
            self._get_JWT_token()

        # If only the JWT token is expired, automatically renew it
        if self._jwt_access_exp and self._jwt_access_exp < time.time():
            self._refresh_JWT_token()

        full_uri = self.address + endpoint

        req = requests.Request(method=method.upper(), url=full_uri, data=body, params=url_params)
        prep_req = self._req_session.prepare_request(req)

        if self.debug_requests:
            pretty_print_request(prep_req)

        try:
            if not allow_retries:
                r = self._req_session.send(prep_req, verify=self._verify, timeout=self.timeout)
            else:
                current_retries = 0
                while True:
                    try:
                        r = self._req_session.send(prep_req, verify=self._verify, timeout=self.timeout)
                        break
                    except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout) as e:
                        if current_retries >= self.retry_max:
                            raise

                        # eg, if jitter fraction is 0.05, then multiply by something on the range 0.95 to 1.05
                        jitter = random.uniform(1.0 - self.retry_jitter_fraction, 1.0 + self.retry_jitter_fraction)
                        time_to_wait = self.retry_delay * (self.retry_backoff**current_retries) * jitter

                        current_retries += 1
                        self._logger.warning(
                            f"Connection failed: {str(e)} - retrying in {time_to_wait:.2f} seconds "
                            f"[{current_retries}/{self.retry_max}]"
                        )
                        time.sleep(time_to_wait)

            if self.debug_requests:
                pretty_print_response(r)

        except requests.exceptions.SSLError:
            raise ConnectionRefusedError(_ssl_error_msg) from None
        except requests.exceptions.ConnectionError:
            raise ConnectionRefusedError(_connection_error_msg.format(self.address)) from None

        # If JWT token expired, automatically renew it and retry once. This should have been caught above,
        # but can happen in rare instances where the token expires between the time we check it and the time
        # we use it.
        if internal_retry and (r.status_code == 401) and "Token has expired" in r.json()["msg"]:
            self._refresh_JWT_token()
            return self._request(method, endpoint, body=body, url_params=url_params, internal_retry=False)

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

    def make_request(
        self,
        method: str,
        endpoint: str,
        response_model: Optional[Type[_V]],
        *,
        body_model: Optional[Type[_T]] = None,
        url_params_model: Optional[Type[_U]] = None,
        body: Optional[Union[_T, Dict[str, Any]]] = None,
        url_params: Optional[Union[_U, Dict[str, Any]]] = None,
        allow_retries: bool = True,
    ) -> _V:
        # If body_model or url_params_model are None, then use the type given
        if body_model is None and body is not None:
            body_model = type(body)

        if url_params_model is None and url_params is not None:
            url_params_model = type(url_params)

        serialized_body = None
        if body_model is not None:
            parsed_body = pydantic.parse_obj_as(body_model, body)
            serialized_body = serialize(parsed_body, self.encoding)

        parsed_url_params = None
        if url_params_model is not None:
            parsed_url_params = pydantic.parse_obj_as(url_params_model, url_params)

        if isinstance(parsed_url_params, pydantic.BaseModel):
            parsed_url_params = parsed_url_params.dict()

        r = self._request(
            method, endpoint, body=serialized_body, url_params=parsed_url_params, allow_retries=allow_retries
        )
        d = deserialize(r.content, r.headers["Content-Type"])

        if response_model is None:
            return None
        else:
            return pydantic.parse_obj_as(response_model, d)

    def ping(self) -> bool:
        """
        Pings the server to see if it is up

        Returns
        -------
        :
            True if the server is up and responded to the ping. False otherwise
        """

        uri = f"{self.address}/api/v1/ping"

        try:
            r = requests.get(uri)
            return r.json()["success"]
        except requests.exceptions.ConnectionError:
            return False

    def get_server_information(self) -> Dict[str, Any]:
        """Request general information about the server

        Returns
        -------
        :
            Server information.
        """

        # Request the info, and store here for later use
        return self.make_request("get", "api/v1/information", Dict[str, Any])
