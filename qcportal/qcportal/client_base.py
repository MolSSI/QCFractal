from __future__ import annotations

import hashlib
import logging
import os
import random
import time
from collections.abc import Iterable
from typing import Any, TypeVar, overload

import jwt
import pydantic
import requests
import urllib3.exceptions
import yaml
from packaging.version import parse as parse_version
from tqdm import tqdm

from . import __version__
from .auth import API_TOKEN_PREFIX, UserInfo, looks_like_api_token
from .exceptions import AuthenticationFailure
from .serialization import serialize, deserialize


def _response_msg(response: requests.Response) -> str:
    """
    Best-effort extraction of a human-readable message from an error response

    The server returns errors as JSON with the message under "msg", but an error may also come from
    a reverse proxy or load balancer as HTML, an empty body, or a non-object JSON value. This never
    raises, so callers can use it on any response.
    """

    try:
        body = response.json()
    except Exception:
        return response.reason or ""

    msg = body.get("msg") if isinstance(body, dict) else None
    if not isinstance(msg, str):
        # A non-string "msg" (null, a number, a list) is not a usable message. Fall back so that
        # callers doing substring checks always get a string.
        return response.reason or ""
    return msg

AllowedConnectionExceptions = (
    ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    urllib3.exceptions.TimeoutError,
)

# Type of the model a response is deserialized into
_V = TypeVar("_V")

# Used for classmethods that construct a client - they return the derived class they are called on
_ClientType = TypeVar("_ClientType", bound="PortalClientBase")


_ssl_error_msg = (
    "\n\nSSL handshake failed. This is likely caused by a failure to retrieve 3rd party SSL certificates.\n"
    "If you trust the server you are connecting to, try 'PortalClient(... verify=False)'"
)
_connection_error_msg = "\n\nCould not connect to server {}, please check the address and try again."


def pretty_print_request(req: requests.PreparedRequest) -> None:
    print("----------------------")
    print(f"{req.method} {req.url}")
    # Header values may be bytes, so convert explicitly. The Authorization header carries a bearer
    # credential (a JWT or a long-lived API token), so its value is redacted even in debug output
    print(
        "\n".join(
            f"{k}: {'<redacted>' if k.lower() == 'authorization' else str(v)}" for k, v in req.headers.items()
        )
    )
    print("----------------------")


def pretty_print_response(res: requests.Response) -> None:
    print("----------------------")
    print(f"RESPONSE {res.url} -> {res.status_code}")
    print(f"Content: {len(res.content)} bytes")
    print("\n".join(f"{k}: {v}" for k, v in res.headers.items()))
    print("----------------------")


class PortalRequestError(Exception):
    def __init__(self, msg: str, status_code: int, details: dict[str, Any]) -> None:
        Exception.__init__(self, msg)
        self.msg = msg
        self.status_code = status_code
        self.details = details

    def __str__(self) -> str:
        return f"{self.msg} (HTTP status {self.status_code})"


class PortalClientBase:
    def __init__(
        self,
        address: str,
        username: str | None = None,
        password: str | None = None,
        verify: bool = True,
        show_motd: bool = True,
        *,
        api_token: str | None = None,
        information_endpoint: str = "api/v1/information",
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
        api_token
            A long-lived API token to authenticate with, as an alternative to a username and
            password. Mutually exclusive with them. Unlike the username/password flow, this needs
            no token refreshing, so it is suitable for clients that only set a static header.
        """

        if api_token is not None and (username is not None or password is not None):
            raise ValueError("Cannot provide both an api_token and a username/password")

        if api_token is not None and not looks_like_api_token(api_token):
            # A cheap client-side check (prefix, length, character set) so an obviously malformed
            # token - e.g. a password pasted into the wrong field - fails immediately with a clear
            # message rather than a 401 after a round trip. The server remains authoritative.
            raise ValueError(f"That does not look like a valid API token (it should start with '{API_TOKEN_PREFIX}')")

        self._logger = logging.getLogger("PortalClientBase")

        # For developer use and debugging
        self.debug_requests = False

        # Where we get the server information from
        self._information_endpoint = information_endpoint.strip("/")

        if not address.startswith("http://") and not address.startswith("https://"):
            address = "https://" + address

        if not address.endswith("/"):
            address += "/"

        self.address = address
        self.username: str | None = username
        self.user_id: int | None = None
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

        # Processing/downloading in threads
        # Number of threads to use when fetching from the server
        self.n_download_threads = 2

        # Target time for how long a request should take (in seconds)
        # Chunk size will be adjusted to try to reach this target time
        self.download_target_time = 0.50

        # If no 3rd party verification, quiet urllib
        if self._verify is False:
            requests.packages.urllib3.disable_warnings(category=urllib3.exceptions.InsecureRequestWarning)

        # Credentials and JWT tokens/expirations. These are all set by _get_JWT_token
        self._username: str | None = None
        self._password: str | None = None
        self._api_token: str | None = None
        self._jwt_access_token: str | None = None
        self._jwt_refresh_token: str | None = None
        self._jwt_access_exp: int | None = None
        self._jwt_refresh_exp: int | None = None

        if username is not None and password is not None:
            self._username = username
            self._password = password
            self._get_JWT_token()
        elif api_token is not None:
            # A static bearer credential - no login, no refresh. Just set it on the session so it
            # rides on every request, exactly as the JWT access token does.
            self._api_token = api_token
            self._req_session.headers.update({"Authorization": f"Bearer {api_token}"})

        # Try to connect and pull the server info
        self.server_info: dict[str, Any] = self.get_server_information()
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

        # With username/password the user id came from the decoded JWT. An API token is opaque, so
        # ask the server who we are. A failure to reach /me is not fatal (a compute-only proxy may
        # not expose it, or security may be disabled), but a 401 means the token itself is bad.
        if self._api_token is not None:
            self._bootstrap_identity_from_token()

        motd = self.server_info.get("motd", "")
        if show_motd and motd:
            print("*" * 10 + "- Message-of-the-Day from the server -" + "*" * 10)
            print()
            print(motd)
            print()
            print("*" * 14 + "- End of Message-of-the-Day -" + "*" * 15)

    @classmethod
    def from_file(
        cls: type[_ClientType], server_name: str | None = None, config_path: str | None = None
    ) -> _ClientType:
        """Creates a new client given information in a file.

        If no path is passed in, the current working directory and finally ~/.qca
        are searched for "qcportal_config.yaml"

        Parameters
        ----------
        server_name
            Name/alias of the server in the yaml file
        config_path
            Full path to a configuration file, or a directory containing "qcportal_config.yaml".

        Returns
        -------
        :
            A new client, constructed with the settings found in the file
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

        # A config file holding a credential (an API token or a password) should not be readable by
        # other users. Warn (but do not refuse), mirroring the caution ssh applies to private keys.
        has_credential = data.get("api_token") is not None or data.get("password") is not None
        if has_credential and os.name == "posix":
            try:
                mode = os.stat(config_path).st_mode
                if mode & 0o077:
                    logging.getLogger("PortalClientBase").warning(
                        f"Config file {config_path} contains a credential but is readable by other "
                        "users. Consider 'chmod 600' on it."
                    )
            except OSError:
                pass

        return cls(**data)

    @classmethod
    def from_env(cls: type[_ClientType]) -> _ClientType:
        """Creates a new client given information stored in environment variables

        The environment variables are:

          * QCPORTAL_ADDRESS (required)
          * QCPORTAL_USERNAME (optional)
          * QCPORTAL_PASSWORD (optional)
          * QCPORTAL_API_TOKEN (optional, mutually exclusive with username/password)
          * QCPORTAL_VERIFY (optional, defaults to True)
          * QCPORTAL_CACHE_DIR (optional)

        Returns
        -------
        :
            A new client, constructed with the settings found in the environment
        """

        address = os.environ.get("QCPORTAL_ADDRESS", None)
        username = os.environ.get("QCPORTAL_USERNAME", None)
        password = os.environ.get("QCPORTAL_PASSWORD", None)
        api_token = os.environ.get("QCPORTAL_API_TOKEN", None)
        verify = os.environ.get("QCPORTAL_VERIFY", True)
        cache_dir = os.environ.get("QCPORTAL_CACHE_DIR", None)

        if address is None:
            raise KeyError("Required environment variable 'QCPORTAL_ADDRESS' not found")

        if api_token is not None and (username is not None or password is not None):
            raise ValueError(
                "Set either QCPORTAL_API_TOKEN or QCPORTAL_USERNAME/QCPORTAL_PASSWORD, not both"
            )

        data: dict[str, Any] = {"address": address}

        if username is not None:
            data["username"] = username

        if password is not None:
            data["password"] = password

        if api_token is not None:
            data["api_token"] = api_token

        if cache_dir is not None:
            data["cache_dir"] = cache_dir

        data["verify"] = verify

        return cls(**data)

    @property
    def encoding(self) -> str:
        return self._encoding

    @encoding.setter
    def encoding(self, encoding: str) -> None:
        self._encoding = encoding
        enc_headers = {"Accept": encoding}
        self._req_session.headers.update(enc_headers)

    def _send_request(self, req: requests.Request, allow_retries: bool = True) -> requests.Response:
        """
        Sends a prepared request, optionally retrying on errors

        Parameters
        ----------
        req
            A prepared request to send
        allow_retries
            If true, attempts to retry on certain kinds of errors

        Returns
        -------
        :
            The response returned from the request
        """

        prep_req = self._req_session.prepare_request(req)

        if self.debug_requests:
            pretty_print_request(prep_req)

        if not allow_retries:
            ret = self._req_session.send(prep_req, verify=self._verify, timeout=self.timeout, allow_redirects=False)

            if self.debug_requests:
                pretty_print_response(ret)

            if ret.is_redirect:
                raise RuntimeError("Redirection is not allowed")
            return ret

        retry_count = 0

        try:
            while True:
                try:
                    ret = self._req_session.send(
                        prep_req, verify=self._verify, timeout=self.timeout, allow_redirects=False
                    )
                    break
                except requests.exceptions.SSLError:
                    raise ConnectionRefusedError(_ssl_error_msg) from None
                except AllowedConnectionExceptions as e:
                    if retry_count >= self.retry_max:
                        raise

                    # eg, if jitter fraction is 0.05, then multiply by something on the range 0.95 to 1.05
                    jitter = random.uniform(1.0 - self.retry_jitter_fraction, 1.0 + self.retry_jitter_fraction)
                    time_to_wait = self.retry_delay * (self.retry_backoff**retry_count) * jitter

                    retry_count += 1
                    self._logger.warning(
                        f"Connection error for {prep_req.url}: {str(e)} - retrying in {time_to_wait:.2f} seconds "
                        f"[{retry_count}/{self.retry_max}]"
                    )
                    time.sleep(time_to_wait)
        except requests.exceptions.SSLError:
            raise ConnectionRefusedError(_ssl_error_msg) from None
        except AllowedConnectionExceptions:
            raise ConnectionRefusedError(_connection_error_msg.format(self.address)) from None

        if self.debug_requests:
            pretty_print_response(ret)

        if ret.is_redirect:
            raise RuntimeError("Redirection is not allowed")

        return ret

    def _bootstrap_identity_from_token(self) -> None:
        """
        Populates self.user_id / self.username for an API-token client by asking the server

        Called only in token mode. On a normal secure server /me is reachable by any authenticated
        user, so this should succeed - the one exception is a server with security disabled, where
        /me returns 401 and there is no identity to learn (but the token may still be usable for
        the open endpoints). Any other error means either the token is bad or something is wrong, so
        it is raised rather than silently producing an identity-less client.
        """

        full_uri = self.address + "api/v1/me"
        req = requests.Request(method="GET", url=full_uri, headers={"Accept": self.encoding})
        ret = self._send_request(req)

        if ret.status_code == 200:
            user_info = deserialize(ret.content, ret.headers["Content-Type"], UserInfo)
            self.user_id = user_info.id
            self.username = user_info.username
            return

        msg = _response_msg(ret)

        # A server with security disabled returns 401 from /me (it requires security). The token is
        # not necessarily bad, and there is simply no identity to learn - tolerate it.
        if ret.status_code == 401 and "security disabled" in msg:
            self._logger.debug("Server has security disabled; cannot determine identity from API token")
            return

        # Anything else (a genuine 401, a 404, a 5xx) is a real problem - do not hide it behind an
        # identity-less client
        raise AuthenticationFailure(f"Could not authenticate API token: {msg}")

    def _get_JWT_token(self) -> None:
        assert self._api_token is None, "JWT login attempted on an API-token client"

        full_uri = self.address + "auth/v1/login"
        json = {"username": self._username, "password": self._password}

        req = requests.Request(method="POST", url=full_uri, json=json)
        ret = self._send_request(req)

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
            self.user_id = int(decoded_access_token["sub"])  # "identity" "subject"
        else:
            raise AuthenticationFailure(_response_msg(ret))

    def _refresh_JWT_token(self) -> None:
        assert self._api_token is None, "JWT refresh attempted on an API-token client"

        full_uri = self.address + "auth/v1/refresh"
        headers = {"Authorization": f"Bearer {self._jwt_refresh_token}"}

        req = requests.Request(method="POST", url=full_uri, headers=headers)
        ret = self._send_request(req)

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

            return

        msg = _response_msg(ret)
        if ret.status_code == 401 and "Token has expired" in msg:
            # If the refresh token has expired, try to log in again
            self._get_JWT_token()
        elif ret.status_code == 401 and " is disabled" in msg:
            raise AuthenticationFailure("User account has been disabled")
        elif ret.status_code == 401 and " does not exist" in msg:
            raise AuthenticationFailure("User account no longer exists")
        else:  # shouldn't happen unless user is blacklisted or something
            raise ConnectionRefusedError("Unable to refresh JWT authorization token! This is a server issue!!")

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        body: bytes | str | None = None,
        url_params: dict[str, Any] | None = None,
        file_data: Iterable[tuple[str, Any]] | None = None,
        internal_retry: bool | None = True,
        allow_retries: bool = True,
        additional_headers: dict[str, Any] | None = None,
    ) -> requests.Response:
        # If refresh token has expired, log in again
        if self._jwt_refresh_exp and self._jwt_refresh_exp < time.time():
            self._get_JWT_token()

        # If only the JWT token is expired, automatically renew it
        if self._jwt_access_exp and self._jwt_access_exp < time.time():
            self._refresh_JWT_token()

        full_uri = self.address + endpoint

        headers = {}

        # Let requests handle content-type if doing multipart
        # but specify our encoding otherwise
        if file_data is None:
            headers = {"Content-Type": self.encoding}

        if additional_headers is not None:
            headers.update(additional_headers)

        req = requests.Request(
            method=method.upper(), url=full_uri, data=body, params=url_params, files=file_data, headers=headers
        )
        r = self._send_request(req, allow_retries=allow_retries)

        # If JWT token expired, automatically renew it and retry once. This should have been caught above,
        # but can happen in rare instances where the token expires between the time we check it and the time
        # we use it. Only applies to the JWT flow - an API token cannot be refreshed.
        if (
            internal_retry
            and self._api_token is None
            and (r.status_code == 401)
            and "Token has expired" in _response_msg(r)
        ):
            self._refresh_JWT_token()
            return self._request(method, endpoint, body=body, url_params=url_params, internal_retry=False)

        if r.status_code != 200:
            try:
                # For many errors returned by our code, the error details are returned as json
                # with the error message stored under "msg"
                details = r.json()
                if not isinstance(details, dict):
                    details = {"msg": str(details)}
            except:
                # If this error comes from, ie, the web server or something else, then
                # we have to use 'reason'
                details = {"msg": r.reason}

            raise PortalRequestError(f"Request failed: {details.get('msg', r.reason)}", r.status_code, details)

        return r

    # Overload for giving a plain class as the response model
    @overload
    def make_request(
        self,
        method: str,
        endpoint: str,
        response_model: type[_V],
        *,
        body_model: Any = None,
        url_params_model: Any = None,
        body: Any = None,
        url_params: Any = None,
        upload_files: Iterable[tuple[str, str]] | None = None,
        allow_retries: bool = True,
        additional_headers: dict[str, Any] | None = None,
    ) -> _V: ...

    # Overload for no response model (nothing is returned by the endpoint)
    @overload
    def make_request(
        self,
        method: str,
        endpoint: str,
        response_model: None,
        *,
        body_model: Any = None,
        url_params_model: Any = None,
        body: Any = None,
        url_params: Any = None,
        upload_files: Iterable[tuple[str, str]] | None = None,
        allow_retries: bool = True,
        additional_headers: dict[str, Any] | None = None,
    ) -> None: ...

    # Overload for anything that is not a plain class - typing special forms like dict[str, Any],
    # list[int], tuple[InsertMetadata, list[int]], etc. These are all valid pydantic TypeAdapter
    # arguments, but cannot be expressed as a type[...], so the return type is not knowable here
    @overload
    def make_request(
        self,
        method: str,
        endpoint: str,
        response_model: Any,
        *,
        body_model: Any = None,
        url_params_model: Any = None,
        body: Any = None,
        url_params: Any = None,
        upload_files: Iterable[tuple[str, str]] | None = None,
        allow_retries: bool = True,
        additional_headers: dict[str, Any] | None = None,
    ) -> Any: ...

    def make_request(
        self,
        method: str,
        endpoint: str,
        response_model: Any,
        *,
        body_model: Any = None,
        url_params_model: Any = None,
        body: Any = None,
        url_params: Any = None,
        upload_files: Iterable[tuple[str, str]] | None = None,
        allow_retries: bool = True,
        additional_headers: dict[str, Any] | None = None,
    ) -> Any:
        # If body_model or url_params_model are None, then use the type given
        if body_model is None and body is not None:
            body_model = type(body)

        if url_params_model is None and url_params is not None:
            url_params_model = type(url_params)

        serialized_body = None
        if body_model is not None:
            parsed_body = pydantic.TypeAdapter(body_model).validate_python(body)
            serialized_body = serialize(parsed_body, self.encoding)

        parsed_url_params = None
        if url_params_model is not None:
            parsed_url_params = pydantic.TypeAdapter(url_params_model).validate_python(url_params)

        if isinstance(parsed_url_params, pydantic.BaseModel):
            parsed_url_params = parsed_url_params.model_dump()

        file_data: list[tuple[str, tuple[Any, ...]]] | None = None
        if upload_files is not None:
            # Yes, a list of tuples. We always use the "files" key, and doing it this way
            # allows for multiple files to be uploaded in a single request.
            file_data = [("files", (fname, open(fpath, "rb"))) for fname, fpath in upload_files]

            # We must also send the serialized body as part of the multipart upload
            if serialized_body is not None:
                file_data.append(("body_data", ("body_data", serialized_body, self.encoding)))
                serialized_body = None

        assert (serialized_body is None) or (file_data is None)  # Just to check my logic

        r = self._request(
            method,
            endpoint,
            body=serialized_body,
            url_params=parsed_url_params,
            file_data=file_data,
            allow_retries=allow_retries,
            additional_headers=additional_headers,
        )

        return deserialize(r.content, r.headers["Content-Type"], response_model)

    def download_file(
        self,
        endpoint: str,
        destination_path: str,
        overwrite: bool = False,
        expected_size: int | None = None,
        show_progress: bool = False,
    ) -> tuple[int, str]:
        """
        Download a file with optional progress bar

        Parameters
        ----------
        endpoint
            API endpoint to download from
        destination_path
            Where to save the file
        overwrite
            Whether to overwrite existing files
        expected_size
            Expected size of the file in bytes (used for progress bar if enabled)
        show_progress
            Whether to show a progress bar during download

        Returns
        -------
        :
            A tuple of the size of the downloaded file (in bytes) and its sha256 checksum
        """

        sha256 = hashlib.sha256()
        file_size = 0

        # Remove if overwrite=True. This allows for any processes still using the old file to keep using it
        # (at least on linux)
        if os.path.exists(destination_path):
            if overwrite:
                os.remove(destination_path)
            else:
                raise RuntimeError(f"File already exists at {destination_path}. To overwrite, use `overwrite=True`")

        full_uri = self.address + endpoint
        response = self._req_session.get(full_uri, stream=True, allow_redirects=False)

        if response.is_redirect:
            # send again, but using a plain requests object
            # that way, we don't pass the JWT to someone else
            new_location = response.headers["Location"]
            response = requests.get(new_location, stream=True, allow_redirects=True)

        response.raise_for_status()

        # Get filename for display in progress bar
        filename = os.path.basename(destination_path)

        with open(destination_path, "wb") as f:
            if show_progress:
                # Show progress bar with expected_size (which can be None)
                with tqdm(
                    total=expected_size,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=f"Downloading to {filename}",
                    miniters=1,  # Update after each iteration
                    mininterval=0.1,  # Allow updates as frequently as every 0.1 seconds
                ) as pbar:
                    # set chunk_size here so that progress bar can be updated
                    # if set to None, it may only be updated after the entire download is complete
                    # 4*1024*1024 is 4MB
                    for chunk in response.iter_content(chunk_size=4 * 1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            sha256.update(chunk)
                            chunk_size = len(chunk)
                            file_size += chunk_size
                            pbar.update(chunk_size)
            else:
                for chunk in response.iter_content(chunk_size=None):
                    if chunk:
                        f.write(chunk)
                        sha256.update(chunk)
                        file_size += len(chunk)

        return file_size, sha256.hexdigest()

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
        except AllowedConnectionExceptions:
            return False

    def get_server_information(self) -> dict[str, Any]:
        """Request general information about the server

        Returns
        -------
        :
            Server information.
        """

        # Request the info, and store here for later use
        # TODO - this fallback is temporary - remove in a future version
        try:
            return self.make_request("get", self._information_endpoint, dict[str, Any])
        except PortalRequestError as e:
            return self.make_request("get", "api/v1/information", dict[str, Any])
