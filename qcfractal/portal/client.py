from __future__ import annotations
from collections import defaultdict

import pydantic
from tabulate import tabulate

from datetime import datetime
import os
from pkg_resources import parse_version
from . import __version__

import requests

from typing import (
    TYPE_CHECKING,
    Any,
    DefaultDict,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    TypeVar,
    Sequence,
    Iterable,
    Type,
)
from pathlib import Path

from pydantic import ValidationError
import pandas as pd

from ..interface.models.rest_models import rest_model
from .records.models import RecordStatusEnum
from qcfractal.portal.managers import ManagerQueryBody, ComputeManager
from qcfractal.portal.records.singlepoint import SinglePointRecord
from qcfractal.portal.records import ComputeHistoryURLParameters, ComputeHistory

from .metadata_models import InsertMetadata, DeleteMetadata
from qcfractal.portal.serverinfo import (
    AccessLogQueryParameters,
    AccessLogQuerySummaryParameters,
    ErrorLogQueryParameters,
    ServerStatsQueryParameters,
    DeleteBeforeDateParameters,
)
from .common_rest import (
    CommonGetURLParametersName,
    CommonGetProjURLParameters,
    CommonGetURLParameters,
    CommonDeleteURLParameters,
)
from qcfractal.portal.molecules import Molecule, MoleculeIdentifiers, MoleculeQueryBody, MoleculeModifyBody
from qcfractal.portal.metadata_models import QueryMetadata, UpdateMetadata
from .collections import Collection, collection_factory, collections_name_map
from .records_ddotson import record_factory
from .cache import PortalCache
from qcfractal.exceptions import AuthenticationFailure
from .serialization import serialize, deserialize

from ..interface.models import (
    ObjectId,
)
from .keywords import KeywordSet
from qcfractal.portal.permissions import (
    UserInfo,
    RoleInfo,
    is_valid_username,
    is_valid_password,
    is_valid_rolename,
)

if TYPE_CHECKING:  # pragma: no cover
    from .collections.collection import Collection
    from ..interface.models.rest_models import (
        CollectionGETResponse,
        ComputeResponse,
        QueryObjectId,
        QueryListStr,
        QueryStr,
        ServiceQueueGETResponse,
    )


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


def make_list(obj: Optional[Union[_T, Sequence[_T]]]) -> Optional[List[_T]]:
    """
    Returns a list containing obj if obj is not a list or sequence type object
    """

    if obj is None:
        return None
    # Be careful. strings are sequences
    if isinstance(obj, str):
        return [obj]
    if not isinstance(obj, Sequence):
        return [obj]
    return list(obj)


def make_str(obj: Optional[Union[_T, Sequence[_T]]]) -> Optional[List[_T]]:
    """
    Returns a list containing obj if obj is not a list or sequence type object
    """

    if obj is None:
        return None
    # Be careful. strings are sequences
    if isinstance(obj, str):
        return obj
    if not isinstance(obj, Sequence):
        return str(obj)
    if isinstance(obj, list):
        return [str(i) for i in obj]
    if isinstance(obj, tuple):
        return tuple(str(i) for i in obj)
    else:
        raise ValueError("`obj` must be `None`, a str, list, tuple, or non-sequence")


# TODO : built-in query limit chunking, progress bars, fs caching and invalidation
class PortalClient:
    def __init__(
        self,
        address: str = "api.qcarchive.molssi.org:443",
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify: bool = True,
        cache: Optional[Union[str, Path]] = None,
        max_memcache_size: Optional[int] = 1000000,
    ) -> None:
        """Initializes a PortalClient instance from an address and verification information.

        Parameters
        ----------
        address : str or FractalServer
            The IP and port of the FractalServer instance ("192.168.1.1:8888") or
            a FractalServer instance
        username : None, optional
            The username to authenticate with.
        password : None, optional
            The password to authenticate with.
        verify : bool, optional
            Verifies the SSL connection with a third party server. This may be False if a
            FractalServer was not provided a SSL certificate and defaults back to self-signed
            SSL keys.
        cache : str, optional
            Path to directory to use for cache.
            If None, only in-memory caching used.
        max_memcache_size : int
            Number of items to hold in client's memory cache.
            Increase this value to improve performance for repeated calls,
            at the cost of higher memory usage.

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

        if (username is not None) or (password is not None):
            self._get_JWT_token(username, password)

        self._request_counter: DefaultDict[Tuple[str, str], int] = defaultdict(int)

        ### Define all attributes before this line

        # Try to connect and pull the server info
        self.server_info = self.get_server_information()
        self.server_name = self.server_info["name"]
        self.response_limits = self.server_info["response_limits"]

        server_version_min_client = parse_version(self.server_info["client_lower_version_limit"])
        server_version_max_client = parse_version(self.server_info["client_upper_version_limit"])

        client_version = parse_version(__version__)

        if not server_version_min_client <= client_version <= server_version_max_client:
            raise RuntimeError(
                f"This client version {str(client_version)} does not fall within the server's allowed "
                f"client versions of [{str(server_version_min_client)}, {str(server_version_max_client)}]."
                f"You may need to upgrade or downgrade"
            )

        self._cache = PortalCache(self, cachedir=cache, max_memcache_size=max_memcache_size)

    def __repr__(self) -> str:
        """A short representation of the current PortalClient.

        Returns
        -------
        str
            The desired representation.
        """
        ret = "PortalClient(server_name='{}', address='{}', username='{}', cache='{}')".format(
            self.server_name, self.address, self.username, self.cache
        )
        return ret

    def _repr_html_(self) -> str:

        output = f"""
        <h3>PortalClient</h3>
        <ul>
          <li><b>Server:   &nbsp; </b>{self.server_name}</li>
          <li><b>Address:  &nbsp; </b>{self.address}</li>
          <li><b>Username: &nbsp; </b>{self.username}</li>
          <li><b>Cache: &nbsp; </b>{self.cache}</li>
        </ul>
        """

        # postprocess due to raw spacing above
        return "\n".join([substr.strip() for substr in output.split("\n")])

    @property
    def encoding(self) -> str:
        return self._encoding

    @property
    def _old_encoding(self) -> str:
        return self.encoding.split("/")[1]

    @encoding.setter
    def encoding(self, encoding: str):
        self._encoding = encoding
        self._headers["Content-Type"] = encoding
        self._headers["Accept"] = encoding

    def _get_JWT_token(self, username: str, password: str) -> None:

        try:
            ret = requests.post(
                self.address + "login", json={"username": username, "password": password}, verify=self._verify
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
            self.address + "refresh", headers={"Authorization": f"Bearer {self.refresh_token}"}, verify=self._verify
        )

        if ret.status_code == 200:
            self._headers["Authorization"] = f'Bearer {ret.json()["access_token"]}'
        else:  # shouldn't happen unless user is blacklisted
            raise ConnectionRefusedError("Unable to refresh JWT authorization token! " "This is a server issue!!")

    def _request(
        self,
        method: str,
        service: str,
        *,
        data: Optional[str] = None,
        noraise: bool = False,
        timeout: Optional[int] = None,
        retry: Optional[bool] = True,
    ) -> requests.Response:

        addr = self.address + service
        kwargs = {"data": data, "timeout": timeout, "headers": self._headers, "verify": self._verify}

        if self._mock_network_error:
            raise requests.exceptions.RequestException("mock_network_error is on, failing by design!")

        try:
            if method == "get":
                r = requests.get(addr, **kwargs)
            elif method == "post":
                r = requests.post(addr, **kwargs)
            elif method == "put":
                r = requests.put(addr, **kwargs)
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
            return self._request(method, service, data=data, noraise=noraise, timeout=timeout, retry=False)

        if (r.status_code != 200) and (not noraise):
            try:
                msg = r.json()["msg"]
            except:
                msg = r.reason

            raise IOError("Server communication failure. Code: {}, Reason: {}".format(r.status_code, msg))

        return r

    def _automodel_request(
        self, name: str, rest: str, payload: Dict[str, Any], full_return: bool = False, timeout: int = None
    ) -> Any:
        """Automatic model request profiling and creation using rest_models

        Parameters
        ----------
        name : str
            The name of the REST endpoint
        rest : str
            The type of the REST endpoint
        payload : Dict[str, Any]
            The input dictionary
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.
        timeout : int, optional
            Timeout time

        Returns
        -------
        Any
            The REST response object
        """
        sname = name.strip("/")
        self._request_counter[(sname, rest)] += 1

        body_model, response_model = rest_model(sname, rest)

        # Provide a reasonable traceback
        try:
            payload = body_model(**payload)
        except ValidationError as exc:
            raise TypeError(str(exc))

        r = self._request(rest, name, data=payload.serialize(self._old_encoding), timeout=timeout)
        encoding = r.headers["Content-Type"].split("/")[1]
        response = response_model.parse_raw(r.content, encoding=encoding)

        if full_return:
            return response
        else:
            return response.data

    def _request2(
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
            return self._request2(method, endpoint, body=body, retry=False)

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

        r = self._request2(method, endpoint, body=serialized_body, url_params=parsed_url_params)
        d = deserialize(r.content, r.headers["Content-Type"])

        if response_model is None:
            return None
        else:
            return pydantic.parse_obj_as(response_model, d)

    @property
    def cache(self):
        if self._cache.cachedir is not None:
            return os.path.relpath(self._cache.cachedir)
        else:
            return None

    def _get_with_cache(self, func, id, missing_ok, entity_type, include=None):
        str_id = make_str(id)
        ids = make_list(str_id)

        # pass through the cache first
        # remove any ids that were found in cache
        # if `include` filters passed, don't use cache, just query DB, as it's often faster
        # for a few fields
        if include is None:
            cached = self._cache.get(ids, entity_type=entity_type)
        else:
            cached = {}

        for i in cached:
            ids.remove(i)

        # if all ids found in cache, no need to go further
        if len(ids) == 0:
            if isinstance(id, list):
                return [cached[i] for i in str_id]
            else:
                return cached[str_id]

        # molecule getting does *not* support "include"
        if include is None:
            payload = {
                "data": {"id": ids},
            }
        else:
            if "id" not in include:
                include.append("id")

            payload = {
                "meta": {"include": include},
                "data": {"id": ids},
            }

        results, to_cache = func(payload)

        # we only cache if no field filtering was done
        if include is None:
            self._cache.put(to_cache, entity_type=entity_type)

        # combine cached records with queried results
        results.update(cached)

        # check that we have results for all ids asked for
        missing = set(make_list(str_id)) - set(results.keys())

        if missing and not missing_ok:
            raise KeyError(f"No objects found for `id`: {missing}")

        # order the results by input id list
        if isinstance(id, list):
            ordered = [results.get(i, None) for i in str_id]
        else:
            ordered = results.get(str_id, None)

        return ordered

    # TODO - needed?
    def _query_cache(self):
        pass

    def get_server_information(self) -> Dict[str, Any]:
        """Request general information about the server

        Returns
        -------
        :
            Server information.
        """

        # Request the info, and store here for later use
        return self._auto_request("get", "v1/information", None, None, Dict[str, Any], None, None)

    # def _get_outputs(
    #    self,
    #    id: Union[int, Sequence[int]],
    #    missing_ok: bool = False,
    # ) -> Union[Optional[OutputStore], List[Optional[OutputStore]]]:
    #    """Obtains outputs from the server via output ids

    #    Note: This is the id of the output, not of the calculation record.

    #    Parameters
    #    ----------
    #    id
    #        An id or list of ids to query.
    #    missing_ok
    #        If True, return ``None`` for ids that were not found on the server.
    #        If False, raise ``KeyError`` if any ids were not found on the server.

    #    Returns
    #    -------
    #    :
    #        The requested outputs, in the same order as the requested ids.
    #        If given a list of ids, the return value will be a list.
    #        Otherwise, it will be a single output.
    #    """

    #    url_params = {"id": make_list(id), "missing_ok": missing_ok}
    #    outputs = self._auto_request(
    #        "get", "v1/output", None, CommonGetURLParameters, List[Optional[OutputStore]], None, url_params
    #    )

    #    if isinstance(id, Sequence):
    #        return outputs
    #    else:
    #        return outputs[0]

    ### Molecule section

    def get_molecules(
        self,
        id: Union[int, Sequence[int]],
        missing_ok: bool = False,
    ) -> Union[Optional[Molecule], List[Optional[Molecule]]]:
        """Obtains molecules from the server via molecule ids

        Parameters
        ----------
        id
            An id or list of ids to query.
        missing_ok
            If True, return ``None`` for ids that were not found on the server.
            If False, raise ``KeyError`` if any ids were not found on the server.

        Returns
        -------
        :
            The requested molecules, in the same order as the requested ids.
            If given a list of ids, the return value will be a list.
            Otherwise, it will be a single Molecule.
        """

        url_params = {"id": make_list(id), "missing_ok": missing_ok}
        mols = self._auto_request(
            "get", "v1/molecule", None, CommonGetURLParameters, List[Optional[Molecule]], None, url_params
        )

        if isinstance(id, Sequence):
            return mols
        else:
            return mols[0]

    # TODO: we would like more fields to be queryable via the REST API for mols
    #       e.g. symbols/elements. Unless these are indexed might not be performant.
    # TODO: what was paginate: bool = False for?
    def query_molecules(
        self,
        molecule_hash: Optional[Union[str, Iterable[str]]] = None,
        molecular_formula: Optional[Union[str, Iterable[str]]] = None,
        identifiers: Optional[Dict[str, Union[str, List[str]]]] = None,
        limit: Optional[int] = None,
        skip: int = 0,
    ) -> List[Molecule]:
        """Query molecules by attributes.

        All matching molecules, up to the lower of `limit` or the server's
        maximum result count, will be returned.

        The return list will be in an indeterminate order

        Parameters
        ----------
        molecule_hash
            Queries molecules by hash
        molecular_formula
            Queries molecules by molecular formula
            Molecular formulas are not order-sensitive (e.g. "H2O == OH2 != Oh2").
        identifiers
            Additional identifiers to search for (smiles, etc)
        limit
            The maximum number of Molecules to query.
        skip
            The number of Molecules to skip in the query, used during pagination
        """

        query_body = {
            "molecule_hash": make_list(molecule_hash),
            "molecular_formula": make_list(molecular_formula),
            "limit": limit,
            "skip": skip,
        }

        if identifiers is not None:
            query_body["identifiers"] = {k: make_list(v) for k, v in identifiers.items()}

        meta, molecules = self._auto_request(
            "post", "v1/molecule/query", MoleculeQueryBody, None, Tuple[QueryMetadata, List[Molecule]], query_body, None
        )
        return meta, molecules

    def add_molecules(self, molecules: List[Molecule]) -> List[str]:
        """Add molecules to the server.

        Parameters
        molecules
            A list of Molecules to add to the server.

        Returns
        -------
        :
            A list of Molecule ids in the same order as the `molecules` parameter.
        """

        mols = self._auto_request(
            "post",
            "v1/molecule",
            List[Molecule],
            None,
            Tuple[InsertMetadata, List[int]],
            make_list(molecules),
            None,
        )
        return mols

    def modify_molecule(
        self,
        id: int,
        name: Optional[str] = None,
        comment: Optional[str] = None,
        identifiers: Optional[Union[Dict[str, Any], MoleculeIdentifiers]] = None,
        overwrite_identifiers: bool = False,
    ) -> UpdateMetadata:
        """
        Modify molecules on the server

        This is only capable of updating the name, comment, and identifiers fields (except molecule_hash
        and molecular formula).

        If a molecule with that id does not exist, an exception is raised

        Parameters
        ----------
        id
            Molecule ID of the molecule to modify
        name
            New name for the molecule. If None, name is not changed.
        comment
            New comment for the molecule. If None, comment is not changed
        identifiers
            A new set of identifiers for the molecule
        overwrite_identifiers
            If True, the identifiers of the molecule are set to be those given exactly (ie, identifiers
            that exist in the DB but not in the new set will be removed). Otherwise, the new set of
            identifiers is merged into the existing ones. Note that molecule_hash and molecular_formula
            are never removed.

        Returns
        -------
        :
            Metadata about the modification/update.
        """

        body = {
            "name": name,
            "comment": comment,
            "identifiers": identifiers,
            "overwrite_identifiers": overwrite_identifiers,
        }

        return self._auto_request("patch", f"v1/molecule/{id}", MoleculeModifyBody, None, UpdateMetadata, body, None)

    def delete_molecules(self, id: Union[int, Sequence[int]]) -> DeleteMetadata:
        """Deletes molecules from the server

        This will not delete any keywords that are in use

        Parameters
        ----------
        id
            An id or list of ids to query.

        Returns
        -------
        :
            Metadata about what was deleted
        """

        url_params = {"id": make_list(id)}
        return self._auto_request(
            "delete", "v1/molecule", None, CommonDeleteURLParameters, DeleteMetadata, None, url_params
        )

    ### Keywords section

    def get_keywords(
        self,
        keywords_id: Union[int, Sequence[int]],
        missing_ok: bool = False,
    ) -> Union[Optional[KeywordSet], List[Optional[KeywordSet]]]:
        """Obtains keywords from the server via keyword ids

        Parameters
        ----------
        keywords_id
            An id or list of ids to query.
        missing_ok
            If True, return ``None`` for ids that were not found on the server.
            If False, raise ``KeyError`` if any ids were not found on the server.

        Returns
        -------
        :
            The requested keywords, in the same order as the requested ids.
            If given a list of ids, the return value will be a list.
            Otherwise, it will be a single KeywordSet.
        """

        url_params = {"id": make_list(keywords_id), "missing_ok": missing_ok}
        keywords = self._auto_request(
            "get", "v1/keyword", None, CommonGetURLParameters, List[Optional[KeywordSet]], None, url_params
        )

        if isinstance(keywords_id, Sequence):
            return keywords
        else:
            return keywords[0]

    def _add_keywords(
        self, keywords: Sequence[KeywordSet], full_return: bool = False
    ) -> Union[List[int], Tuple[InsertMetadata, List[int]]]:
        """Adds keywords to the server

        This function is not expected to be used by end users

        Parameters
        ----------
        keywords
            A KeywordSet or list of KeywordSet to add to the server.
        full_return
            If True, return additional metadata about the insertion. The return will be a tuple
            of (metadata, ids)

        Returns
        -------
        :
            A list of KeywordSet ids that were added or existing on the server, in the
            same order as specified in the keywords parameter. If full_return is True,
            this function will return a tuple containing metadata and the ids.
        """

        ret = self._auto_request(
            "post", "v1/keyword", List[KeywordSet], None, Tuple[InsertMetadata, List[int]], make_list(keywords), None
        )

        if full_return:
            return ret
        else:
            return ret[1]

    def _delete_keywords(self, keywords_id: Union[int, Sequence[int]]) -> DeleteMetadata:
        """Deletes keywords from the server

        This will not delete any keywords that are in use

        Parameters
        ----------
        keywords_id
            An id or list of ids to query.

        Returns
        -------
        :
            Metadata about what was deleted
        """

        url_params = {"id": make_list(keywords_id)}
        return self._auto_request(
            "delete", "v1/keyword", None, CommonDeleteURLParameters, DeleteMetadata, None, url_params
        )

    ### Collections section

    def list_collections(
        self,
        collection_type: Optional[str] = None,
        full: bool = False,
        taglines: bool = False,
        as_list: bool = False,
        as_df: bool = False,
        group: Optional[str] = "default",
        show_hidden: bool = False,
        tag: Optional[Union[str, List[str]]] = None,
    ) -> Union[None, List, pd.DataFrame]:
        """Print or return the available collections currently on the server.

        Parameters
        ----------
        collection_type : Optional[str], optional
            If `None` all collection types will be returned, otherwise only the
            specified collection type will be returned
        full : bool, optional
            Whether to include tags, group in output; default False.
        taglines : bool, optional
            Whether to include taglines in output; default False.
        as_list : bool, optional
            Return output as a list instead of printing.
        as_df : bool, optional
            Return output as a `pandas` DataFrame instead of printing.
        group : Optional[str], optional
            Show only collections belonging to a specified group.
            To explicitly return all collections, set group=None
        show_hidden : bool, optional
            Show collections whose visibility flag is set to False. Default: False.
        tag : Optional[Union[str, List[str]]], optional
            Show collections whose tags match one of the passed tags.
            By default, collections are not filtered on tag.

        Returns
        -------
        Union[None, List, pandas.DataFrame]
            Prints output as table to screen; if `as_list=True`,
            returns list of output content instead.
        """
        # preprocess inputs
        if tag is not None:
            if isinstance(tag, str):
                tag = [tag]

        query: Dict[str, str] = {}
        if collection_type is not None:
            query = {"collection": collection_type.lower()}

        payload = {"meta": {"include": ["name", "collection", "tagline", "visibility", "group", "tags"]}, "data": query}
        response: List[Dict[str, Any]] = self._automodel_request("collection", "get", payload, full_return=False)

        collection_data = sorted(response, key=lambda x: (x["collection"], x["name"]))

        # apply filters
        if not show_hidden:
            collection_data = [item for item in collection_data if item["visibility"]]
        if group is not None:
            collection_data = [item for item in collection_data if item["group"] == group]
        if tag is not None:
            collection_data = [item for item in collection_data if set(item["tags"]).intersection(tag)]
        if collection_type is not None:
            collection_data = [item for item in collection_data if item["collection"]]

        name_map = collections_name_map()
        output = []
        for item in collection_data:
            if item["collection"] in name_map:
                trimmed = {}
                collection_type_i = name_map[item["collection"]]

                if collection_type is not None:
                    if collection_type_i.lower() != collection_type.lower():
                        continue
                else:
                    trimmed["Collection Type"] = collection_type_i

                trimmed["Collection Name"] = item["name"]

                if full:
                    trimmed["Tags"] = item["tags"]
                    trimmed["Group"] = item["group"]

                if taglines:
                    trimmed["Tagline"] = item["tagline"]
                output.append(trimmed)

        # give representation
        if not (as_list or as_df):
            print(tabulate(output, headers="keys"))
        elif as_list:
            return output
        elif as_df:
            return pd.DataFrame(output)

    def get_collection(
        self,
        collection_type: str,
        name: str,
    ) -> "Collection":
        """Returns a given collection from the server.

        Parameters
        ----------
        collection_type : str
            The collection type.
        name : str
            The name of the collection.
        Returns
        -------
        Collection
            A Collection object if the given collection was found; otherwise returns `None`.

        """

        payload = {"meta": {}, "data": {"collection": collection_type, "name": name}}

        print("{} : '{}' || {}".format(collection_type, name, self.address))
        response = self._automodel_request("collection", "get", payload, full_return=True)

        # Watching for nothing found
        if len(response.data):
            return collection_factory(response.data[0], client=self)
        else:
            raise KeyError("Collection '{}:{}' not found.".format(collection_type, name))

    def add_collection(
        self, collection: Collection, overwrite: bool = False
    ) -> Union["CollectionGETResponse", List["ObjectId"]]:
        """Adds a new Collection to the server.

        Parameters
        ----------
        collection :
            The full collection data representation.
        overwrite : bool, optional
            Overwrites the collection if it already exists in the database, used for updating collection.

        Returns
        -------
        ObjectId
            The ObjectId of the added collection.

        """
        if overwrite and collection.id == "local":
            raise KeyError("Attempting to overwrite collection, but no server ID found (cannot use 'local').")

        payload = {"meta": {"overwrite": overwrite}, "data": collection.to_dict()}
        return self._automodel_request("collection", "post", payload)

    def delete_collection(self, collection_type: str, name: str) -> None:
        """Deletes a given collection from the server.

        Parameters
        ----------
        collection_type : str
            The collection type to be deleted
        name : str
            The name of the collection to be deleted

        Returns
        -------
        None
        """
        collection = self.get_collection(collection_type, name)
        self._automodel_request(f"collection/{collection.data.id}", "delete", payload={"meta": {}})

    ## TODO: we would want to cache these
    # def get_wavefunctions(
    #    self,
    #    id: Union[int, Sequence[int]],
    #    missing_ok: bool = False,
    # ) -> Union[Optional[WavefunctionProperties], List[Optional[WavefunctionProperties]]]:
    #    """Obtains wavefunction data from the server via wavefunction ids

    #    Note: This is the id of the wavefunction, not of the calculation record.

    #    Parameters
    #    ----------
    #    id
    #        An id or list of ids to query.
    #    missing_ok
    #        If True, return ``None`` for ids that were not found on the server.
    #        If False, raise ``KeyError`` if any ids were not found on the server.

    #    Returns
    #    -------
    #    :
    #        The requested wavefunctions, in the same order as the requested ids.
    #        If given a list of ids, the return value will be a list.
    #        Otherwise, it will be a single output.
    #    """

    #    url_params = {"id": make_list(id), "missing_ok": missing_ok}
    #    wfns = self._auto_request(
    #        "get",
    #        "v1/wavefunction",
    #        None,
    #        CommonGetURLParameters,
    #        List[Optional[WavefunctionProperties]],
    #        None,
    #        url_params,
    #    )

    #    if isinstance(id, Sequence):
    #        return wfns
    #    else:
    #        return wfns[0]

    def get_records(
        self,
        id: QueryObjectId,
        missing_ok: bool = False,
        include: Optional["QueryListStr"] = None,
    ) -> Union[List["Record"], "Record"]:
        """Get result records by id.

        This is used by collections to retrieve their results when demanded.
        Can reliably use the client's own caching for performance.

        Parameters
        ----------
        id : QueryObjectId
            Queries the record ``id`` field.
            Multiple ids can be included in a list; result records will be returned in the same order.
        missing_ok : bool
            If True, return ``None`` for ids with no associated result.
            If False, raise ``KeyError`` for an id with no result on the server.
        include : QueryListStr, optional
            Filters the returned fields, will return a dictionary rather than an object.

        Returns
        -------
        records : Union[List[Record], Record, List[Dict[str, Any]], Dict[str, Any]]
            If `id` is a list of ids, then a list of records will be returned in the same order.
            If `id` is a single id, then only that record will be returned.
            If `include` set, then all records will be dictionaries with only those fields and 'id'.

        """

        def get_records(payload):
            records = self._automodel_request("procedure", "get", payload)

            # if `include` filter set, we must return dicts for each record
            if ("meta" in payload) and ("include" in payload["meta"]):
                results = {res["id"]: res for res in records}
                to_cache = []
            else:
                results = {res["id"]: record_factory(res, client=self) for res in records}
                to_cache = [record for record in results.values() if record.status == RecordStatusEnum.complete]

            return results, to_cache

        return self._get_with_cache(get_records, id, missing_ok, entity_type="record", include=include)

    # TODO: expand REST API to allow more queryables from Record datamodel fields
    def query_singlepoints(
        self,
        program: QueryStr = None,
        molecule: QueryObjectId = None,
        driver: QueryStr = None,
        method: QueryStr = None,
        basis: QueryStr = None,
        keywords: QueryObjectId = None,
        status: "QueryStr" = None,
        limit: Optional[int] = None,
        skip: int = 0,
        include: Optional["QueryListStr"] = None,
    ) -> Union[List["SinglePointRecord"], List[Dict[str, Any]]]:
        """Queries SinglePointRecords from the server.

        Parameters
        ----------
        program : QueryStr, optional
            Queries the SinglePointRecord ``program`` field.
        molecule : QueryObjectId, optional
            Queries the SinglePointRecord ``molecule`` field.
        driver : QueryStr, optional
            Queries the SinglePointRecord ``driver`` field.
        method : QueryStr, optional
            Queries the SinglePointRecord ``method`` field.
        basis : QueryStr, optional
            Queries the SinglePointRecord ``basis`` field.
        keywords : QueryObjectId, optional
            Queries the SinglePointRecord ``keywords`` field.
        status : QueryStr, optional
            Queries the SinglePointRecord ``status`` field.
        limit : Optional[int], optional
            The maximum number of SinglePointRecords to query, up to server's own query limit.
        skip : int, optional
            The number of SinglePointRecords to skip in the query, used during pagination
        include : QueryListStr, optional
            Filters the returned fields, will return a dictionary rather than an object.

        Returns
        -------
        Union[List[SinglePointRecord], List[Dict[str, Any]]]
            Returns a List of found SinglePointRecords without include,
            or a List of dictionaries with `include`.
        """

        if status is None and id is None:
            status = [RecordStatusEnum.complete]

        payload = {
            "meta": {"limit": limit, "skip": skip, "include": include},
            "data": {
                "program": make_list(program),
                "molecule": make_list(molecule),
                "driver": make_list(driver),
                "method": make_list(method),
                "basis": make_list(basis),
                "keywords": make_list(keywords),
                "status": make_list(status),
            },
        }
        results = self._automodel_request("result", "get", payload)

        # Add references back to the client
        if not include:
            results = [record_factory(res, client=self) for res in results]

            # cache results if we aren't customizing the field set
            self._cache.put(results, entity_type="record")

        return results

    def query_reactions(
        self,
    ):
        ...

    def query_optimizations(
        self,
        program: Optional["QueryStr"] = None,
        status: "QueryStr" = None,
        limit: Optional[int] = None,
        skip: int = 0,
        include: Optional["QueryListStr"] = None,
    ) -> Union[List["OptimizationRecord"], List[Dict[str, Any]]]:
        """Queries Procedures from the server.

        Parameters
        ----------
        status : QueryStr, optional
            Queries the Procedure ``status`` field.
        limit : Optional[int], optional
            The maximum number of Procedures to query
        skip : int, optional
            The number of Procedures to skip in the query, used during pagination
        include : QueryListStr, optional
            Filters the returned fields, will return a dictionary rather than an object.

        Returns
        -------
        Union[List['RecordBase'], Dict[str, Any]]
            Returns a List of found RecordResult's without include, or a
            dictionary of results with include.
        """

        if status is None:
            status = [RecordStatusEnum.complete]

        payload = {
            "meta": {"limit": limit, "skip": skip, "include": include},
            "data": {
                "status": make_list(status),
            },
        }
        optimizations = self._automodel_request("optimization", "get", payload)

        if not include:
            for ind in range(len(optimizations)):
                optimizations[ind] = record_factory(optimizations[ind], client=self)

            # cache optimizations if we aren't customizing the field set
            self._cache.put(optimizations, entity_type="record")

        return optimizations

    def query_torsiondrives(
        self,
        id: Optional["QueryObjectId"] = None,
        procedure_id: Optional["QueryObjectId"] = None,
        hash_index: Optional["QueryStr"] = None,
        status: Optional["QueryStr"] = None,
        limit: Optional[int] = None,
        skip: int = 0,
        full_return: bool = False,
    ) -> Union["ServiceQueueGETResponse", List[Dict[str, Any]]]:
        """Checks the status of services in the Fractal queue.

        Parameters
        ----------
        id : QueryObjectId, optional
            Queries the Services ``id`` field.
        procedure_id : QueryObjectId, optional
            Queries the Services ``procedure_id`` field, or the ObjectId of the procedure associated with the service.
        status : QueryStr, optional
            Queries the Services ``status`` field.
        limit : Optional[int], optional
            The maximum number of Services to query
        skip : int, optional
            The number of Services to skip in the query, used during pagination
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        List[Dict[str, Any]]
            A dictionary of each match that contains the current status
            and, if an error has occurred, the error message.
        """
        payload = {
            "meta": {"limit": limit, "skip": skip},
            "data": {"id": id, "procedure_id": procedure_id, "status": status},
        }
        return self._automodel_request("service_queue", "get", payload, full_return=full_return)
        pass

    def query_gridoptimizations(self):
        ...

    ### Compute section

    def add_singlepoints(
        self,
        program: str = None,
        method: str = None,
        basis: Optional[str] = None,
        driver: str = None,
        keywords: Optional["ObjectId"] = None,
        molecule: Union["ObjectId", "Molecule", List[Union["ObjectId", "Molecule"]]] = None,
        *,
        priority: Optional[str] = None,
        protocols: Optional[Dict[str, Any]] = None,
        tag: Optional[str] = None,
        full_return: bool = False,
    ) -> "ComputeResponse":
        """
        Adds a "single" compute to the server.

        Parameters
        ----------
        program : str, optional
            The computational program to execute the result with (e.g., "rdkit", "psi4").
        method : str, optional
            The computational method to use (e.g., "B3LYP", "PBE")
        basis : Optional[str], optional
            The basis to apply to the computation (e.g., "cc-pVDZ", "6-31G")
        driver : str, optional
            The primary result that the compute will acquire {"energy", "gradient", "hessian", "properties"}
        keywords : Optional['ObjectId'], optional
            The KeywordSet ObjectId to use with the given compute
        molecule : Union['ObjectId', 'Molecule', List[Union['ObjectId', 'Molecule']]], optional
            The Molecules or Molecule ObjectId's to compute with the above methods
        priority : Optional[str], optional
            The priority of the job {"HIGH", "MEDIUM", "LOW"}. Default is "MEDIUM".
        protocols : Optional[Dict[str, Any]], optional
            Protocols for store more or less data per field. Current valid
            protocols: {'wavefunction'}
        tag : Optional[str], optional
            The computational tag to add to your compute, managers can optionally only pull
            based off the string tags. These tags are arbitrary, but several examples are to
            use "large", "medium", "small" to denote the size of the job or "project1", "project2"
            to denote different projects.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        ComputeResponse
            An object that contains the submitted ObjectIds of the new compute. This object has the following fields:
              - ids: The ObjectId's of the task in the order of input molecules
              - submitted: A list of ObjectId's that were submitted to the compute queue
              - existing: A list of ObjectId's of tasks already in the database

        Raises
        ------
        ValueError
            Description
        """

        # Scan the input
        if program is None:
            raise ValueError("Program must be specified for the computation.")
        if method is None:
            raise ValueError("Method must be specified for the computation.")
        if driver is None:
            raise ValueError("Driver must be specified for the computation.")
        if molecule is None:
            raise ValueError("Molecule must be specified for the computation.")

        # Always a list
        if not isinstance(molecule, list):
            molecule = [molecule]

        if protocols is None:
            protocols = {}

        payload = {
            "meta": {
                "procedure": "single",
                "driver": driver,
                "program": program,
                "method": method,
                "basis": basis,
                "keywords": keywords,
                "protocols": protocols,
                "tag": tag,
                "priority": priority,
            },
            "data": molecule,
        }

        return self._automodel_request("task_queue", "post", payload, full_return=full_return)

    def get_compute_history(self, record_id: int, include_outputs: bool = False) -> List[ComputeHistory]:
        url_params = {"include_outputs": include_outputs}
        return self._auto_request(
            "get",
            f"v1/record/{record_id}/compute_history",
            None,
            ComputeHistoryURLParameters,
            List[ComputeHistory],
            None,
            url_params,
        )

    def get_singlepoint(
        self, record_id: Union[int, Sequence[int]], missing_ok: bool = False
    ) -> Union[Optional[SinglePointRecord], List[Optional[SinglePointRecord]]]:
        url_params = {"id": make_list(record_id), "missing_ok": missing_ok}
        dmodels = self._auto_request(
            "get",
            "v1/record/singlepoint",
            None,
            CommonGetProjURLParameters,
            List[Optional[SinglePointRecord._DataModel]],
            None,
            url_params,
        )

        sp_records = [SinglePointRecord(self, d) for d in dmodels]
        if isinstance(record_id, Sequence):
            return sp_records
        else:
            return sp_records[0]

    def get_managers(
        self,
        name: Union[str, Sequence[str]],
        missing_ok: bool = False,
    ) -> Union[Optional[ComputeManager], List[Optional[ComputeManager]]]:
        """Obtains manager information from the server via name

        Parameters
        ----------
        name
            A manager name or list of names
        missing_ok
            If True, return ``None`` for managers that were not found on the server.
            If False, raise ``KeyError`` if any managers were not found on the server.

        Returns
        -------
        :
            The requested managers, in the same order as the requested ids.
            If given a list of ids, the return value will be a list.
            Otherwise, it will be a single manager.
        """

        url_params = {"name": make_list(name), "missing_ok": missing_ok}
        managers = self._auto_request(
            "get", "v1/manager", None, CommonGetURLParametersName, List[Optional[ComputeManager]], None, url_params
        )

        if isinstance(name, Sequence):
            return managers
        else:
            return managers[0]

    def query_managers(
        self,
        id: Optional[Union[int, Iterable[int]]] = None,
        name: Optional[Union[str, Iterable[str]]] = None,
        cluster: Optional[Union[str, Iterable[str]]] = None,
        hostname: Optional[Union[str, Iterable[str]]] = None,
        status: QueryStr = None,
        modified_before: Optional[datetime] = None,
        modified_after: Optional[datetime] = None,
        include_log: bool = False,
        limit: Optional[int] = None,
        skip: int = 0,
    ) -> Tuple[QueryMetadata, Dict[str, Any]]:
        """Obtains information about compute managers attached to this Fractal instance

        Parameters
        ----------
        id
            ID assigned to the manager (this is not the UUID. This should be used very rarely).
        name
            Queries the managers name
        cluster
            Queries the managers cluster
        hostname
            Queries the managers hostname
        status
            Queries the manager's status field
        modified_before
            Query for managers last modified before a certain time
        modified_after
            Query for managers last modified after a certain time
        include_log
            If True, include the log entries for the manager
        limit
            The maximum number of managers to query
        skip
            The number of managers to skip in the query, used during pagination

        Returns
        -------
        :
            Metadata about the query results, and a list of dictionaries with information matching the specified query.
        """

        query_body = {
            "id": make_list(id),
            "name": make_list(name),
            "cluster": make_list(cluster),
            "hostname": make_list(hostname),
            "status": make_list(status),
            "modified_before": modified_before,
            "modified_after": modified_after,
            "limit": limit,
            "skip": skip,
        }

        if include_log:
            query_body["include"] = ["*", "log"]

        return self._auto_request(
            "post",
            "v1/manager/query",
            ManagerQueryBody,
            None,
            Tuple[QueryMetadata, List[ComputeManager]],
            query_body,
            None,
        )

    def query_server_stats(
        self,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        limit: Optional[int] = None,
        skip: int = 0,
    ) -> Tuple[QueryMetadata, List[Dict[str, Any]]]:
        """Obtains individual entries in the server stats logs"""

        url_params = {"before": before, "after": after, "limit": limit, "skip": skip}
        return self._auto_request(
            "get",
            "v1/server_stats",
            None,
            ServerStatsQueryParameters,
            Tuple[QueryMetadata, List[Dict[str, Any]]],
            None,
            url_params,
        )

    def delete_server_stats(self, before: datetime):
        url_params = {"before": before}
        return self._auto_request("delete", "v1/server_stats", None, DeleteBeforeDateParameters, int, None, url_params)

    def query_access_log(
        self,
        access_type: QueryStr = None,
        access_method: QueryStr = None,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        limit: Optional[int] = None,
        skip: int = 0,
    ) -> Tuple[QueryMetadata, Dict[str, Any]]:
        """Obtains individual entries in the access logs"""

        url_params = {
            "access_type": make_list(access_type),
            "access_method": make_list(access_method),
            "before": before,
            "after": after,
            "limit": limit,
            "skip": skip,
        }

        return self._auto_request(
            "get",
            "v1/access",
            None,
            AccessLogQueryParameters,
            Tuple[QueryMetadata, List[Dict[str, Any]]],
            None,
            url_params,
        )

    def delete_access_log(self, before: datetime):
        url_params = {"before": before}
        return self._auto_request("delete", "v1/access", None, DeleteBeforeDateParameters, int, None, url_params)

    def query_error_log(
        self,
        id: QueryObjectId = None,
        username: QueryStr = None,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        limit: Optional[int] = None,
        skip: int = 0,
    ) -> Tuple[QueryMetadata, Dict[str, Any]]:
        """Obtains individual entries in the access logs"""

        url_params = {
            "id": make_list(id),
            "username": make_list(username),
            "before": before,
            "after": after,
            "limit": limit,
            "skip": skip,
        }

        return self._auto_request(
            "get",
            "v1/server_error",
            None,
            ErrorLogQueryParameters,
            Tuple[QueryMetadata, List[Dict[str, Any]]],
            None,
            url_params,
        )

    def delete_error_log(self, before: datetime):
        url_params = {"before": before}
        return self._auto_request("delete", "v1/server_error", None, DeleteBeforeDateParameters, int, None, url_params)

    def query_access_summary(
        self,
        group_by: str = "day",
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Obtains daily summaries of accesses

        Parameters
        ----------
        group_by
            How to group the data. Valid options are "user", "hour", "day", "country", "subdivision"
        before
            Query for log entries with a timestamp before a specific time
        after
            Query for log entries with a timestamp after a specific time
        """

        url_params = {
            "group_by": group_by,
            "before": before,
            "after": after,
        }

        return self._auto_request(
            "get", "v1/access/summary", None, AccessLogQuerySummaryParameters, Dict[str, Any], None, url_params
        )

    def list_roles(self) -> List[RoleInfo]:
        """
        List all user roles on the server
        """

        return self._auto_request("get", "v1/role", None, None, List[RoleInfo], None, None)

    def get_role(self, rolename: str) -> RoleInfo:
        """
        Get information about a role on the server
        """

        is_valid_rolename(rolename)
        return self._auto_request("get", f"v1/role/{rolename}", None, None, RoleInfo, None, None)

    def add_role(self, role_info: RoleInfo) -> None:
        """
        Adds a role with permissions to the server

        If not successful, an exception is raised.
        """

        is_valid_rolename(role_info.rolename)
        return self._auto_request("post", "v1/role", RoleInfo, None, None, role_info, None)

    def modify_role(self, role_info: RoleInfo) -> RoleInfo:
        """
        Modifies the permissions of a role on the server

        If not successful, an exception is raised.

        Returns
        -------
        :
            A copy of the role as it now appears on the server
        """

        is_valid_rolename(role_info.rolename)
        return self._auto_request("put", f"v1/role/{role_info.rolename}", RoleInfo, None, RoleInfo, role_info, None)

    def delete_role(self, rolename: str) -> None:
        """
        Deletes a role from the server

        This will not delete any role to which a user is assigned

        Will raise an exception on error

        Parameters
        ----------
        rolename
            Name of the role to delete

        """
        is_valid_rolename(rolename)
        return self._auto_request("delete", f"v1/role/{rolename}", None, None, None, None, None)

    def list_users(self) -> List[UserInfo]:
        """
        List all user roles on the server
        """

        return self._auto_request("get", "v1/user", None, None, List[UserInfo], None, None)

    def get_user(self, username: Optional[str] = None, as_admin: bool = False) -> UserInfo:
        """
        Get information about a user on the server

        If the username is not supplied, then info about the currently logged-in user is obtained

        Parameters
        ----------
        username
            The username to get info about
        as_admin
            If True, then fetch the user from the admin user management endpoint. This is the default
            if requesting a user other than the currently logged-in user

        Returns
        -------
        :
            Information about the user
        """

        if username is None:
            username = self.username

        if username is None:
            raise RuntimeError("Cannot get user - not logged in?")

        # Check client side so we can bail early
        is_valid_username(username)

        if username != self.username:
            as_admin = True

        if as_admin is False:
            # For the currently logged-in user, use the "me" endpoint. The other endpoint is
            # restricted to admins
            uinfo = self._auto_request("get", f"v1/me", None, None, UserInfo, None, None)

            if uinfo.username != self.username:
                raise RuntimeError(
                    f"Inconsistent username - client is {self.username} but logged in as {uinfo.username}"
                )
        else:
            uinfo = self._auto_request("get", f"v1/user/{username}", None, None, UserInfo, None, None)

        return uinfo

    def add_user(self, user_info: UserInfo, password: Optional[str] = None) -> str:
        """
        Adds a user to the server

        Parameters
        ----------
        user_info
            Info about the user to add
        password
            The user's password. If None, then one will be generated

        Returns
        -------
        :
            The password of the user (either the same as the supplied password, or the
            server-generated one)

        """

        is_valid_username(user_info.username)
        is_valid_rolename(user_info.role)

        if password is not None:
            is_valid_password(password)

        if user_info.id is not None:
            raise RuntimeError("Cannot add user when user_info contains an id")

        return self._auto_request(
            "post", "v1/user", Tuple[UserInfo, Optional[str]], None, str, (user_info, password), None
        )

    def modify_user(self, user_info: UserInfo, as_admin: bool = False) -> UserInfo:
        """
        Modifies a user on the server

        The user is determined by the username field of the input UserInfo, although the id
        and username are checked for consistency.

        Depending on the current user's permissions, some fields may not be updatable.



        Parameters
        ----------
        user_info
            Updated information for a user
        as_admin
            If True, then attempt to modify fields that are only modifiable by an admin (enabled, role).
            This is the default if requesting a user other than the currently logged-in user.

        Returns
        -------
        :
            The updated user information as it appears on the server
        """

        is_valid_username(user_info.username)
        is_valid_rolename(user_info.role)

        if as_admin or (user_info.username != self.username):
            url = f"v1/user/{user_info.username}"
        else:
            url = "v1/me"

        return self._auto_request("put", url, UserInfo, None, UserInfo, user_info, None)

    def change_user_password(self, username: Optional[str] = None, new_password: Optional[str] = None) -> str:
        """
        Change a users password

        If the username is not specified, then the current logged-in user is used.

        If the password is not specified, then one is automatically generated by the server.

        Parameters
        ----------
        username
            The name of the user whose password to change. If None, then use the currently logged-in user
        new_password
            Password to change to. If None, let the server generate one.

        Returns
        -------
        :
            The new password (either the same as the supplied one, or the server generated one
        """

        if username is None:
            username = self.username

        is_valid_username(username)

        if new_password is not None:
            is_valid_password(new_password)

        if username == self.username:
            url = "v1/me/password"
        else:
            url = f"v1/user/{username}/password"

        return self._auto_request("put", url, Optional[str], None, str, new_password, None)

    def delete_user(self, username: str) -> None:
        is_valid_username(username)

        if username == self.username:
            raise RuntimeError("Cannot delete your own user!")

        return self._auto_request("delete", f"v1/user/{username}", None, None, None, None, None)
