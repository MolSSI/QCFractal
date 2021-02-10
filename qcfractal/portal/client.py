from collections import defaultdict
import re
import os

import requests

from typing import TYPE_CHECKING, Any, DefaultDict, Dict, List, Optional, Tuple, Union
from pathlib import Path

from pydantic import ValidationError

from ..interface.models.rest_models import rest_model
from ..interface.models import build_procedure
from .collections import collection_factory, collections_name_map
from .cache import PortalCache

_ssl_error_msg = (
    "\n\nSSL handshake failed. This is likely caused by a failure to retrieve 3rd party SSL certificates.\n"
    "If you trust the server you are connecting to, try 'PortalClient(... verify=False)'"
)
_connection_error_msg = "\n\nCould not connect to server {}, please check the address and try again."

def _version_list(version):
    version_match = re.search(r"\d+\.\d+\.\d+", version)
    if version_match is None:
        raise ValueError(
            f"Could not read version of form XX.YY.ZZ from {version}. There is something very "
            f"malformed about the version string. Please report this to the Fractal developers."
        )
    version = version_match.group(0)
    return [int(x) for x in version.split(".")]

# TODO : built-in query limit chunking, progress bars, fs caching and invalidation
class PortalClient:

    def __init__(
        self,
        address: Union[str, "FractalServer"] = "api.qcarchive.molssi.org:443",
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify: bool = True,
        cache: Optional[Union[str, Path]] = None,
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
            
        """

        if hasattr(address, "get_address"):
            # We are a FractalServer-like object
            verify = address.client_verify
            address = address.get_address()

        if "http" not in address:
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
        self.encoding = "msgpack-ext"

        # Mode toggle for network error testing, not public facing
        self._mock_network_error = False

        # If no 3rd party verification, quiet urllib
        if self._verify is False:
            from urllib3.exceptions import InsecureRequestWarning

            requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

        if (username is not None) or (password is not None):
            self._headers["Authorization"] = json.dumps({"username": username, "password": password})

        from . import __version__  # Import here to avoid circular import
        from . import _isportal

        self._headers["Content-Type"] = f"application/{self.encoding}"
        self._headers["User-Agent"] = f"qcportal/{__version__}"

        self._request_counter: DefaultDict[Tuple[str, str], int] = defaultdict(int)

        ### Define all attributes before this line

        # Try to connect and pull general data
        self.server_info = self._automodel_request("information", "get", {}, full_return=True).dict()

        self.server_name = self.server_info["name"]
        self.query_limit: int = self.server_info["query_limit"]

        if _isportal:
            try:
                server_version_min_client = _version_list(self.server_info["client_lower_version_limit"])[:2]
                server_version_max_client = _version_list(self.server_info["client_upper_version_limit"])[:2]
            except KeyError:
                server_ver_str = ".".join([str(i) for i in self.server_info["version"]])
                raise IOError(
                    f"The Server at {self.address}, version {self.server_info['version']} does not report "
                    f"what Client versions it accepts! It can be almost asserted your Client is too new for "
                    f"the Server you are connecting to. Please downgrade your Client with "
                    f"the one of following commands (pip or conda):"
                    f"\n\t- pip install qcportal=={server_ver_str}"
                    f"\n\t- conda install -c conda-forge qcportal=={server_ver_str}"
                    f"\n(Only MAJOR.MINOR versions are checked)"
                )
            client_version = _version_list(__version__)[:2]
            if not server_version_min_client <= client_version <= server_version_max_client:
                client_ver_str = ".".join([str(i) for i in client_version])
                server_version_min_str = ".".join([str(i) for i in server_version_min_client])
                server_version_max_str = ".".join([str(i) for i in server_version_max_client])
                raise IOError(
                    f"This Client of version {client_ver_str} does not fall within the Server's allowed "
                    f"Client versions of [{server_version_min_str}, {server_version_max_str}] at "
                    f"Server address: {self.address}. Please change your Client version with one of the "
                    f"following commands:"
                    f"\n\t- pip install qcportal=={server_version_max_str}.*"
                    f"\n\t- conda install -c conda-forge qcportal=={server_version_max_str}.*"
                    f"\n(Only MAJOR.MINOR versions are checked and shown)"
                )

        self._cache = PortalCache(self, cachedir=cache)

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

    def _request(
        self,
        method: str,
        service: str,
        *,
        data: Optional[str] = None,
        noraise: bool = False,
        timeout: Optional[int] = None,
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

        if (r.status_code != 200) and (not noraise):
            raise IOError("Server communication failure. Reason: {}".format(r.reason))

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
            The method to use on the REST endpoint: GET, POST, PUT, DELETE
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

        r = self._request(rest, name, data=payload.serialize(self.encoding), timeout=timeout)
        encoding = r.headers["Content-Type"].split("/")[1]
        response = response_model.parse_raw(r.content, encoding=encoding)

        if full_return:
            return response
        else:
            return response.data


    @property
    def cache(self):
        return os.path.relpath(self._cache.cachedir)
    
    def get_collection(
        self,
        collection_type: str,
        name: str,
        full_return: bool = False,
        include: "QueryListStr" = None,
        exclude: "QueryListStr" = None,
    ) -> "Collection":
        """Returns a given collection from the server.

        Parameters
        ----------
        collection_type : str
            The collection type.
        name : str
            The name of the collection.
        full_return : bool, optional
            If True, returns the full server response.
        include : QueryListStr, optional
            Return only these columns.
        exclude : QueryListStr, optional
            Return all but these columns.
        Returns
        -------
        Collection
            A Collection object if the given collection was found; otherwise returns `None`.

        """

        payload = {"meta": {}, "data": {"collection": collection_type, "name": name}}
        if include is None and exclude is None:
            if collection_type.lower() in ["dataset", "reactiondataset"]:  # XXX
                payload["meta"]["exclude"] = ["contributed_values", "records"]
        else:
            payload["meta"]["include"] = include
            payload["meta"]["exclude"] = exclude

        response = self._automodel_request("collection", "get", payload, full_return=True)
        if full_return:
            return response

        # Watching for nothing found
        if len(response.data):
            return collection_factory(response.data[0], client=self)
        else:
            raise KeyError("Collection '{}:{}' not found.".format(collection_type, name))

    # TODO: make this just take collections themselves, not dicts
    def add_collection(
        self, collection: Dict[str, Any], overwrite: bool = False, full_return: bool = False
    ) -> Union["CollectionGETResponse", List["ObjectId"]]:
        """Adds a new Collection to the server.

        Parameters
        ----------
        collection : Dict[str, Any]
            The full collection data representation.
        overwrite : bool, optional
            Overwrites the collection if it already exists in the database, used for updating collection.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        List[ObjectId]
            The ObjectId's of the added collection.

        """
        # Can take in either molecule or lists

        if overwrite and ("id" not in collection or collection["id"] == "local"):
            raise KeyError("Attempting to overwrite collection, but no server ID found (cannot use 'local').")

        payload = {"meta": {"overwrite": overwrite}, "data": collection}
        return self._automodel_request("collection", "post", payload, full_return=full_return)

    # TODO: what are the query_limit rules exactly?
    #       does it use the total count of query terms, including mixed-and-matching fields
    #def _chunk_request(self, items):
    #    procedures: List[Dict[str, Any]] = []
    #    for i in range(0, len(items), self.query_limit):
    #        chunk_ids = query_ids[i : i + self.client.query_limit]
    #        procedures.extend(self.client._query_procedures(id=chunk_ids))


    def _query_procedures(
        self,
        id: Optional["QueryObjectId"] = None,
        task_id: Optional["QueryObjectId"] = None,
        procedure: Optional["QueryStr"] = None,
        program: Optional["QueryStr"] = None,
        hash_index: Optional["QueryStr"] = None,
        status: "QueryStr" = "COMPLETE",
        limit: Optional[int] = None,
        skip: int = 0,
        include: Optional["QueryListStr"] = None,
        full_return: bool = False,
    ) -> Union["ProcedureGETResponse", List[Dict[str, Any]]]:
        """Queries Procedures from the server.

        Parameters
        ----------
        id : QueryObjectId, optional
            Queries the Procedure ``id`` field.
        task_id : QueryObjectId, optional
            Queries the Procedure ``task_id`` field.
        procedure : QueryStr, optional
            Queries the Procedure ``procedure`` field.
        program : QueryStr, optional
            Queries the Procedure ``program`` field.
        hash_index : QueryStr, optional
            Queries the Procedure ``hash_index`` field.
        status : QueryStr, optional
            Queries the Procedure ``status`` field.
        limit : Optional[int], optional
            The maximum number of Procedures to query
        skip : int, optional
            The number of Procedures to skip in the query, used during pagination
        include : QueryListStr, optional
            Filters the returned fields, will return a dictionary rather than an object.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.
            Disables use of client cache.

        Returns
        -------
        Union[List['RecordBase'], Dict[str, Any]]
            Returns a List of found RecordResult's without include, or a
            dictionary of results with include.
        """
        # passthrough the cache first
        # if id is specified
        if not full_return:
            procs = self._cache.get(id)

            if isinstance(id, list):
                for i in procs:
                    id.remove(i)
            else:
                id = None

        # NOTE: is there a way to query the server with this kind of structure,
        #       but only get back object ids or perhaps hash_indices?
        #       This would allow our cache to be fairly dumb on this side, just a kv store
        payload = {
            "meta": {"limit": limit, "skip": skip, "include": include},
            "data": {
                "id": id,
                "task_id": task_id,
                "program": program,
                "procedure": procedure,
                "hash_index": hash_index,
                "status": status,
            },
        }
        response = self._automodel_request("procedure", "get", payload, full_return=True)

        if not include:
            for ind in range(len(response.data)):
                response.data[ind] = build_procedure(response.data[ind], client=self)

        if full_return:
            return response
        else:
            # NOTE: no particular order returned here
            # could put the "only complete" logic into the cache itself as a policy
            self._cache.put([proc for proc in response.data if proc.status == 'COMPLETE'])
            return response.data + list(procs.values())
