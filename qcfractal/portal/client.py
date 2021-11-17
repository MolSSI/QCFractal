from __future__ import annotations
from collections import defaultdict
from tabulate import tabulate

from datetime import datetime
import json
import os
from pkg_resources import parse_version
from . import __version__

import requests

from typing import TYPE_CHECKING, Any, DefaultDict, Dict, List, Optional, Tuple, Union, TypeVar, Sequence
from pathlib import Path

from pydantic import ValidationError
import pandas as pd

from ..interface.models.rest_models import rest_model
from ..interface.models import RecordStatusEnum, ManagerStatusEnum, PriorityEnum
from .collections import Collection, collection_factory, collections_name_map
from .records import record_factory
from .cache import PortalCache

if TYPE_CHECKING:  # pragma: no cover
    from .collections.collection import Collection
    from ..interface.models import (
        KeywordSet,
        Molecule,
        ObjectId,
        TaskRecord,
    )
    from ..interface.models.rest_models import (
        CollectionGETResponse,
        ComputeResponse,
        KeywordGETResponse,
        QueryObjectId,
        QueryListStr,
        QueryStr,
        ServiceQueueGETResponse,
        TaskQueueGETResponse,
        WavefunctionStoreGETResponse,
    )


_T = TypeVar("_T")


_ssl_error_msg = (
    "\n\nSSL handshake failed. This is likely caused by a failure to retrieve 3rd party SSL certificates.\n"
    "If you trust the server you are connecting to, try 'PortalClient(... verify=False)'"
)
_connection_error_msg = "\n\nCould not connect to server {}, please check the address and try again."


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
        address: Union[str, "FractalServer"] = "api.qcarchive.molssi.org:443",
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
        self._headers["User-Agent"] = f"qcportal/{__version__}"
        self.encoding = "msgpack-ext"

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

        # Try to connect and pull general data
        self.server_info = self._automodel_request("information", "get", {}, full_return=True).dict()

        self.server_name = self.server_info["name"]
        self.query_limits = self.server_info["query_limits"]

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

    @encoding.setter
    def encoding(self, encoding: str):
        self._encoding = encoding
        self._headers["Content-Type"] = f"application/{self.encoding}"

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
            raise ConnectionRefusedError("Authentication failed, wrong username or password.")

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

        r = self._request(rest, name, data=payload.serialize(self.encoding), timeout=timeout)
        encoding = r.headers["Content-Type"].split("/")[1]
        response = response_model.parse_raw(r.content, encoding=encoding)

        if full_return:
            return response
        else:
            return response.data

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

    def server_information(self) -> Dict[str, Any]:
        """Pull down various data on the connected server.

        Returns
        -------
        Dict[str, str]
            Server information.
        """
        return json.loads(json.dumps(self.server_info))

    ### KVStore / OutputStore section

    def _get_outputs(self, id: "QueryObjectId") -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Get output_store items by id.

        Parameters
        ----------
        id : QueryObjectId
            Queries the KVStore by key.
            Multiple ids can be included in a list; KVStore items will be returned in the same order.

        Returns
        -------
        results : Union[List[Dict[str, Any]], Dict[str, Any]]
            If `id` is a list of ids, then a list of items will be returned in the same order.
            If `id` is a single id, then only that item will be returned.

        """
        # TODO: consider utilizing the client cache for these

        payload = {
            "meta": {},
            "data": {
                "id": id,
            },
        }

        results = self._automodel_request("kvstore", "get", payload)

        if isinstance(id, list):
            return [results[i] for i in id]
        else:
            return results[id]

    ### Molecule section

    def get_molecules(
        self,
        id: QueryObjectId,
        missing_ok: bool = False,
    ) -> Union[List["Molecule"], "Molecule"]:
        """Get molecules by id.

        Uses the client's own caching for performance.

        Parameters
        ----------
        id : QueryObjectId
            Queries the record ``id`` field.
            Multiple ids can be included in a list; result records will be returned in the same order.
        missing_ok : bool
            If True, return ``None`` for ids with no associated result.
            If False, raise ``KeyError`` for an id with no result on the server.

        Returns
        -------
        records : Union[List[Record], Record]
            If `id` is a list of ids, then a list of records will be returned in the same order.
            If `id` is a single id, then only that record will be returned.

        """

        def get_mols(payload):
            mols = self._automodel_request("molecule", "get", payload)
            results = {mol.id: mol for mol in mols}
            to_cache = mols

            return results, to_cache

        return self._get_with_cache(get_mols, id, missing_ok, entity_type="molecule")

    # TODO: we would like more fields to be queryable via the REST API for mols
    #       e.g. symbols/elements. Unless these are indexed might not be performant.
    # TODO: for query methods, hands tied to what the REST API exposes
    def query_molecules(
        self,
        molecule_hash: Optional["QueryStr"] = None,
        molecular_formula: Optional["QueryStr"] = None,
        limit: Optional[int] = None,
        skip: int = 0,
        paginate: bool = False,
    ) -> List["Molecule"]:
        """Query molecules by attributes.

        All matching molecules, up to the lower of `limit` or the server's
        maximum result count, will be returned.

        Parameters
        ----------
        molecule_hash : QueryStr, optional
            Queries the Molecule ``molecule_hash`` field.
        molecular_formula : QueryStr, optional
            Queries the Molecule ``molecular_formula`` field. Molecular formulas are case-sensitive.
            Molecular formulas are not order-sensitive (e.g. "H2O == OH2 != Oh2").
        limit : Optional[int], optional
            The maximum number of Molecules to query.
        skip : int, optional
            The number of Molecules to skip in the query, used during pagination

        """
        payload = {
            "meta": {"limit": limit, "skip": skip},
            "data": {"molecule_hash": molecule_hash, "molecular_formula": molecular_formula},
        }
        molecules = self._automodel_request("molecule", "get", payload)

        # cache results
        self._cache.put(molecules, entity_type="molecule")

        return molecules

    def add_molecules(self, molecules: List["Molecule"]) -> List[str]:
        """Add molecules to the server.

        Parameters
        ----------
        molecules : List[Molecule]
            A list of Molecules to add to the server.

        Returns
        -------
        List[str]
            A list of Molecule ids in `molecules` order;
            `None` given for molecules that fail to add.

        """
        return self._automodel_request("molecule", "post", {"meta": {}, "data": molecules})

    ### Keywords section

    def get_keywords(
        self,
        id: Optional["QueryObjectId"] = None,
        missing_ok: bool = False,
    ) -> Union["KeywordGETResponse", List["KeywordSet"]]:
        """Obtains KeywordSets from the server using keyword ids.

        Parameters
        ----------
        id : QueryObjectId, optional
            A list of ids to query.

        Returns
        -------
        List[KeywordSet]
            The requested KeywordSet objects.
        """
        str_id = make_str(id)
        ids = make_list(str_id)

        # workaround since keywords don't have thier own id field set
        # otherwise we could do this all in one API query
        results = {}
        for i in ids:
            payload = {"meta": {}, "data": {"id": i}}
            result = self._automodel_request("keyword", "get", payload)
            if result:
                results[i] = result[0]
            else:
                results[i] = None

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

    # TODO: make this invisible?
    # one of our goals initially was to make keyword handling an implementation detail for the server
    def add_keywords(self, keywords: List["KeywordSet"], full_return: bool = False) -> List[str]:
        """Adds KeywordSets to the server.

        Parameters
        ----------
        keywords : List[KeywordSet]
            A list of KeywordSets to add.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        List[str]
            A list of KeywordSet id's in the sent order, can be None where issues occured.
        """
        return self._automodel_request("keyword", "post", {"meta": {}, "data": keywords}, full_return=full_return)

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

    ### Results section

    # TODO: Grabbed the raw version from `next`. Needs fixing up
    # TODO: we would want to cache these
    def get_wavefunctions(
        self,
        id: ObjectId,
        include: QueryListStr = None,
        full_return: bool = False,
    ) -> Union[WavefunctionStoreGETResponse, Dict[str, Any]]:
        """Queries ResultRecords from the server.

        Parameters
        ----------
        id
            Queries the Result ``id`` field.
        include
            Filters the returned fields
        full_return
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        :
            Returns a List of found Wavefunction data
        """

        payload = {
            "meta": {"include": include},
            "data": {
                "id": id,
            },
        }
        response = self._automodel_request("wavefunctionstore", "get", payload, full_return=True)

        # Add references back to the client
        if not include:
            for result in response.data:
                result.__dict__["client"] = self

        if full_return:
            return response
        else:
            return response.data

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

    def add_reactions(self):
        ...

    def add_optimizations(self):
        ...

    def add_torsiondrives(self):
        pass

    def add_gridoptimizations(self):
        pass

    def get_tasks(
        self,
        id: "QueryObjectId",
    ):
        pass

    def query_tasks(
        self,
        id: QueryObjectId = None,
        program: QueryStr = None,
        status: QueryStr = None,
        base_result: QueryStr = None,
        tag: QueryStr = None,
        manager: QueryStr = None,
        limit: Optional[int] = None,
        skip: int = 0,
        include: Optional["QueryListStr"] = None,
        full_return: bool = False,
    ) -> Union["TaskQueueGETResponse", List["TaskRecord"], List[Dict[str, Any]]]:
        """Checks the status of Tasks in the Fractal queue.

        Parameters
        ----------
        id : QueryObjectId, optional
            Queries the Tasks ``id`` field.
        program : QueryStr, optional
            Queries the Tasks ``program`` field.
        status : QueryStr, optional
            Queries the Tasks ``status`` field.
        base_result : QueryStr, optional
            Queries the Tasks ``base_result`` field.
        tag : QueryStr, optional
            Queries the Tasks ``tag`` field.
        manager : QueryStr, optional
            Queries the Tasks ``manager`` field.
        limit : Optional[int], optional
            The maximum number of Tasks to query
        skip : int, optional
            The number of Tasks to skip in the query, used during pagination
        include : QueryListStr, optional
            Filters the returned fields, will return a dictionary rather than an object.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        List[Dict[str, Any]]
            A dictionary of each match that contains the current status
            and, if an error has occurred, the error message.

        Examples
        --------

        >>> client.query_tasks(id="12345",include=["status"])
        [{"status": "WAITING"}]


        """

        payload = {
            "meta": {"limit": limit, "skip": skip, "include": include},
            "data": {
                "id": make_list(id),
                "program": make_list(program),
                "status": make_list(status),
                "base_result": make_list(base_result),
                "tag": make_list(tag),
                "manager": make_list(manager),
            },
        }

        return self._automodel_request("task_queue", "get", payload, full_return=full_return)

    def modify_tasks(
        self,  # lgtm [py/similar-function]
        operation: str,
        base_result: "QueryObjectId",
        id: Optional["QueryObjectId"] = None,
        new_tag: Optional[str] = None,
        new_priority: Optional[int] = None,
        full_return: bool = False,
    ) -> int:
        """Summary

        Parameters
        ----------
        operation : str
            The operation to perform on the selected tasks. Valid operations are:
             - `restart` - Restarts a task by moving its status from 'ERROR' to 'WAITING'
             - `regenerate` - Regenerates a missing task
             - `modify` - Modify a tasks tag or priority
        base_result : QueryObjectId
            The id of the result that the task is associated with.
        id : QueryObjectId, optional
            The id of the individual task to restart. As a note querying tasks via their id is rarely performed and
            is often an internal quantity.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        int
            The number of modified tasks.
        """
        operation = operation.lower()
        valid_ops = {"restart", "regenerate", "modify"}

        if operation not in valid_ops:
            raise ValueError(f"Operation '{operation}' is not available, valid operations are: {valid_ops}")

        # make sure priority is valid
        if new_priority is not None:
            new_priority = PriorityEnum(new_priority).value

        payload = {
            "meta": {"operation": operation},
            "data": {"id": id, "base_result": base_result, "new_tag": new_tag, "new_priority": new_priority},
        }

        return self._automodel_request("task_queue", "put", payload, full_return=full_return)

    def query_services(
        self,
        id: Optional["QueryObjectId"] = None,
        procedure_id: Optional["QueryObjectId"] = None,
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
            "data": {"id": make_list(id), "procedure_id": make_list(procedure_id), "status": make_list(status)},
        }
        return self._automodel_request("service_queue", "get", payload, full_return=full_return)

    def modify_services(
        self,
        operation: str,
        id: Optional["QueryObjectId"] = None,
        procedure_id: Optional["QueryObjectId"] = None,
        full_return: bool = False,
    ) -> int:
        """Checks the status of services in the Fractal queue.

        Parameters
        ----------
        operation : str
            The operation to perform on the selected tasks. Valid operations are:
             - `restart` - Restarts a task by moving its status from 'ERROR'/'WAITING' to 'RUNNING'
        id : QueryObjectId, optional
            Queries the Services ``id`` field.
        procedure_id : QueryObjectId, optional
            Queries the Services ``procedure_id`` field, or the ObjectId of the procedure associated with the service.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        int
            The number of modified tasks.
        """
        operation = operation.lower()
        valid_ops = {"restart"}

        if operation not in valid_ops:
            raise ValueError(f"Operation '{operation}' is not available, valid operations are: {valid_ops}")

        payload = {"meta": {"operation": operation}, "data": {"id": id, "procedure_id": procedure_id}}

        return self._automodel_request("service_queue", "put", payload, full_return=full_return)

    def query_managers(
        self,
        name: QueryStr = None,
        status: QueryStr = None,
        limit: Optional[int] = None,
        skip: int = 0,
        full_return: bool = False,
    ) -> Dict[str, Any]:
        """Obtains information about compute managers attached to this Fractal instance

        Parameters
        ----------
        name : QueryStr, optional
            Queries the managers name.
        status : QueryStr, optional
            Queries the manager's ``status`` field. Default is to search for only ACTIVE managers
        limit : Optional[int], optional
            The maximum number of managers to query
        skip : int, optional
            The number of managers to skip in the query, used during pagination
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        List[Dict[str, Any]]
            A dictionary of each match that contains all the information for each manager
        """

        if status is None and name is None:
            status = [ManagerStatusEnum.active]

        payload = {
            "meta": {"limit": limit, "skip": skip},
            "data": {"name": make_list(name), "status": make_list(status)},
        }
        return self._automodel_request("manager", "get", payload, full_return=full_return)

    def query_server_stats(
        self,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        limit: Optional[int] = None,
        skip: int = 0,
        full_return: bool = False,
    ) -> Dict[str, Any]:
        """Obtains individual entries in the server stats logs"""

        payload = {
            "meta": {"limit": limit, "skip": skip},
            "data": {"before": before, "after": after},
        }
        return self._automodel_request("server_stats", "get", payload, full_return=full_return)

    def query_access_log(
        self,
        access_type: QueryStr = None,
        access_method: QueryStr = None,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        limit: Optional[int] = None,
        skip: int = 0,
        full_return: bool = False,
    ) -> Dict[str, Any]:
        """Obtains individual entries in the access logs"""

        payload = {
            "meta": {"limit": limit, "skip": skip},
            "data": {
                "access_type": make_list(access_type),
                "access_method": make_list(access_method),
                "before": before,
                "after": after,
            },
        }
        return self._automodel_request("access/log", "get", payload, full_return=full_return)

    def query_error_log(
        self,
        id: QueryObjectId = None,
        user: QueryStr = None,
        before: Optional[datetime] = None,
        after: Optional[datetime] = None,
        limit: Optional[int] = None,
        skip: int = 0,
        full_return: bool = False,
    ) -> Dict[str, Any]:
        """Obtains individual entries in the access logs"""

        payload = {
            "meta": {"limit": limit, "skip": skip},
            "data": {"id": make_list(id), "user": make_list(user), "before": before, "after": after},
        }
        return self._automodel_request("error", "get", payload, full_return=full_return)

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

        payload = {
            "meta": {},
            "data": {"group_by": group_by, "before": before, "after": after},
        }
        return self._automodel_request("access/summary", "get", payload, full_return=False)
