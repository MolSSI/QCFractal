"""Provides an interface the QCDB Server instance"""

import json
import os
import re
from collections import defaultdict
from typing import TYPE_CHECKING, Any, DefaultDict, Dict, List, Optional, Tuple, Union

import pandas as pd
import requests
from pydantic import ValidationError

from .collections import collection_factory, collections_name_map
from .models import build_procedure
from .models.task_models import PriorityEnum
from .models.rest_models import rest_model

if TYPE_CHECKING:  # pragma: no cover
    from qcfractal import FractalServer

    from .collections.collection import Collection
    from .models import (
        GridOptimizationInput,
        KeywordSet,
        Molecule,
        ObjectId,
        ResultRecord,
        TaskRecord,
        TorsionDriveInput,
    )
    from .models.rest_models import (
        CollectionGETResponse,
        ComputeResponse,
        KeywordGETResponse,
        MoleculeGETResponse,
        ProcedureGETResponse,
        QueryObjectId,
        QueryListStr,
        QueryStr,
        ResultGETResponse,
        ServiceQueueGETResponse,
        TaskQueueGETResponse,
    )

### Common docs

_common_docs = {"full_return": "Returns the full server response if True that contains additional metadata."}
_ssl_error_msg = (
    "\n\nSSL handshake failed. This is likely caused by a failure to retrieve 3rd party SSL certificates.\n"
    "If you trust the server you are connecting to, try 'FractalClient(... verify=False)'"
)
_connection_error_msg = "\n\nCould not connect to server {}, please check the address and try again."

### Helper functions


def _version_list(version):
    version_match = re.search(r"\d+\.\d+\.\d+", version)
    if version_match is None:
        raise ValueError(
            f"Could not read version of form XX.YY.ZZ from {version}. There is something very "
            f"malformed about the version string. Please report this to the Fractal developers."
        )
    version = version_match.group(0)
    return [int(x) for x in version.split(".")]


### Fractal Client


class FractalClient(object):
    def __init__(
        self,
        address: Union[str, "FractalServer"] = "api.qcarchive.molssi.org:443",
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify: bool = True,
    ) -> None:
        """Initializes a FractalClient instance from an address and verification information.

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

    def __repr__(self) -> str:
        """A short representation of the current FractalClient.

        Returns
        -------
        str
            The desired representation.
        """
        ret = "FractalClient(server_name='{}', address='{}', username='{}')".format(
            self.server_name, self.address, self.username
        )
        return ret

    def _repr_html_(self) -> str:

        return f"""
<h3>FractalClient</h3>
<ul>
  <li><b>Server:   &nbsp; </b>{self.server_name}</li>
  <li><b>Address:  &nbsp; </b>{self.address}</li>
  <li><b>Username: &nbsp; </b>{self.username}</li>
</ul>
"""

    def _set_encoding(self, encoding: str) -> None:
        self.encoding = encoding
        self._headers["Content-Type"] = f"application/{self.encoding}"

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

    @classmethod
    def from_file(cls, load_path: Optional[str] = None) -> "FractalClient":
        """Creates a new FractalClient from file. If no path is passed in, the
        current working directory and ~.qca/ are searched for "qcportal_config.yaml"

        Parameters
        ----------
        load_path : Optional[str], optional
            Path to find "qcportal_config.yaml", the filename, or a dictionary containing keys
            {"address", "username", "password", "verify"}

        Returns
        -------
        FractalClient
            A new FractalClient from file.
        """

        # Search canonical paths
        if load_path is None:
            test_paths = [os.getcwd(), os.path.join(os.path.expanduser("~"), ".qca")]

            for path in test_paths:
                local_path = os.path.join(path, "qcportal_config.yaml")
                if os.path.exists(local_path):
                    load_path = local_path
                    break

            if load_path is None:
                raise FileNotFoundError(
                    "Could not find `qcportal_config.yaml` in the following paths:\n    {}".format(
                        ", ".join(test_paths)
                    )
                )

        # Load if string, or use if dict
        if isinstance(load_path, str):
            load_path = os.path.join(os.path.expanduser(load_path))

            # Gave folder, not file
            if os.path.isdir(load_path):
                load_path = os.path.join(load_path, "qcportal_config.yaml")

            with open(load_path, "r") as handle:
                import yaml

                data = yaml.load(handle, Loader=yaml.FullLoader)

        elif isinstance(load_path, dict):
            data = load_path
        else:
            raise TypeError("Could not infer data from load_path of type {}".format(type(load_path)))

        if "address" not in data:
            raise KeyError("Config file must at least contain an address field.")

        return cls(data.pop("address"), **data)

    def server_information(self) -> Dict[str, str]:
        """Pull down various data on the connected server.

        Returns
        -------
        Dict[str, str]
            Server information.
        """
        return json.loads(json.dumps(self.server_info))

    ### KVStore section

    def query_kvstore(self, id: "QueryObjectId", full_return: bool = False) -> Dict[str, Any]:
        """Queries items from the database's KVStore

        Parameters
        ----------
        id : QueryObjectId
            A list of KVStore id's
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        Dict[str, Any]
            A list of found KVStore objects in {"id": "value"} format
        """

        return self._automodel_request("kvstore", "get", {"meta": {}, "data": {"id": id}}, full_return=full_return)

    ### Molecule section

    def query_molecules(
        self,
        id: Optional["QueryObjectId"] = None,
        molecule_hash: Optional["QueryStr"] = None,
        molecular_formula: Optional["QueryStr"] = None,
        limit: Optional[int] = None,
        skip: int = 0,
        full_return: bool = False,
    ) -> Union["MoleculeGETResponse", List["Molecule"]]:
        """Queries molecules from the database.

        Parameters
        ----------
        id : QueryObjectId, optional
            Queries the Molecule ``id`` field.
        molecule_hash : QueryStr, optional
            Queries the Molecule ``molecule_hash`` field.
        molecular_formula : QueryStr, optional
            Queries the Molecule ``molecular_formula`` field. Molecular formulas are case-sensitive.
            Molecular formulas are not order-sensitive (e.g. "H2O == OH2 != Oh2").
        limit : Optional[int], optional
            The maximum number of Molecules to query
        skip : int, optional
            The number of Molecules to skip in the query, used during pagination
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        List[Molecule]
            A list of found molecules.
        """

        payload = {
            "meta": {"limit": limit, "skip": skip},
            "data": {"id": id, "molecule_hash": molecule_hash, "molecular_formula": molecular_formula},
        }
        response = self._automodel_request("molecule", "get", payload, full_return=full_return)
        return response

    def add_molecules(self, mol_list: List["Molecule"], full_return: bool = False) -> List[str]:
        """Adds molecules to the Server.

        Parameters
        ----------
        mol_list : List[Molecule]
            A list of Molecules to add to the server.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        List[str]
            A list of Molecule id's in the sent order, can be None where issues occured.

        """

        return self._automodel_request("molecule", "post", {"meta": {}, "data": mol_list}, full_return=full_return)

    ### Keywords section

    def query_keywords(
        self,
        id: Optional["QueryObjectId"] = None,
        *,
        hash_index: Optional["QueryStr"] = None,
        limit: Optional[int] = None,
        skip: int = 0,
        full_return: bool = False,
    ) -> Union["KeywordGETResponse", List["KeywordSet"]]:
        """Obtains KeywordSets from the server using keyword ids.

        Parameters
        ----------
        id : QueryObjectId, optional
            A list of ids to query.
        hash_index : QueryStr, optional
            The hash index to look up
        limit : Optional[int], optional
            The maximum number of keywords to query
        skip : int, optional
            The number of keywords to skip in the query, used during pagination
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        List[KeywordSet]
            The requested KeywordSet objects.
        """

        payload = {"meta": {}, "data": {"id": id, "hash_index": hash_index}}
        return self._automodel_request("keyword", "get", payload, full_return=full_return)

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
        aslist: bool = False,
        group: Optional[str] = "default",
        show_hidden: bool = False,
        tag: Optional[Union[str, List[str]]] = None,
    ) -> pd.DataFrame:
        """Lists the available collections currently on the server.

        Parameters
        ----------
        collection_type : Optional[str], optional
            If `None` all collection types will be returned, otherwise only the
            specified collection type will be returned
        aslist : bool, optional
            Returns a canonical list rather than a dataframe.
        group: Optional[str], optional
            Show only collections belonging to a specified group.
            To explicitly return all collections, set group=None
        show_hidden: bool, optional
            Show collections whose visibility flag is set to False. Default: False.
        tag: Optional[Union[str, List[str]]], optional
            Show collections whose tags match one of the passed tags. By default, collections are not filtered on tag.
        Returns
        -------
        DataFrame
            A dataframe containing the collection, name, and tagline.
        """

        query: Dict[str, str] = {}
        if collection_type is not None:
            query = {"collection": collection_type.lower()}

        payload = {"meta": {"include": ["name", "collection", "tagline", "visibility", "group", "tags"]}, "data": query}
        response: List[Dict[str, Any]] = self._automodel_request("collection", "get", payload, full_return=False)

        # Rename collection names
        repl_name_map = collections_name_map()
        for item in response:
            item.pop("id", None)
            if item["collection"] in repl_name_map:
                item["collection"] = repl_name_map[item["collection"]]

        if len(response) == 0:
            df = pd.DataFrame(columns=["name", "collection", "tagline"])
        else:
            df = pd.DataFrame.from_dict(response)
            if not show_hidden:
                df = df[df["visibility"]]
            if group is not None:
                df = df[df["group"].str.lower() == group.lower()]
            if tag is not None:
                if isinstance(tag, str):
                    tag = [tag]
                tag = {t.lower() for t in tag}
                df = df[df.apply(lambda x: len({t.lower() for t in x["tags"]} & tag) > 0, axis=1)]

            df.drop(["visibility", "group", "tags"], axis=1, inplace=True)
        if not aslist:
            df.set_index(["collection", "name"], inplace=True)
            df.sort_index(inplace=True)
            return df
        else:
            if collection_type is None:
                ret: DefaultDict[Any, List] = defaultdict(list)
                for entry in df.iterrows():
                    ret[entry[1]["collection"]].append(entry[1]["name"])
                return dict(ret)
            else:
                return list(df.name)

    def get_collection(
        self,
        collection_type: str,
        name: str,
        full_return: bool = False,
        include: "QueryListStr" = None,
        exclude: "QueryListStr" = None,
    ) -> "Collection":
        """Acquires a given collection from the server.

        Parameters
        ----------
        collection_type : str
            The collection type to be accessed
        name : str
            The name of the collection to be accessed
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.
        include : QueryListStr, optional
            Return only these columns.
        exclude : QueryListStr, optional
            Return all but these columns.
        Returns
        -------
        Collection
            A Collection object if the given collection was found otherwise returns `None`.

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
        self._automodel_request(f"collection/{collection.data.id}", "delete", payload={"meta": {}}, full_return=True)

    ### Results section

    def query_results(
        self,
        id: Optional["QueryObjectId"] = None,
        task_id: Optional["QueryObjectId"] = None,
        program: Optional["QueryStr"] = None,
        molecule: Optional["QueryObjectId"] = None,
        driver: Optional["QueryStr"] = None,
        method: Optional["QueryStr"] = None,
        basis: Optional["QueryStr"] = None,
        keywords: Optional["QueryObjectId"] = None,
        status: "QueryStr" = "COMPLETE",
        limit: Optional[int] = None,
        skip: int = 0,
        include: Optional["QueryListStr"] = None,
        full_return: bool = False,
    ) -> Union["ResultGETResponse", List["ResultRecord"], Dict[str, Any]]:
        """Queries ResultRecords from the server.

        Parameters
        ----------
        id : QueryObjectId, optional
            Queries the Result ``id`` field.
        task_id : QueryObjectId, optional
            Queries the Result ``task_id`` field.
        program : QueryStr, optional
            Queries the Result ``program`` field.
        molecule : QueryObjectId, optional
            Queries the Result ``molecule`` field.
        driver : QueryStr, optional
            Queries the Result ``driver`` field.
        method : QueryStr, optional
            Queries the Result ``method`` field.
        basis : QueryStr, optional
            Queries the Result ``basis`` field.
        keywords : QueryObjectId, optional
            Queries the Result ``keywords`` field.
        status : QueryStr, optional
            Queries the Result ``status`` field.
        limit : Optional[int], optional
            The maximum number of Results to query
        skip : int, optional
            The number of Results to skip in the query, used during pagination
        include : QueryListStr, optional
            Filters the returned fields, will return a dictionary rather than an object.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        Union[List[RecordResult], Dict[str, Any]]
            Returns a List of found RecordResult's without include, or a
            dictionary of results with include.
        """
        payload = {
            "meta": {"limit": limit, "skip": skip, "include": include},
            "data": {
                "id": id,
                "task_id": task_id,
                "program": program,
                "molecule": molecule,
                "driver": driver,
                "method": method,
                "basis": basis,
                "keywords": keywords,
                "status": status,
            },
        }
        response = self._automodel_request("result", "get", payload, full_return=True)

        # Add references back to the client
        if not include:
            for result in response.data:
                result.__dict__["client"] = self

        if full_return:
            return response
        else:
            return response.data

    def query_procedures(
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

        Returns
        -------
        Union[List['RecordBase'], Dict[str, Any]]
            Returns a List of found RecordResult's without include, or a
            dictionary of results with include.
        """

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
            return response.data

    ### Compute section

    def add_compute(
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
            The primary result that the compute will aquire {"energy", "gradient", "hessian", "properties"}
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

    def add_procedure(
        self,
        procedure: str,
        program: str,
        program_options: Dict[str, Any],
        molecule: Union["ObjectId", "Molecule", List[Union[str, "Molecule"]]],
        priority: Optional[str] = None,
        tag: Optional[str] = None,
        full_return: bool = False,
    ) -> "ComputeResponse":
        """
        Adds a "single" Procedure to the server.

        Parameters
        ----------
        procedure : str
            The computational procedure to spawn {"optimization"}
        program : str
            The program to use for the given procedure (e.g., "geomeTRIC")
        program_options : Dict[str, Any]
            Additional options and specifications for the given procedure.
        molecule : Union[ObjectId, Molecule, List[Union[str, Molecule]]]
            The Molecules or Molecule ObjectId's to use with the above procedure
        priority : str, optional
            The priority of the job {"HIGH", "MEDIUM", "LOW"}. Default is "MEDIUM".
        tag : str, optional
            The computational tag to add to your procedure, managers can optionally only pull
            based off the string tags. These tags are arbitrary, but several examples are to
            use "large", "medium", "small" to denote the size of the job or "project1", "project2"
            to denote different projects.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        ComputeResponse
            An object that contains the submitted ObjectIds of the new procedure. This object has the following fields:
              - ids: The ObjectId's of the task in the order of input molecules
              - submitted: A list of ObjectId's that were submitted to the compute queue
              - existing: A list of ObjectId's of tasks already in the database

        """

        # Always a list
        if isinstance(molecule, str):
            molecule = [molecule]

        payload = {
            "meta": {"procedure": procedure, "program": program, "tag": tag, "priority": priority},
            "data": molecule,
        }
        payload["meta"].update(program_options)
        return self._automodel_request("task_queue", "post", payload, full_return=full_return)

    def query_tasks(
        self,
        id: Optional["QueryObjectId"] = None,
        hash_index: Optional["QueryStr"] = None,
        program: Optional["QueryStr"] = None,
        status: Optional["QueryStr"] = None,
        base_result: Optional["QueryStr"] = None,
        tag: Optional["QueryStr"] = None,
        manager: Optional["QueryStr"] = None,
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
        hash_index : QueryStr, optional
            Queries the Tasks ``hash_index`` field.
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

        >>> client.query_tasks(id="5bd35af47b878715165f8225",include=["status"])
        [{"status": "WAITING"}]


        """

        payload = {
            "meta": {"limit": limit, "skip": skip, "include": include},
            "data": {
                "id": id,
                "hash_index": hash_index,
                "program": program,
                "status": status,
                "base_result": base_result,
                "tag": tag,
                "manager": manager,
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

    def add_service(
        self,  # lgtm [py/similar-function]
        service: Union[List["GridOptimizationInput"], List["TorsionDriveInput"]],
        tag: Optional[str] = None,
        priority: Optional[str] = None,
        full_return: bool = False,
    ) -> "ComputeResponse":
        """Adds a new service to the service queue.

        Parameters
        ----------
        service : Union[GridOptimizationInput, TorsionDriveInput]
            An available service input
        tag : Optional[str], optional
            The compute tag to add the service under.
        priority : Optional[str], optional
            The priority of the job within the compute queue.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        ComputeResponse
            An object that contains the submitted ObjectIds of the new service. This object has the following fields:
              - ids: The ObjectId's of the task in the order of input molecules
              - submitted: A list of ObjectId's that were submitted to the compute queue
              - existing: A list of ObjectId's of tasks already in the database
        """
        payload = {"meta": {"tag": tag, "priority": priority}, "data": service}
        return self._automodel_request("service_queue", "post", payload, full_return=full_return)

    def query_services(
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
        hash_index : QueryStr, optional
            Queries the Services ``procedure_id`` field.
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
            "data": {"id": id, "procedure_id": procedure_id, "hash_index": hash_index, "status": status},
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
        name: Optional["QueryStr"] = None,
        status: Optional["QueryStr"] = "ACTIVE",
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
        payload = {
            "meta": {"limit": limit, "skip": skip},
            "data": {"name": name, "status": status},
        }
        return self._automodel_request("manager", "get", payload, full_return=full_return)

    # -------------------------------------------------------------------------
    # ------------------   Advanced Queries -----------------------------------
    # -------------------------------------------------------------------------

    def custom_query(
        self,
        object_name: str,
        query_type: str,
        data: Dict[str, Any],
        limit: Optional[int] = None,
        skip: int = 0,
        meta: Dict[str, Any] = None,
        include: Optional["QueryListStr"] = None,
        full_return: bool = False,
    ) -> Any:
        """Custom queries that are supported by the REST APIs.

        Parameters
        ----------
        object_name : str
            Object name like optimization, datasets, etc (TODO: add more)
        query_type : str
            The required query within the given class
        data : Dict[str, Any]
            a dictionary of the keys to be used in the query
        limit : Optional[int], optional
            The maximum number of Procedures to query
        skip : int, optional
            The number of Procedures to skip in the query, used during pagination
        meta : Dict[str, Any], optional
            Additional metadata keys to specify
        include : Optional['QueryListStr'], optional
            Filters the returned fields, will return a dictionary rather than an object.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        Any
        In the form of Dict[str, Any] (TODO)
        """

        payload = {"meta": {"limit": limit, "skip": skip, "include": include}, "data": data}
        if meta:
            payload["meta"].update(meta)

        if query_type:
            addr = f"{object_name}/{query_type}"
        else:
            addr = object_name
        response = self._automodel_request(addr, "get", payload, full_return=True)

        if full_return:
            return response
        else:
            return response.data
