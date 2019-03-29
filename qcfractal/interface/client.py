"""Provides an interface the QCDB Server instance"""

import json
import os
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union

from pydantic import ValidationError

import requests

from .collections import collection_factory, collections_name_map
from .models import GridOptimizationInput, Molecule, ObjectId, TorsionDriveInput, build_procedure
from .models.rest_models import ComputeResponse, rest_model

### Common docs

_common_docs = {"full_return": "Returns the full server response if True that contains additional metadata."}

### Fractal Client


class FractalClient(object):
    def __init__(self,
                 address: Union[str, 'FractalServer']='api.qcarchive.molssi.org:443',
                 username: Optional[str]=None,
                 password: Optional[str]=None,
                 verify: bool=True):
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
        self._headers = {}

        # If no 3rd party verification, quiet urllib
        if self._verify is False:
            from urllib3.exceptions import InsecureRequestWarning
            requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

        if (username is not None) or (password is not None):
            self._headers["Authorization"] = json.dumps({"username": username, "password": password})

        self._headers["content_type"] = 'application/json'

        # Try to connect and pull general data
        self.server_info = self._request("get", "information", {}).json()

        self.server_name = self.server_info["name"]

    def __str__(self) -> str:
        """A short representation of the current FractalClient.

        Returns
        -------
        str
            The desired representation.
        """
        ret = "FractalClient(server_name='{}', address='{}', username='{}')".format(
            self.server_name, self.address, self.username)
        return ret

    def _request(self,
                 method: str,
                 service: str,
                 payload: Dict[str, Any]=None,
                 *,
                 data: str=None,
                 noraise: bool=False,
                 timeout=None):

        addr = self.address + service
        kwargs = {
            "json": payload,
            "data": data,
            "timeout": timeout,
            "headers": self._headers,
            "verify": self._verify,
        }
        try:
            if method == "get":
                r = requests.get(addr, **kwargs)
            elif method == "post":
                r = requests.post(addr, **kwargs)
            elif method == "put":
                r = requests.put(addr, **kwargs)
            else:
                raise KeyError("Method not understood: '{}'".format(method))
        except requests.exceptions.SSLError as exc:
            error_msg = (
                "\n\nSSL handshake failed. This is likely caused by a failure to retrieve 3rd party SSL certificates.\n"
                "If you trust the server you are connecting to, try 'FractalClient(... verify=False)'")
            raise requests.exceptions.SSLError(error_msg)
        except requests.exceptions.ConnectionError as exc:
            error_msg = (
                "\n\nCould not connect to server {}, please check the address and try again.".format(self.address))
            raise requests.exceptions.ConnectionError(error_msg)

        if (r.status_code != 200) and (not noraise):
            raise IOError("Server communication failure. Reason: {}".format(r.reason))

        return r

    def _automodel_request(self,
                           name: str,
                           rest: str,
                           payload: Dict[str, Any],
                           full_return: bool=False,
                           timeout: int=None) -> Any:
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
        body_model, response_model = rest_model(name, rest)

        # Provide a reasonable traceback
        try:
            payload = body_model(**payload)
        except ValidationError as exc:
            raise TypeError(str(exc))

        r = self._request(rest, name, data=payload.json(), timeout=timeout)
        response = response_model.parse_raw(r.text)

        if full_return:
            return response
        else:
            return response.data

    @classmethod
    def from_file(cls, load_path: Optional[str]=None) -> 'FractalClient':
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
            test_paths = [os.getcwd(), os.path.join(os.path.expanduser('~'), ".qca")]

            for path in test_paths:
                local_path = os.path.join(path, "qcportal_config.yaml")
                if os.path.exists(local_path):
                    load_path = local_path
                    break

            if load_path is None:
                raise FileNotFoundError("Could not find `qcportal_config.yaml` in the following paths:\n    {}".format(
                    ", ".join(test_paths)))

        # Load if string, or use if dict
        if isinstance(load_path, str):
            load_path = os.path.join(os.path.expanduser(load_path))

            # Gave folder, not file
            if os.path.isdir(load_path):
                load_path = os.path.join(load_path, "qcportal_config.yaml")

            with open(load_path, "r") as handle:
                import yaml
                data = yaml.load(handle)

        elif isinstance(load_path, dict):
            data = load_path
        else:
            raise TypeError("Could not infer data from load_path of type {}".format(type(load_path)))

        if "address" not in data:
            raise KeyError("Config file must at least contain an address field.")

        address = data["address"]
        username = data.get("username", None)
        password = data.get("password", None)
        verify = data.get("verify", True)

        return cls(address, username=username, password=password, verify=verify)

    def server_information(self) -> Dict[str, str]:
        """Pull down various data on the connected server.

        Returns
        -------
        Dict[str, str]
            Server information.
        """
        return json.loads(json.dumps(self.server_info))

### KVStore section

    def query_kvstore(self, id: 'QueryObjectId', full_return: bool=False) -> Dict[str, Any]:
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

        return self._automodel_request("kvstore", "get", {"meta": {}, "data": id}, full_return=full_return)

### Molecule section

    def query_molecules(self,
                        id: 'QueryObjectId'=None,
                        molecule_hash: 'QueryStr'=None,
                        molecular_formula: 'QueryStr'=None,
                        full_return: bool=False) -> List[Molecule]:
        """Queries molecules from the database.

        Parameters
        ----------
        id : QueryObjectId, optional
            Queries the Molecule ``id`` field.
        molecule_hash : QueryStr, optional
            Queries the Molecule ``molecule_hash`` field.
        molecular_formula : QueryStr, optional
            Queries the Molecule ``molecular_formula`` field.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        List[Molecule]
            A list of found molecules.
        """

        payload = {
            "meta": {},
            "data": {
                "id": id,
                "molecule_hash": molecule_hash,
                "molecular_formula": molecular_formula
            }
        }
        response = self._automodel_request("molecule", "get", payload, full_return=full_return)
        return response

    def add_molecules(self, mol_list: List[Molecule], full_return: bool=False) -> List[str]:
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

    def query_keywords(self, id: 'QueryObjectId'=None, *, hash_index: 'QueryStr'=None,
                       full_return: bool=False) -> 'List[KeywordSet]':
        """Obtains KeywordSets from the server using keyword ids.

        Parameters
        ----------
        id : QueryObjectId, optional
            A list of ids to query.
        hash_index : QueryStr, optional
            The hash index to look up
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        List[KeywordSet]
            The requested KeywordSet objects.
        """

        payload = {"meta": {}, "data": {"id": id, "hash_index": hash_index}}
        return self._automodel_request("keyword", "get", payload, full_return=full_return)

    def add_keywords(self, keywords: List['KeywordSet'], full_return: bool=False) -> List[str]:
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

    def list_collections(self, collection_type: Optional[str]=None) -> Dict[str, Any]:
        """Lists the available collections currently on the server.

        Parameters
        ----------
        collection_type : Optional[str], optional
            If `None` all collection types will be returned, otherwise only the
            specified collection type will be returned

        Returns
        -------
        Dict[str, Any]
            A dictionary containing the available collection types.
        """

        query = {}
        if collection_type is not None:
            query = {"collection": collection_type.lower()}

        payload = {"meta": {"projection": {"name": True, "collection": True}}, "data": query}
        response = self._automodel_request("collection", "get", payload, full_return=False)

        if collection_type is None:
            repl_name_map = collections_name_map()
            ret = defaultdict(list)
            for entry in response:
                colname = entry["collection"]
                if colname in repl_name_map:
                    colname = repl_name_map[colname]
                ret[colname].append(entry["name"])
            return dict(ret)
        else:
            return [x["name"] for x in response]

    def get_collection(self, collection_type: str, name: str, full_return: bool=False) -> 'Collection':
        """Acquires a given collection from the server.

        Parameters
        ----------
        collection_type : str
            The collection type to be accessed
        name : str
            The name of the collection to be accessed
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        Collection
            A Collection object if the given collection was found otherwise returns `None`.

        """

        payload = {"meta": {}, "data": {"collection": collection_type, "name": name}}
        response = self._automodel_request("collection", "get", payload, full_return=True)

        if full_return:
            return response

        # Watching for nothing found
        if len(response.data):
            return collection_factory(response.data[0], client=self)
        else:
            raise KeyError("Collection '{}:{}' not found.".format(collection_type, name))

    def add_collection(self, collection: Dict[str, Any], overwrite: bool=False,
                       full_return: bool=False) -> List[ObjectId]:
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

        if overwrite and ("id" not in collection or collection['id'] == 'local'):
            raise KeyError("Attempting to overwrite collection, but no server ID found (cannot use 'local').")

        payload = {"meta": {"overwrite": overwrite}, "data": collection}
        return self._automodel_request("collection", "post", payload, full_return=full_return)

### Results section

    def query_results(self,
                      id: 'QueryObjectId'=None,
                      task_id: 'QueryObjectId'=None,
                      program: 'QueryStr'=None,
                      molecule: 'QueryObjectId'=None,
                      driver: 'QueryStr'=None,
                      method: 'QueryStr'=None,
                      basis: 'QueryStr'=None,
                      keywords: 'QueryObjectId'=None,
                      status: 'QueryStr'="COMPLETE",
                      projection: 'QueryProjection'=None,
                      full_return: bool=False) -> Union[List['RecordResult'], Dict[str, Any]]:
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
        projection : QueryProjection, optional
            Filters the returned fields, will return a dictionary rather than an object.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        Union[List[RecordResult], Dict[str, Any]]
            Returns a List of found RecordResult's without projection, or a
            dictionary of results with projection.
        """
        payload = {
            "meta": {
                "projection": projection
            },
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
            }
        }
        response = self._automodel_request("result", "get", payload, full_return=True)

        # Add references back to the client
        if not projection:
            for result in response.data:
                result.client = self

        if full_return:
            return response
        else:
            return response.data

    def query_procedures(self,
                         id: 'QueryObjectId'=None,
                         task_id: 'QueryObjectId'=None,
                         procedure: 'QueryStr'=None,
                         program: 'QueryStr'=None,
                         hash_index: 'QueryStr'=None,
                         status: 'QueryStr'="COMPLETE",
                         projection: 'QueryProjection'=None,
                         full_return: bool=False) -> Union[List['RecordBase'], Dict[str, Any]]:
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
        projection : QueryProjection, optional
            Filters the returned fields, will return a dictionary rather than an object.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        Union[List['RecordBase'], Dict[str, Any]]
            Returns a List of found RecordResult's without projection, or a
            dictionary of results with projection.
        """

        payload = {
            "meta": {
                "projection": projection
            },
            "data": {
                "id": id,
                "task_id": task_id,
                "program": program,
                "procedure": procedure,
                "hash_index": hash_index,
                "status": status,
            }
        }
        response = self._automodel_request("procedure", "get", payload, full_return=True)

        if not projection:
            for ind in range(len(response.data)):
                response.data[ind] = build_procedure(response.data[ind], client=self)

        if full_return:
            return response
        else:
            return response.data

    ### Compute section

    def add_compute(self,
                    program: str,
                    method: str,
                    basis: str,
                    driver: str,
                    keywords: Union[ObjectId, None],
                    molecule: Union[ObjectId, Molecule, List[Union[str, Molecule]]],
                    priority: str=None,
                    tag: str=None,
                    full_return: bool=False) -> ComputeResponse:
        """
        Adds a "single" compute to the server.

        Parameters
        ----------
        program : str
            The computational program to execute the result with (e.g., "rdkit", "psi4").
        method : str
            The computational method to use (e.g., "B3LYP", "PBE")
        basis : str
            The basis to apply to the computation (e.g., "cc-pVDZ", "6-31G")
        driver : str
            The primary result that the compute will aquire {"energy", "gradient", "hessian", "properties"}
        keywords : Union[str, None]
            The KeywordSet ObjectId to use with the given compute
        molecule : Union[str, Molecule, List[Union[str, Molecule]]]
            The Molecules or Molecule ObjectId's to compute with the above methods
        priority : str, optional
            The priority of the job {"HIGH", "MEDIUM", "LOW"}. Default is "MEDIUM".
        tag : str, optional
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
        """

        # Always a list
        if not isinstance(molecule, list):
            molecule = [molecule]

        payload = {
            "meta": {
                "procedure": "single",
                "driver": driver,
                "program": program,
                "method": method,
                "basis": basis,
                "keywords": keywords,
                "tag": tag,
                "priority": priority,
            },
            "data": molecule
        }

        return self._automodel_request("task_queue", "post", payload, full_return=full_return)

    def add_procedure(self,
                      procedure: str,
                      program: str,
                      program_options: Dict[str, Any],
                      molecule: Union[ObjectId, Molecule, List[Union[str, Molecule]]],
                      priority: str=None,
                      tag: str=None,
                      full_return: bool=False) -> ComputeResponse:
        """Adds a "single" Procedure to the server.
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
            "meta": {
                "procedure": procedure,
                "program": program,
                "tag": tag,
                "priority": priority,
            },
            "data": molecule
        }
        payload["meta"].update(program_options)
        return self._automodel_request("task_queue", "post", payload, full_return=full_return)

    def query_tasks(self,
                    id: 'QueryObjectId'=None,
                    hash_index: 'QueryStr'=None,
                    program: 'QueryStr'=None,
                    status: 'QueryStr'=None,
                    projection: 'QueryProjection'=None,
                    full_return: bool=False):
        """Checks the status of tasks in the Fractal queue.

        Parameters
        ----------
        id : QueryObjectId, optional
            Queries the Services ``id`` field.
        hash_index : QueryStr, optional
            Queries the Services ``procedure_id`` field.
        program : QueryStr, optional
            Queries the Services ``program`` field.
        status : QueryStr, optional
            Queries the Services ``status`` field.
        projection : QueryProjection, optional
            Filters the returned fields, will return a dictionary rather than an object.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        list of dict
            A dictionary of each match that contains the current status
            and, if an error has occured, the error message.

		Examples
		--------
        >>> client.check_tasks(id="5bd35af47b878715165f8225", projection={"status": True})
        [{"status": "WAITING"}]

        """

        payload = {
            "meta": {
                "projection": projection
            },
            "data": {
                "id": id,
                "hash_index": hash_index,
                "program": program,
                "status": status
            }
        }

        return self._automodel_request("task_queue", "get", payload, full_return=full_return)

    def add_service(self,
                    service: Union[GridOptimizationInput, TorsionDriveInput],
                    tag: Optional[str]=None,
                    priority: Optional[str]=None,
                    full_return: bool=False) -> ComputeResponse:
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
        payload = {
            "meta": {
                "tag": tag,
                "priority": priority
            },
            "data": service,
        }

        return self._automodel_request("service_queue", "post", payload, full_return=full_return)

    def query_services(self,
                       id: 'QueryObjectId'=None,
                       procedure_id: 'QueryObjectId'=None,
                       hash_index: 'QueryStr'=None,
                       status: 'QueryStr'=None,
                       projection: 'QueryProjection'=None,
                       full_return: bool=False):
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
        projection : QueryProjection, optional
            Filters the returned fields, will return a dictionary rather than an object.
        full_return : bool, optional
            Returns the full server response if True that contains additional metadata.

        Returns
        -------
        list of dict
            A dictionary of each match that contains the current status
            and, if an error has occurred, the error message.

        Examples
        --------
        >>> client.check_services(id="5bd35af47b878715165f8225", projection={"status": True})
        [{"status": "RUNNING"}]

        """
        payload = {
            "meta": {
                "projection": projection
            },
            "data": {
                "id": id,
                "procedure_id": procedure_id,
                "hash_index": hash_index,
                "status": status
            }
        }
        return self._automodel_request("service_queue", "get", payload, full_return=full_return)
