"""Provides an interface the QCDB Server instance"""

import json
import os
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union

import requests

from .collections import collection_factory, collections_name_map
from .models import GridOptimizationInput, Molecule, TorsionDriveInput, build_procedure
from .models.rest_models import (
    CollectionGETBody, CollectionGETResponse, CollectionPOSTBody, CollectionPOSTResponse, KeywordGETBody,
    KeywordGETResponse, KeywordPOSTBody, KeywordPOSTResponse, KVStoreGETBody, KVStoreGETResponse, MoleculeGETBody,
    MoleculeGETResponse, MoleculePOSTBody, MoleculePOSTResponse, ProcedureGETBody, ProcedureGETReponse, ResultGETBody,
    ResultGETResponse, ServiceQueueGETBody, ServiceQueueGETResponse, ServiceQueuePOSTBody, ServiceQueuePOSTResponse,
    TaskQueueGETBody, TaskQueueGETResponse, TaskQueuePOSTBody, TaskQueuePOSTResponse)


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
        """A short short representation of the current FractalClient.

        Returns
        -------
        str
            The desired representation.
        """
        ret = "FractalClient(server_name='{}', address='{}', username='{}')".format(
            self.server_name, self.address, self.username)
        return ret

    def _request(self, method: str, service: str, payload: Dict[str, Any]=None, *, data: str=None,
                 noraise: bool=False):

        addr = self.address + service
        try:
            if method == "get":
                r = requests.get(addr, json=payload, data=data, headers=self._headers, verify=self._verify)
            elif method == "post":
                r = requests.post(addr, json=payload, data=data, headers=self._headers, verify=self._verify)
            elif method == "put":
                r = requests.put(addr, json=payload, data=data, headers=self._headers, verify=self._verify)
            else:
                raise KeyError("Method not understood: '{}'".format(method))
        except requests.exceptions.SSLError as exc:
            error_msg = (
                "\n\nSSL handshake failed. This is likely caused by a failure to retrive 3rd party SSL certificates.\n"
                "If you trust the server you are connecting to, try 'FractalClient(... verify=False)'")
            raise requests.exceptions.SSLError(error_msg)
        except requests.exceptions.ConnectionError as exc:
            error_msg = (
                "\n\nCould not connect to server {}, please check the address and try again.".format(self.address))
            raise requests.exceptions.ConnectionError(error_msg)

        if (r.status_code != 200) and (not noraise):
            raise requests.exceptions.HTTPError("Server communication failure. Reason: {}".format(r.reason))

        return r

    @classmethod
    def from_file(cls, load_path: Optional[str]=None) -> 'FractalClient':
        """Creates a new FractalClient from file. If no path is passed in searches
        current working directory and ~.qca/ for "qcportal_config.yaml"

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
            raise KeyError("Config file must at least contain a address field.")

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

    def query_kvstore(self, id: List[str], full_return: bool=False) -> Dict[str, Any]:
        """Queries items from the database's KVStore

        Parameters
        ----------
        id : List[str]
            A list of KVStore id's
        full_return : bool, optional
            Optionally returns the full request result including the ``meta`` field.

        Returns
        -------
        Dict[str, Any]
            A list of found KVStore objects in {"id": "value"} format
        """
        body = KVStoreGETBody(data=id, meta={})
        r = self._request("get", "kvstore", data=body.json())
        r = KVStoreGETResponse.parse_raw(r.text)

        if full_return:
            return r
        else:
            return r.data

### Molecule section

    def query_molecules(self,
                        id: Optional[List[str]]=None,
                        molecule_hash: Optional[List[str]]=None,
                        molecular_formula: Optional[List[str]]=None,
                        full_return: bool=False) -> 'List[Molecule]':
        """Queries molecules from the database.

        Parameters
        ----------
        id : Optional[List[str]], optional
            Queries the Molecule ``id`` field.
        molecule_hash : Optional[List[str]], optional
            Queries the Molecule ``molecule_hash`` field.
        molecular_formula : Optional[List[str]], optional
            Queries the Molecule ``molecular_formula`` field.
        full_return : bool, optional
            Optionally returns the full request result including the ``meta`` field.

        Returns
        -------
        Dict[str, 'Molecule']
            A list of found molecules.
        """
        data = {"id": id, "molecule_hash": molecule_hash, "molecular_formula": molecular_formula}
        body = MoleculeGETBody(data=data, meta={})
        r = self._request("get", "molecule", data=body.json())
        r = MoleculeGETResponse.parse_raw(r.text)

        if full_return:
            return r
        else:
            return r.data

    def add_molecules(self, mol_list: List[Molecule], full_return: bool=False) -> List[str]:
        """Adds molecules to the Server.

        Parameters
        ----------
        mol_list : List[Molecule]
            A list of Molecules to add to the server.
        full_return : bool, optional
            Optionally returns the full request result including the ``meta`` field.

        Returns
        -------
        List[str]
            A list of Molecule id's in the sent order, can be None where issues occured.

        """

        body = MoleculePOSTBody(meta={}, data=mol_list)
        r = self._request("post", "molecule", data=body.json())
        r = MoleculePOSTResponse.parse_raw(r.text)

        if full_return:
            return r
        else:
            return r.data

### Keywords section

    def query_keywords(self, id: List[str]=None, *, hash_index: List[str]=None, full_return: bool=False) -> 'List[KeywordSet]':
        """Obtains KeywordSets from the server using keyword ids.

        Parameters
        ----------
        id : List[str]
            A list of ids to query.
        full_return : bool, optional
            Optionally returns the full request result including the ``meta`` field.

        Returns
        -------
        List[KeywordSet]
            The requested KeywordSet objects.
        """
        data = {"id": id, "hash_index": hash_index}
        body = KeywordGETBody(meta={}, data=data)
        r = self._request("get", "keyword", data=body.json())
        r = KeywordGETResponse.parse_raw(r.text)

        if full_return:
            return r
        else:
            return r.data

    def add_keywords(self, keywords: 'List[KeywordSet]', full_return: bool=False) -> List[str]:
        """Adds KeywordSets to the server.

        Parameters
        ----------
        keywords : List[KeywordSet]
            A list of KeywordSets to add.
        full_return : bool, optional
            Optionally returns the full request result including the ``meta`` field.

        Returns
        -------
        List[str]
            A list of KeywordSet id's in the sent order, can be None where issues occured.
        """
        body = KeywordPOSTBody(meta={}, data=keywords)
        r = self._request("post", "keyword", data=body.json())
        r = KeywordPOSTResponse.parse_raw(r.text)

        if full_return:
            return r
        else:
            return r.data

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
        r = self._request("get", "collection", payload)

        if collection_type is None:
            repl_name_map = collections_name_map()
            ret = defaultdict(list)
            for entry in r.json()["data"]:
                colname = entry["collection"]
                if colname in repl_name_map:
                    colname = repl_name_map[colname]
                ret[colname].append(entry["name"])
            return dict(ret)
        else:
            return [x["name"] for x in r.json()["data"]]

    def get_collection(self, collection_type: str, name: str, full_return: bool=False) -> 'Collection':
        """Acquires a given collection from the server

        Parameters
        ----------
        collection_type : str
            The collection type to be accessed
        name : str
            The name of the collection to be accessed
        full_return : bool, optional
            If False, returns a Collection object otherwise returns raw JSON

        Returns
        -------
        Collection
            A Collection object if the given collection was found otherwise returns `None`.

        """

        body = CollectionGETBody(meta={}, data={"collection": collection_type.lower(), "name": name.lower()})
        r = self._request("get", "collection", data=body.json())
        cols = CollectionGETResponse.parse_raw(r.text)
        if full_return:
            return cols
        else:
            # If nothing found
            if len(cols.data):
                return collection_factory(cols.data[0], client=self)
            else:
                raise KeyError("Collection '{}:{}' not found.".format(collection_type, name))

    def add_collection(self, collection: Dict[str, Any], overwrite: bool=False, full_return: bool=False):

        # Can take in either molecule or lists

        if overwrite and ("id" not in collection or collection['id'] == 'local'):
            raise KeyError("Attempting to overwrite collection, but no server ID found (cannot use 'local').")

        payload = {"meta": {"overwrite": overwrite}, "data": collection}
        body = CollectionPOSTBody(**payload)
        r = self._request("post", "collection", data=body.json())
        assert r.status_code == 200
        r = CollectionPOSTResponse.parse_raw(r.text)

        if full_return:
            return r
        else:
            return r.data

### Results section

    def query_results(self, **kwargs):
        projection = kwargs.pop("projection", None)
        full_return = kwargs.pop("full_return", False)

        payload = {"meta": {}, "data": kwargs}
        if projection is not None:
            payload["meta"]["projection"] = projection

        body = ResultGETBody(**payload)
        r = self._request("get", "result", data=body.json())
        r = ResultGETResponse.parse_raw(r.text)

        # Add references back to the client
        if not projection:
            for result in r.data:
                result.client = self

        if full_return:
            return r
        else:
            return r.data

    def check_results(self, **kwargs):

        kwargs["status"] = None
        if "projection" in kwargs:
            kwargs["projection"]["status"] = True
        else:
            kwargs["projection"] = {"status": True}
        return self.query_results(**kwargs)

    def query_procedures(self, procedure_query: Dict[str, Any], return_objects: bool=True):

        body = ProcedureGETBody(data=procedure_query)
        r = self._request("get", "procedure", data=body.json())
        r = ProcedureGETReponse.parse_raw(r.text)

        if return_objects:
            ret = []
            for packet in r.data:
                tmp = build_procedure(packet, client=self)
                ret.append(tmp)
            return ret
        else:
            # Equivalent to full_return from other gets
            return r

    ### Compute section

    def add_compute(self,
                    program: str,
                    method: str,
                    basis: str,
                    driver: str,
                    keywords: Union[str, None],
                    molecule_id: Union[str, Molecule, List[Union[str, Molecule]]],
                    full_return: bool=False,
                    tag: str=None) -> Union[TaskQueuePOSTResponse, TaskQueuePOSTResponse.Data]:

        # Always a list
        if not isinstance(molecule_id, list):
            molecule_id = [molecule_id]

        payload = {
            "meta": {
                "procedure": "single",
                "driver": driver,
                "program": program,
                "method": method,
                "basis": basis,
                "keywords": keywords,
                "tag": tag,
            },
            "data": molecule_id
        }

        body = TaskQueuePOSTBody(**payload)

        r = self._request("post", "task_queue", data=body.json())
        r = TaskQueuePOSTResponse.parse_raw(r.text)

        if full_return:
            return r
        else:
            return r.data

    def add_procedure(self,
                      procedure: str,
                      program: str,
                      program_options: Dict[str, Any],
                      molecule_id: List[str],
                      full_return: bool=False):

        # Always a list
        if isinstance(molecule_id, str):
            molecule_id = [molecule_id]

        payload = {
            "meta": {
                "procedure": procedure,
                "program": program,
            },
            "data": molecule_id
        }
        payload["meta"].update(program_options)

        r = self._request("post", "task_queue", payload)
        r = TaskQueuePOSTResponse.parse_raw(r.text)

        if full_return:
            return r
        else:
            return r.data

    def check_tasks(self, query: Dict[str, Any], projection: Optional[Dict[str, Any]]=None, full_return: bool=False):
        """Checks the status of tasks in the Fractal queue.

        Parameters
        ----------
        query : dict
            A query to find tasks
        projection: dict, optional
            Projection of data to call from the database
        full_return : bool, optional
            Returns the full JSON return if True

        Returns
        -------
        list of dict
            A dictionary of each match that contains the current status
            and, if an error has occured, the error message.

        >>> client.check_tasks({"id": "5bd35af47b878715165f8225"})
        [{"status": "WAITING"}]
        """

        payload = {"meta": {"projection": projection}, "data": query}
        body = TaskQueueGETBody(**payload)

        r = self._request("get", "task_queue", data=body.json())
        r = TaskQueueGETResponse.parse_raw(r.text)

        if full_return:
            return r
        else:
            return r.data

    def add_service(self, service: Union[GridOptimizationInput, TorsionDriveInput], full_return: bool=False):

        body = ServiceQueuePOSTBody(meta={}, data=service)

        r = self._request("post", "service_queue", data=body.json())
        r = ServiceQueuePOSTResponse.parse_raw(r.text)

        if full_return:
            return r
        else:
            return r.data

    def check_services(self, query: Dict[str, Any], full_return: bool=False):
        """Checks the status of services in the Fractal queue.

        Parameters
        ----------
        query : dict
            A query to find services
        full_return : bool, optional
            Returns the full JSON return if True

        Returns
        -------
        list of dict
            A dictionary of each match that contains the current status
            and, if an error has occurred, the error message.

        >>> client.check_services({"id": "5bd35af47b878715165f8225"})
        [{"status": "RUNNING"}]
        """

        payload = {"meta": {}, "data": query}

        body = ServiceQueueGETBody(**payload)

        r = self._request("get", "service_queue", data=body.json())
        r = ServiceQueueGETResponse.parse_raw(r.text)

        if full_return:
            return r
        else:
            return r.data
