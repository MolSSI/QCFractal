"""Provides an interface the QCDB Server instance"""

import json
import os
from collections import defaultdict

import requests
import yaml

from . import molecule
from . import orm
from .collections import collection_factory


class FractalClient(object):
    def __init__(self, address, username=None, password=None, verify=True):
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

        # Try to connect and pull general data
        self.server_info = self._request("get", "information", {}).json()

        self.server_name = self.server_info["name"]

    def __str__(self):
        """A short short representation of the current FractalClient.

        Returns
        -------
        str
            The desired representation.
        """
        ret = "FractalClient(server_name='{}', address='{}', username='{}')".format(
            self.server_name, self.address, self.username)
        return ret

    def _request(self, method, service, payload, noraise=False):

        addr = self.address + service
        try:
            if method == "get":
                r = requests.get(addr, json=payload, headers=self._headers, verify=self._verify)
            elif method == "post":
                r = requests.post(addr, json=payload, headers=self._headers, verify=self._verify)
            elif method == "put":
                r = requests.put(addr, json=payload, headers=self._headers, verify=self._verify)
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
    def from_file(cls, load_path=None):
        """Creates a new FractalClient from file. If no path is passed in searches
        current working directory and ~.qca/ for "qcportal_config.yaml"

        Parameters
        ----------
        load_path : str, dict, optional
            Path to find "qcportal_config.yaml", the filename, or a dictionary containing keys
            ["address", "username", "password", "verify"]

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

    def server_information(self):
        return json.loads(json.dumps(self.server_info))

    ### Molecule section

    def get_molecules(self, mol_list, index="id", full_return=False):
        """Get molecules from the Server.

        Parameters
        ----------
        mol_list : list of str
            Either molecule Id's or molecule hashes to query.
        index : str, ("id", "hash")
            The index to search on
        full_return : bool, optional
            Flags to return all metadata or only the query.

        Returns
        -------
        list of molecule JSON
            Returns all found molecules.
        """
        # Can take in either molecule or lists
        if not isinstance(mol_list, (tuple, list)):
            mol_list = [mol_list]

        index = index.lower()
        if index not in ["id", "index", "molecular_formula"]:
            raise KeyError("Search index must either be 'id' or hash, found: {}".format(index))

        payload = {"meta": {"index": index}, "data": mol_list}
        r = self._request("get", "molecule", payload)

        if full_return:
            return r.json()
        else:
            return r.json()["data"]

    def add_molecules(self, mol_list, full_return=False):
        """Adds molecules to the Server

        Parameters
        ----------
        mol_list : dict
            A (key: molecule) dictionary for the molecules to be added. The molecules can either be a
            Molecule class or a JSON Molecule representation.
        full_return : bool, optional
            Flags to return all metadata or only the submitted ids.

        Returns
        -------
        dict
            A (key: molecule id) dictionary of added molecules.

        """
        # Can take in either molecule or lists

        mol_submission = {}
        for key, mol in mol_list.items():
            if isinstance(mol, molecule.Molecule):
                mol_submission[key] = mol.to_json()
            elif isinstance(mol, dict):
                mol_submission[key] = mol
            else:
                raise TypeError("Input molecule type '{}' not recognized".format(type(mol)))

        payload = {"meta": {}, "data": mol_submission}
        r = self._request("post", "molecule", payload)

        if full_return:
            return r.json()
        else:
            return r.json()["data"]

    ### Options section

    def get_options(self, opt_list):

        # Logic to figure out if we are doing single/multiple pulling.
        # Need to fix later
        # if not isinstance(opt_list, (tuple, list)):
        #     opt_list = [opt_list]

        payload = {"meta": {}, "data": opt_list}
        r = self._request("get", "option", payload)

        return r.json()["data"]

    def add_options(self, opt_list, full_return=False):

        # Can take in either molecule or lists

        payload = {"meta": {}, "data": opt_list}
        r = self._request("post", "option", payload)

        if full_return:
            return r.json()
        else:
            return r.json()["data"]

    ### Collections section

    def list_collections(self, collection_type=None):
        """Lists the available collections currently on the server.

        Parameters
        ----------
        collection_type : None, optional
            If `None` all collection types will be returned, otherwise only the
            specified collection type will be returned

        Returns
        -------
        dict
            A dictionary containing the available collection types.
        """

        query = {}
        if collection_type is not None:
            query = {"collection": collection_type.lower()}

        payload = {"meta": {"projection": {"name": True, "collection": True}}, "data": query}
        r = self._request("get", "collection", payload)

        if collection_type is None:
            ret = defaultdict(list)
            for entry in r.json()["data"]:
                ret[entry["collection"]].append(entry["name"])
            return dict(ret)
        else:
            return [x["name"] for x in r.json()["data"]]

    def get_collection(self, collection_type, collection_name, full_return=False):
        """Aquires a given collection from the server

        Parameters
        ----------
        collection_type : str
            The collection type to be accessed
        collection_name : str
            The name of the collection to be accssed
        full_return : bool, optional
            If False, returns a Collection object otherwise returns raw JSON

        Returns
        -------
        Collection
            A Collection object if the given collection was found otherwise returns `None`.
        """

        payload = {"meta": {}, "data": {"collection": collection_type.lower(), "name": collection_name}}
        r = self._request("get", "collection", payload)

        if full_return:
            return r.json()
        else:
            # If nothing found
            if len(r.json()["data"]):
                return collection_factory(r.json()["data"][0], client=self)
            else:
                raise KeyError("Collection '{}:{}' not found.".format(collection_type, collection_name))

    def add_collection(self, collection, overwrite=False, full_return=False):

        # Can take in either molecule or lists

        if overwrite and ("id" not in collection):
            raise KeyError("Attempting to overwrite collection, but no server ID found.")

        payload = {"meta": {"overwrite": overwrite}, "data": collection}

        r = self._request("post", "collection", payload)
        assert r.status_code == 200

        if full_return:
            return r.json()
        else:
            return r.json()["data"]

    ### Results section

    def get_results(self, **kwargs):

        keys = ["program", "molecule", "driver", "method", "basis", "options", "hash_index", "id", "status"]
        query = {}
        for key in keys:
            if key in kwargs:
                query[key] = kwargs[key]

        payload = {"meta": {}, "data": query}
        if "projection" in kwargs:
            payload["meta"]["projection"] = kwargs["projection"]

        r = self._request("get", "result", payload)

        if kwargs.get("return_full", False):
            return r.json()
        else:
            return r.json()["data"]

    def get_procedures(self, procedure_id, return_objects=True):

        payload = {"meta": {}, "data": procedure_id}
        r = self._request("get", "procedure", payload)

        if return_objects:
            ret = []
            for packet in r.json()["data"]:
                tmp = orm.build_orm(packet, client=self)
                ret.append(tmp)
            return ret
        else:
            return r.json()

    # Must compute results?
    # def add_results(self, db, full_return=False):

    #     # Can take in either molecule or lists

    #     payload = {"meta": {}, "data": {}}
    #     payload["data"] = db

    #     r = requests.post(self._result_addr, json=payload)
    #     assert r.status_code == 200

    #     if full_return:
    #         return r.json()
    #     else:
    #         return r.json()["data"]

    ### Compute section

    def add_compute(self, program, method, basis, driver, options, molecule_id, return_full=False, tag=None):

        # Always a list
        if isinstance(molecule_id, str):
            molecule_id = [molecule_id]

        payload = {
            "meta": {
                "procedure": "single",
                "driver": driver,
                "program": program,
                "method": method,
                "basis": basis,
                "options": options,
                "tag": tag,
            },
            "data": molecule_id
        }

        r = self._request("post", "task_queue", payload)

        if return_full:
            return r.json()
        else:
            return r.json()["data"]

    def add_procedure(self, procedure, program, program_options, molecule_id, return_full=False):

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

        if return_full:
            return r.json()
        else:
            return r.json()["data"]

    def check_tasks(self, query, projection=None, return_full=False):
        """Checks the status of tasks in the Fractal queue.

        Parameters
        ----------
        query : dict
            A query to find tasks
        return_full : bool, optional
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

        r = self._request("get", "task_queue", payload)

        if return_full:
            return r.json()
        else:
            return r.json()["data"]

    def add_service(self, service, data, options, return_full=False):

        # Always a list
        if isinstance(data, str):
            data = [data]

        payload = {
            "meta": {
                "service": service,
            },
            "data": data
        }
        payload["meta"].update(options)

        r = self._request("post", "service_queue", payload)

        if return_full:
            return r.json()
        else:
            return r.json()["data"]

    def check_services(self, query, return_full=False):
        """Checks the status of services in the Fractal queue.

        Parameters
        ----------
        query : dict
            A query to find services
        return_full : bool, optional
            Returns the full JSON return if True

        Returns
        -------
        list of dict
            A dictionary of each match that contains the current status
            and, if an error has occured, the error message.

        >>> client.check_services({"id": "5bd35af47b878715165f8225"})
        [{"status": "RUNNING"}]
        """

        payload = {"meta": {}, "data": query}

        r = self._request("get", "service_queue", payload)

        if return_full:
            return r.json()
        else:
            return r.json()["data"]
