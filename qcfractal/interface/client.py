"""Provides an interface the QCDB Server instance"""

import cryptography.fernet
import json
import requests
import yaml
import base64

from . import molecule
from . import orm


class FractalClient(object):
    def __init__(self, address, username=None, password=None, shared_secret=None):
        """Constructs a FractalClient

        Parameters
        ----------
        address : str
            The address of the FractalServer to connect to.
        username : str, optional
            The username to authenticate the connection with.
        password : str, optional
            The password to authenticate the connection with.
        shared_secret : str, optional
            The shared secret to use to encrypt the username and password.
        """
        if "http" not in address:
            address = "http://" + address

        if not address.endswith("/"):
            address += "/"

        self.address = address
        self.username = username

        # Default shared secret, not really better than in the clear for non-SSL communication

        if shared_secret:
            shared_secret = base64.urlsafe_b64encode((shared_secret + " " * (32 - len(shared_secret))).encode("UTF-8"))

            f = cryptography.fernet.Fernet(shared_secret)
            key = f.encrypt(json.dumps({"username": username, "password": password}).encode("UTF-8"))
            self._auth = {"Authorization": key}
        else:
            self._auth = {}

    def __str__(self):
        """A short short representation of the current FractalClient.

        Returns
        -------
        str
            The desired representation.
        """
        ret = "FractalClient("
        ret += "server='{}', ".format(self.address)
        ret += "username='{}')".format(self.username)
        return ret

        # self.info = self.get_information()
    def _request(self, method, service, payload):

        addr = self.address + service
        if method == "get":
            r = requests.get(addr, json=payload, headers=self._auth)
        elif method == "post":
            r = requests.post(addr, json=payload, headers=self._auth)
        else:
            raise KeyError("Method not understood: {}".format(method))

        if r.status_code != 200:
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
            ["address", "username", "password", "shared_secret"]

        """

        # Search canonical paths
        if load_path is None:
            test_paths = [os.getcwd(), os.path.join(os.path.expanduser('~'), ".qca")]

            for path in test_paths:
                local_path = os.path.join(path, "qcportal_config.yaml")
                if os.path.exists(path):
                    load_path = local_path
                    break

            raise FileNotFoundError(
                "Could not find `qcportal_config.yaml` in the following paths:\n    {}".format(", ".join(test_paths)))

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
        shared_secret = data.get("shared_secret", None)

        return cls(address, username=username, password=password, shared_secret=shared_secret)

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
        if index not in ["id", "index"]:
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

    ### Database section

    def get_databases(self, db_list):

        payload = {"meta": {}, "data": db_list}
        r = self._request("get", "database", payload)

        return r.json()["data"]

    def add_database(self, db, overwrite=False, full_return=False):

        # Can take in either molecule or lists

        if overwrite and ("id" not in db):
            raise KeyError("Attempting to overwrite database, but no server ID found.")

        payload = {"meta": {"overwrite": overwrite}, "data": db}

        r = self._request("post", "database", payload)
        assert r.status_code == 200

        if full_return:
            return r.json()
        else:
            return r.json()["data"]

    ### Results section

    def get_results(self, **kwargs):

        query = {}
        for key in ["program", "molecule_id", "driver", "method", "basis", "options"]:
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

    def get_service(self, service_id, **kwargs):

        payload = {"meta": {}, "data": [service_id]}
        r = self._request("get", "service", payload)

        if kwargs.get("return_objects", True):
            ret = []
            for packet in r.json()["data"]:
                tmp = orm.build_orm(packet)
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

    def add_compute(self, program, method, basis, driver, options, molecule_id, return_full=False, procedure="single"):

        # Always a list
        if isinstance(molecule_id, str):
            molecule_id = [molecule_id]

        payload = {
            "meta": {
                "procedure": procedure,
                "driver": driver,
                "program": program,
                "method": method,
                "basis": basis,
                "options": options
            },
            "data": molecule_id
        }

        r = self._request("post", "scheduler", payload)

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

        r = self._request("post", "service_scheduler", payload)

        if return_full:
            return r.json()
        else:
            return r.json()["data"]

    # Def add_service
