"""Provides an interface the QCDB Server instance"""

import requests

from . import molecule
from . import orm


class FractalClient(object):
    def __init__(self, port, username=None, password=None, shared_secret=None):
        if "http" not in port:
            port = "http://" + port

        if not port.endswith("/"):
            port += "/"

        self.port = port
        self._api_key = (username, password)

        self._mol_addr = self.port + "molecule"
        self._option_addr = self.port + "option"
        self._database_addr = self.port + "database"
        self._result_addr = self.port + "result"
        self._service_addr = self.port + "service"
        self._scheduler_addr = self.port + "scheduler"
        self._service_scheduler_addr = self.port + "service_scheduler"
        # self.info = self.get_information()

    ### Molecule section

    def get_molecules(self, mol_list, index="id"):

        # Can take in either molecule or lists
        if not isinstance(mol_list, (tuple, list)):
            mol_list = [mol_list]

        payload = {"meta": {"index": index}, "data": mol_list}
        r = requests.get(self._mol_addr, json=payload, auth=self._api_key)
        if r.status_code != 200:
            raise requests.exceptions.HTTPError("Failed query. Reason: {}".format(r.reason))

        return r.json()["data"]

    def add_molecules(self, mol_list, full_return=False):

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

        r = requests.post(self._mol_addr, json=payload, auth=self._api_key)
        if r.status_code != 200:
            raise requests.exceptions.HTTPError("Failed query. Reason: {}".format(r.reason))

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
        r = requests.get(self._option_addr, json=payload)
        assert r.status_code == 200

        return r.json()["data"]

    def add_options(self, opt_list, full_return=False):

        # Can take in either molecule or lists

        payload = {"meta": {}, "data": opt_list}

        r = requests.post(self._option_addr, json=payload)
        assert r.status_code == 200

        if full_return:
            return r.json()
        else:
            return r.json()["data"]

    ### Database section

    def get_databases(self, db_list):

        payload = {"meta": {}, "data": db_list}
        r = requests.get(self._database_addr, json=payload)
        assert r.status_code == 200

        return r.json()["data"]

    def add_database(self, db, full_return=False):

        # Can take in either molecule or lists

        payload = {"meta": {}, "data": db}

        r = requests.post(self._database_addr, json=payload)
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

        r = requests.get(self._result_addr, json=payload)
        assert r.status_code == 200

        if kwargs.get("return_full", False):
            return r.json()
        else:
            return r.json()["data"]

    def get_service(self, service_id, **kwargs):

        payload = {"meta": {}, "data": [service_id]}

        r = requests.get(self._service_addr, json=payload)
        assert r.status_code == 200

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

        r = requests.post(self._scheduler_addr, json=payload)
        assert r.status_code == 200

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

        r = requests.post(self._service_scheduler_addr, json=payload)
        assert r.status_code == 200

        if return_full:
            return r.json()
        else:
            return r.json()["data"]

    # Def add_service
