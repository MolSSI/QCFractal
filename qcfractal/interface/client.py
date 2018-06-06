"""Provides an interface the QCDB Server instance"""

import json
import requests
import pandas as pd

from . import molecule


class QCPortal(object):
    def __init__(self, port, username="", password=""):
        if "http" not in port:
            port = "http://" + port

        if not port.endswith("/"):
            port += "/"

        self.port = port
        # self.http_header = {"project": self.project, "username": username, "password": password}

        self._mol_addr = self.port + "molecule"
        self._option_addr = self.port + "option"
        self._database_addr = self.port + "database"
        self._result_addr = self.port + "result"
        # self.info = self.get_information()

    ### Molecule section

    def get_molecules(self, mol_list, index="id"):

        # Can take in either molecule or lists
        if not isinstance(mol_list, (tuple, list)):
            mol_list = [mol_list]

        payload = {"meta": {}, "data": {}}
        payload["data"] = {"ids": mol_list, "index": index}
        r = requests.get(self._mol_addr, json=payload)
        assert r.status_code == 200

        return r.json()["data"]

    def add_molecules(self, mol_list, full_return=False):

        # Can take in either molecule or lists

        mol_submission = {}
        for key, mol in mol_list.items():
            if isinstance(mol, molecule.Molecule):
                mol = mol.to_json()
            elif isinstance(mol, dict):
                mol = mol
            else:
                raise TypeError("Input molecule type '{}' not recognized".format(type(mol)))

            mol_submission[key] = mol

        payload = {"meta": {}, "data": {}}
        payload["data"] = mol_submission

        r = requests.post(self._mol_addr, json=payload)
        assert r.status_code == 200

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

        payload = {"meta": {}, "data": {}}
        payload["data"] = opt_list
        r = requests.get(self._option_addr, json=payload)
        assert r.status_code == 200

        return r.json()["data"]

    def add_options(self, opt_list, full_return=False):

        # Can take in either molecule or lists

        payload = {"meta": {}, "data": {}}
        payload["data"] = opt_list

        r = requests.post(self._option_addr, json=payload)
        assert r.status_code == 200

        if full_return:
            return r.json()
        else:
            return r.json()["data"]

    ### Database section

    def get_databases(self, db_list):

        payload = {"meta": {}, "data": {}}
        payload["data"] = db_list
        r = requests.get(self._database_addr, json=payload)
        assert r.status_code == 200

        return r.json()["data"]

    def add_database(self, db, full_return=False):

        # Can take in either molecule or lists

        payload = {"meta": {}, "data": {}}
        payload["data"] = db

        r = requests.post(self._database_addr, json=payload)
        assert r.status_code == 200

        if full_return:
            return r.json()
        else:
            return r.json()["data"]


