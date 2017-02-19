"""Mongo QCDB Database object and helpers
"""

import numpy as np
import itertools as it
import math
import json
import copy
import pandas as pd

from . import molecule


def _nCr(n, r):
    """
    Compute the binomial coefficient n! / (k! * (n-k)!)
    """
    return math.factorial(n) / math.factorial(r) / math.factorial(n - r)


class Database(object):
    """
    This is a Mongo QCDB database class.
    """

    def __init__(self, name, mongod=None):

        self.data = {}
        self.data["reactions"] = {}

        if mongod is not None:
            raise KeyError("Database: Cannot yet intialize a database object from a Mongo server.")
        else:

            self.df = pd.DataFrame()
            self.data["name"] = name
            self.data["provenence"] = {}

        # If we making a new database we may need new hashes and json objects
        self.new_molecule_hashes = []
        self.new_molecule_jsons = []

        self.rxn_name_list = []

    def get_index(self):
        """
        Returns the current index of the database.
        """
        return list(self.data["reactions"])

    def get_rxn(self, name):
        """
        Returns the JSON object of a specific reaction.
        """
        return self.data["reactions"][name]

    def parse_stoichiometry(self, stoichiometry):
        """
        Parses a stiochiometry list.

        Parameters
        ----------
        stoichiometry : list
            A list of tuples describing the stoichiometry.

        Returns
        -------
        stoich : list
            A list of formatted tuples describing the stoichiometry for use in a MongoDB.

        Notes
        -----
        This function attempts to convert the molecule into its correspond hash. The following will happen depending on the form of the Molecule.
            - Molecule hash - Used directly in the stoichiometry.
            - Molecule class - Hash is obtained and the molecule will be added to the databse upon saving.
            - Molecule string - Molecule will be converted to a Molecule class and the same process as the above will occur.


        Examples
        --------

        """

        ret = {}

        mol_hashes = []
        mol_values = []

        for line in stoichiometry:
            if len(line) != 2:
                raise KeyError(
                    "Database: Parse stoichiometry: passed in as a list must of key : value type")

            # Get the values
            try:
                mol_values.append(float(line[1]))
            except:
                raise TypeError(
                    "Database: Parse stoichiometry: second value must be convertable to a float.")

            # What kind of molecule is it?
            mol = line[0]

            # This is a molecule hash, should be in the database
            if isinstance(mol, str) and (len(mol) == 40):
                molecule_hash = mol

            elif isinstance(mol, str):
                qcdb_mol = molecule.Molecule(mol)

                molecule_hash = qcdb_mol.get_hash()

                if molecule_hash not in self.new_molecule_hashes:
                    self.new_molecule_hashes.append(molecule_hash)
                    self.new_molecule_jsons.append(qcdb_mol.to_json())

            elif isinstance(mol, molecule.Molecule):
                molecule_hash = mol.get_hash()

                if molecule_hash not in self.new_molecule_hashes:
                    self.new_molecule_hashes.append(molecule_hash)
                    self.new_molecule_jsons.append(mol.to_json())

            else:
                raise TypeError(
                    "Database: Parse stoichiometry: first value must either be a molecule hash, a molecule str, or a Molecule class."
                )

            mol_hashes.append(molecule_hash)

        # Sum together the coefficients of duplicates
        ret = {}
        for mol, coef in zip(mol_hashes, mol_values):
            if mol in list(ret):
                ret[mol] += coef
            else:
                ret[mol] = coef

        return ret

    def add_rxn(self, name, stoichiometry, return_values={}, attributes={}, other_fields={}):
        """
        Adds a reaction to a database object.

        Parameters
        ----------
        name : str
            Name of the reaction.
        stoichiometry : list or dict
            Either a list or dictionary of lists

        Notes
        -----

        Examples
        --------

        """
        rxn = {}

        # Set name
        rxn["name"] = name
        if name in self.rxn_name_list:
            raise KeyError(
                "Database: Name '%s' already exists. Please either delete this entry or call the update function."
                % name)

        # Set stoich
        if isinstance(stoichiometry, dict):
            rxn["stoichiometry"] = {}

            if "default" not in list(stoichiometry):
                raise KeyError("Database:add_rxn: Stoichiometry dict must have a 'default' key.")

            for k, v in stoichiometry.items():
                rxn["stoichiometry"][k] = self.parse_stoichiometry(v)

        elif isinstance(stoichiometry, (tuple, list)):
            rxn["stoichiometry"] = {}
            rxn["stoichiometry"]["default"] = self.parse_stoichiometry(stoichiometry)
        else:
            raise TypeError("Database:add_rxn: Type of stoichiometry input was not recognized '%s'",
                            type(stoichiometry))

        # Set attributes
        if not isinstance(attributes, dict):
            raise TypeError("Database:add_rxn: attributes must be a dictionary, not '%s'",
                            type(attributes))

        rxn["attributes"] = attributes

        if not isinstance(other_fields, dict):
            raise TypeError("Database:add_rxn: other_fields must be a dictionary, not '%s'",
                            type(attributes))

        for k, v in other_fields.items():
            rxn[k] = v

        self.data["reactions"][name] = rxn

        return rxn

    def add_ie_rxn(self, name, mol, return_values={}, attributes={}, other_fields={}):

        stoichiometry = self.build_ie_fragments(mol)
        return self.add_rxn(
            name,
            stoichiometry,
            return_values=return_values,
            attributes=attributes,
            other_fields=other_fields)

    def to_json(self, filename=None):
        """
        If a filename is provided, dumps the file to disk. Otherwise returns a copy of the current data.
        """
        if filename:
            json.dumps(filename, self.data)

        else:
            return copy.deepcopy(self.data)

    def build_ie_fragments(self, mol, do_default=True, do_cp=True, do_vmfc=False, max_nbody=0):
        """
        Build the stoichiometry for an Interaction Energy.

        Parameters
        ----------
        mol : Molecule class or str
            Molecule to fragment.
        do_default : bool
            Create the default (noCP) stoichiometry.
        do_cp : bool
            Create the counterpoise (CP) corrected stoichiometry.
        do_vmfc : bool
            Create the Valiron-Mayer Function Counterpoise (VMFC) corrected stoichiometry.
        max_nbody : int
            What is the maximum fragment level built, if zero defaults to the maximum number of fragments.

        Notes
        -----

        Examples
        --------

        """

        if isinstance(mol, str):
            mol = molecule.Molecule(mol)

        ret = {}

        max_frag = len(mol.fragments)
        if max_nbody == 0:
            max_nbody = max_frag

        if max_nbody < 2:
            raise AttributeError(
                "Database:build_ie_fragments: Molecule must have at least two fragments.")

        # Build some info
        fragment_range = list(range(max_frag))

        nocp_dict = {}
        cp_dict = {}

        # Loop over the bodis
        for nbody in range(1, max_nbody):
            nocp_tmp = []
            cp_tmp = []
            vmfc_tmp = []
            for k in range(1, nbody + 1):
                take_nk = _nCr(max_frag - k - 1, nbody - k)
                sign = ((-1)**(nbody - k))
                coef = take_nk * sign
                for frag in it.combinations(fragment_range, k):
                    if do_default:
                        nocp_tmp.append((mol.get_fragment(frag), coef))
                    if do_cp:
                        ghost = list(set(fragment_range) - set(frag))
                        cp_tmp.append((mol.get_fragment(frag, ghost), coef))

            if do_default:
                ret["default" + str(nbody)] = nocp_tmp

            if do_cp:
                ret["cp" + str(nbody)] = cp_tmp

        # Add in the maximal position
        if do_default:
            ret["default"] = [(mol, 1.0)]

        if do_cp:
            ret["cp"] = [(mol, 1.0)]

        return ret
