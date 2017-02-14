
"""Mongo QCDB Database object and helpers
"""

import numpy as np
import itertools as it
import math
import json
import copy

from . import molecule

def nCr(n, r):
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
        self.data["reactions"] = []

        if mongod is not None:
            raise KeyError("Database: Cannot yet intialize a database object from a Mongo server.")
        else:

            self.data["name"] = name
            self.data["provenence"] = {}

        # If we making a new database we may need new hashes and json objects
        self.new_molecule_hashes = []
        self.new_molecule_jsons = []

        self.rxn_name_list = []

    def parse_stoichiometry(self, stoichiometry):
        """
        Parses a stiochiometry list.

        Parameters
        ----------
        stoichiometry : list
            A list of tuples describing the stoichiometry.

        Returns
        -------

        Notes
        -----
        This function attempts to convert the molecule into its correspond hash. If a new molecule is present it will be
        be stored in preperation to send back to the primary database.


        Examples
        --------

        """

        ret = {}

        mol_hashes = []
        mol_values = []

        for line in stoichiometry:
            if len(line) != 2:
                raise KeyError("Database: Parse stoichiometry: passed in as a list must of key : value type")

            # Get the values
            try:
                mol_values.append(float(line[1]))
            except:
                raise TypeError("Database: Parse stoichiometry: second value must be convertable to a float.")

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
                raise TypeError("Database: Parse stoichiometry: first value must either be a molecule hash, a molecule str, or a Molecule class.")

            mol_hashes.append(molecule_hash)

        return {mol : coef for mol, coef in zip(mol_hashes, mol_values)}



    def add_rxn(self, name, stoichiometry, attributes={}, other_fields={}):
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
            raise KeyError("Database: Name '%s' already exists. Please either delete this entry or call the update function." % name)


        # Set stoich
        if isinstance(stoichiometry, dict):
            raise AttributeError("Database:add_rxn: Dict stoich yet implemented!")

        elif isinstance(stoichiometry, (tuple, list)):
            rxn["stoichiometry"] = {}
            rxn["stoichiometry"]["default"] = self.parse_stoichiometry(stoichiometry)
        else:
            raise TypeError("Database:add_rxn: Type of stoichiometry was not recognized '%s'", type(stoichiometry))

        # Set attributes
        if not isinstance(attributes, dict):
            raise TypeError("Database:add_rxn: attributes must be a dictionary, not '%s'", type(attributes))

        rxn["attributes"] = attributes

        if not isinstance(other_fields, dict):
            raise TypeError("Database:add_rxn: other_fields must be a dictionary, not '%s'", type(attributes))

        for k, v in other_fields.items():
            rxn[k] = v

        self.data["reactions"].append(rxn)

        return rxn


    def add_ie_rxn(self, name, molecule, attribute={}):
        raise AttributeError("Database:add_ie_rxn: Not yet implemented!")

    def to_json(self, filename=None):
        """
        If a filename is provided, dumps the file to disk. Otherwise returns a copy of the current data.
        """
        if filename:
            json.dumps(filename, self.data)

        else:
            return copy.deepcopy(self.data)


    def build_ie_fragments(self, do_cp=True, do_vmfc=False):

        ret = {}

        # Default nocp, everything in monomer basis
        ret["default"] = {}
        for nbody in nbody_range:
            for x in it.combinations(fragment_range, nbody):
                nocp_compute_list[nbody].add( (x, x) )


            for k in range(1, n + 1):
                take_nk =  nCr(max_frag - k - 1, n - k)
                sign = ((-1) ** (n - k))
                value = nocp_energy_by_level[k]
                nocp_energy_body_dict[n] += take_nk * sign * value

                if ptype != 'energy':
                    value = nocp_ptype_by_level[k]
                    nocp_ptype_body_dict[n] += take_nk * sign * value

        if do_cp:
            # Everything is in dimer basis
            basis_tuple = tuple(fragment_range)
            for nbody in nbody_range:
                for x in it.combinations(fragment_range, nbody):
                    cp_compute_list[nbody].add( (x, basis_tuple) )


        if do_vmfc:
            # Like a CP for all combinations of pairs or greater
            for nbody in nbody_range:
                for cp_combos in it.combinations(fragment_range, nbody):
                    basis_tuple = tuple(cp_combos)
                    for interior_nbody in nbody_range:
                        for x in it.combinations(cp_combos, interior_nbody):
                            combo_tuple = (x, basis_tuple)
                            vmfc_compute_list[interior_nbody].add( combo_tuple )
                            vmfc_level_list[len(basis_tuple)].add( combo_tuple )


