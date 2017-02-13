import numpy as np
import os


class Molecule(object):
    """
    This is a Mongo QCDB molecule class.
    """

    def __init__(self, mol_str=None, dtype="psi4"):

        self.symbols = []
        self.masses = []
        self.name = ""
        self.charge = 0.0
        self.multiplicity = 1
        self.real = []
        self.comment = ""
        self.geometry = None
        self.fragments = []
        self.fragment_charges = []
        self.fragment_multiplicities = []
        self.provenance = {}

        if mol_str:
            self.from_str(mol_str)
