import numpy as np
import os
import re

from . import constants


class Molecule(object):
    """
    This is a Mongo QCDB molecule class.
    """

    def __init__(self, mol_str, dtype="psi4", orient=True, name=""):
        """
        the Mongo QCDB molecule class which is capable of reading and writing many formats.
        """

        self.symbols = []
        self.masses = []
        self.name = name
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
            if dtype == "psi4":
                self._molecule_from_string_psi4(mol_str)
            else:
                raise KeyError("Molecule: dtype of %s not recognized.")

            if orient:
                self.orient_molecule()
        else:
            # In case a user wants to build one themselves
            pass

    def _molecule_from_string_psi4(self, text):
        """Given a string *text* of psi4-style geometry specification
        (including newlines to separate lines), builds a new molecule.
        Called from constructor.

        """

        # Setup re expressions
        comment = re.compile(r'^\s*#')
        blank = re.compile(r'^\s*$')
        bohr = re.compile(r'^\s*units?[\s=]+(bohr|au|a.u.)\s*$', re.IGNORECASE)
        ang = re.compile(r'^\s*units?[\s=]+(ang|angstrom)\s*$', re.IGNORECASE)
        atom = re.compile(
            r'^(?:(?P<gh1>@)|(?P<gh2>Gh\())?(?P<label>(?P<symbol>[A-Z]{1,3})(?:(_\w+)|(\d+))?)(?(gh2)\))(?:@(?P<mass>\d+\.\d+))?$',
            re.IGNORECASE)
        cgmp = re.compile(r'^\s*(-?\d+)\s+(\d+)\s*$')
        frag = re.compile(r'^\s*--\s*$')
        ghost = re.compile(r'@(.*)|Gh\((.*)\)', re.IGNORECASE)
        realNumber = re.compile(r"""[-+]?(?:(?:\d*\.\d+)|(?:\d+\.?))(?:[Ee][+-]?\d+)?""",
                                re.VERBOSE)

        lines = re.split('\n', text)
        glines = []
        ifrag = 0

        # Assume angstrom, we want bohr
        unit_conversion = 1 / constants.physconst["bohr2angstroms"]

        for line in lines:

            # handle comments
            if comment.match(line) or blank.match(line):
                pass

            # handle units
            elif bohr.match(line):
                unit_conversion = 1.0

            # handle charge and multiplicity
            elif cgmp.match(line):
                tempCharge = int(cgmp.match(line).group(1))
                tempMultiplicity = int(cgmp.match(line).group(2))

                if ifrag == 0:
                    self.charge = tempCharge
                    self.multiplicity = tempMultiplicity
                self.fragment_charges.append(tempCharge)
                self.fragment_multiplicities.append(tempMultiplicity)

            # handle fragment markers and default fragment cgmp
            elif frag.match(line):
                try:
                    self.fragment_charges[ifrag]
                except:
                    self.fragment_charges.append(0)
                    self.fragment_multiplicities.append(1)
                ifrag += 1
                glines.append(line)

            elif atom.match(line.split()[0].strip()):
                glines.append(line)
            else:
                raise ValidationError(
                    'Molecule::create_molecule_from_string: Unidentifiable line in geometry specification: %s'
                    % (line))

        # catch last default fragment cgmp
        try:
            self.fragment_charges[ifrag]
        except:
            self.fragment_charges.append(0)
            self.fragment_multiplicities.append(1)

        # Now go through the rest of the lines looking for fragment markers
        ifrag = 0
        iatom = 0
        tempfrag = []
        atomSym = ""
        atomLabel = ""
        self.geometry = []

        # handle number values

        for line in glines:

            # handle fragment markers
            if frag.match(line):
                ifrag += 1
                self.fragments.append(list(range(tempfrag[0], tempfrag[-1] + 1)))
                self.real.extend([True for x in range(tempfrag[0], tempfrag[-1] + 1)])
                tempfrag = []

            # handle atom markers
            else:
                entries = re.split(r'\s+|\s*,\s*', line.strip())
                atomm = atom.match(line.split()[0].strip().upper())
                atomLabel = atomm.group('label')
                atomSym = atomm.group('symbol')

                # We don't know whether the @C or Gh(C) notation matched. Do a quick check.
                ghostAtom = False if (atomm.group('gh1') is None and
                                      atomm.group('gh2') is None) else True

                # Check that the atom symbol is valid
                if not atomSym in constants.el2z:
                    raise ValidationError(
                        'Molecule::create_molecule_from_string: Illegal atom symbol in geometry specification: %s'
                        % (atomSym))

                self.symbols.append(atomSym)
                zVal = constants.el2z[atomSym]
                atomMass = constants.el2masses[atomSym] if atomm.group('mass') is None else float(
                    atomm.group('mass'))
                self.masses.append(atomMass)

                charge = float(zVal)
                if ghostAtom:
                    zVal = 0
                    charge = 0.0

                # handle cartesians
                if len(entries) == 4:
                    tempfrag.append(iatom)
                    if realNumber.match(entries[1]):
                        xval = float(entries[1])
                    else:
                        raise ValidationError(
                            "Molecule::create_molecule_from_string: Unidentifiable entry %s.",
                            entries[1])

                    if realNumber.match(entries[2]):
                        yval = float(entries[2])
                    else:
                        raise ValidationError(
                            "Molecule::create_molecule_from_string: Unidentifiable entry %s.",
                            entries[2])

                    if realNumber.match(entries[3]):
                        zval = float(entries[3])
                    else:
                        raise ValidationError(
                            "Molecule::create_molecule_from_string: Unidentifiable entry %s.",
                            entries[3])

                    self.geometry.append([xval, yval, zval])
                else:
                    raise ValidationError(
                        'Molecule::create_molecule_from_string: Illegal geometry specification line : %s. \
                        You should provide either Z-Matrix or Cartesian input' % (line))

                iatom += 1

        self.geometry = np.array(self.geometry) * unit_conversion
        self.fragments.append(list(range(tempfrag[0], tempfrag[-1] + 1)))
        self.real.extend([True for x in range(tempfrag[0], tempfrag[-1] + 1)])


    def pretty_print(self):
        """Print the molecule in Angstroms. Same as :py:func:`print_out` only always in Angstroms.
        (method name in libmints is print_in_angstrom)

        """
        text = ""

        text += """    Geometry (in %s), charge = %d, multiplicity = %d:\n\n""" % \
            ('Angstrom', self.charge, self.multiplicity)
        text += """       Center              X                  Y                   Z       \n"""
        text += """    ------------   -----------------  -----------------  -----------------\n"""

        for i in range(len(self.geometry)):
            text += """    %8s%4s """ % (self.symbols[i], "" if self.real[i] else "(Gh)")
            for j in range(3):
                text += """  %17.12f""" % (self.geometry[i][j] *
                                           constants.physconst["bohr2angstroms"])
            text += "\n"
        text += "\n"

        return text


    def __repr__(self):
        return self.pretty_print()


    def orient_molecule(self):
        """
        Centers the molecule and orients via inertia tensor.
        """

        np_mass = np.array(self.masses)

        # Center on Mass
        self.geometry += np.average(self.geometry, axis=0, weights=np_mass)

        # Build inertia tensor
        tensor = np.zeros((3, 3))


        # Diagonal
        tensor[0][0] = np.sum(np_mass * (self.geometry[:, 1] * self.geometry[:, 1] + self.geometry[:, 2] * self.geometry[:, 2]))
        tensor[1][1] = np.sum(np_mass * (self.geometry[:, 0] * self.geometry[:, 0] + self.geometry[:, 2] * self.geometry[:, 2]))
        tensor[2][2] = np.sum(np_mass * (self.geometry[:, 0] * self.geometry[:, 0] + self.geometry[:, 1] * self.geometry[:, 1]))

        # I(alpha, beta)
        # Off diagonal
        tensor[0][1] = np.sum(np_mass * self.geometry[:, 0] * self.geometry[:, 1])
        tensor[0][2] = np.sum(np_mass * self.geometry[:, 0] * self.geometry[:, 2])
        tensor[1][2] = np.sum(np_mass * self.geometry[:, 1] * self.geometry[:, 2])

        # Rotate into inertial frame
        evals, evecs = np.linalg.eigh(tensor)
        self.geometry = np.dot(self.geometry, evecs)



