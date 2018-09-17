"""
DQM Molecule object and helpers
"""

import hashlib
import json
import os
import re
import collections

import numpy as np

from . import constants
from . import hash_helpers
from . import schema

# Rounding quantities for hashing
GEOMETRY_NOISE = 8
MASS_NOISE = 6
CHARGE_NOISE = 4


class Molecule:
    """
    This is a Mongo QCDB molecule class.
    """

    def __init__(self, mol_str, **kwargs):
        """
        __init__(self, mol_str, name="", dtype=None, orient=False)

        A molecule class which supports many different IO formats, provides unique molecule hashes, and
        other molecular maniuplations.

        Parameters
        ----------
        mol_str : str, dict
            A string or dictionary representation of the molecule.
        name : {"", str}, optional
            The name of the molecule, defaults to "".
        dtype : {None, "numpy", "json", "psi4"}, optional
            Determines how the input is parsed.
        orient : bool, optional
            Orientate the molecule to a standard frame or not.

        """

        # Layout all known attributes
        self._symbols = []
        self._geometry = None

        self._masses = []
        self.name = kwargs.pop("name", "")
        self.comment = ""
        self.charge = 0.0
        self.multiplicity = 1
        self.real = []
        self.fragments = []
        self.fragment_charges = []
        self.fragment_multiplicities = []
        self.provenance = {}
        self.connectivity = []
        self.identifiers = {}

        # List any flags
        self._custom_masses = False
        self._fix_com = True
        self._fix_orientation = True

        # Figure out how and if we will parse the Molecule adata
        if mol_str is not None:
            dtype = kwargs.pop("dtype", "none").lower()
            if dtype == "none":
                if isinstance(mol_str, str):
                    dtype = "psi4"
                elif isinstance(mol_str, dict):
                    dtype = "json"
                else:
                    raise TypeError("Unable to infer dtype.")

            if dtype == "psi4":
                self._molecule_from_string_psi4(mol_str)
            elif dtype == "numpy":
                frags = kwargs.pop("frags", [])
                self._molecule_from_numpy(mol_str, frags, units=kwargs.pop("units", "angstrom"))
            elif dtype == "json":
                self._molecule_from_json(mol_str)
            else:
                raise KeyError("Molecule: dtype of {} not recognized.".format(dtype))

            self.geometry = hash_helpers.float_prep(self.geometry, GEOMETRY_NOISE)
            if kwargs.pop("orient", False):
                self.orient_molecule()
                self.geometry = hash_helpers.float_prep(self.geometry, GEOMETRY_NOISE)

            # Cleanup un-initialized variables
            if len(self.real) == 0:
                self.real = [True for _ in range(self._geometry.shape[0])]

            if not self.fragments:
                natoms = self._geometry.shape[0]
                self.fragments = [list(range(natoms))]
                self.fragment_charges = [self.charge]
                self.fragment_multiplicities = [self.multiplicity]
            else:
                if not self.fragment_charges:
                    if np.isclose(self.charge, 0.0):
                        self.fragment_charges = [0 for _ in self.fragments]
                    else:
                        raise KeyError("Fragments passed in, but not fragment charges for a charged molecule.")

                if not self.fragment_multiplicities:
                    if self.multiplicity == 1:
                        self.fragment_multiplicities = [1 for _ in self.fragments]
                    else:
                        raise KeyError("Fragments passed in, but not fragment multiplicities for a non-singlet molecule.")


            # Validate
            self.validate()
        else:
            # In case a user wants to build one themselves
            pass

        if len(kwargs):
            raise KeyError("Not all kwargs were correctly parsed, remaining: {}".format(", ".join(kwargs.keys())))

### Any needed setters and getters

    @property
    def symbols(self):
        return self._symbols

    @symbols.setter
    def symbols(self, value):
        self._symbols = value

    @property
    def geometry(self):
        return self._geometry

    @geometry.setter
    def geometry(self, value):
        self._geometry = np.array(value).reshape(-1, 3)

    @property
    def masses(self):
        return self._masses

    @masses.setter
    def masses(self, value):
        self._custom_masses = True
        self._masses = value

### Classmethods

    @classmethod
    def from_file(cls, filename, dtype=None, orient=False):
        """
        Constructs a molecule object from a file.

        Parameters
        ----------
        filename : str
            The filename to build
        dtype : {None, "psi4", "numpy", "json"}, optional
            The type of file to interpret.
        orient: bool, optional
            Orientates the molecule to a standard frame or not.

        Returns
        -------
        Molecule
            A constructed molecule class.
        """

        ext = os.path.splitext(filename)[1]

        if dtype is None:
            if ext in [".psimol"]:
                dtype = "psi4"
            elif ext in [".npy"]:
                dtype = "numpy"
            elif ext in [".json"]:
                dtype = "json"
            else:
                raise KeyError("No dtype provided and ext '{}' not understood.".format(ext))

        if dtype == "psi4":
            with open(filename, "r") as infile:
                data = infile.read()
        elif dtype == "numpy":
            data = np.load(filename)
        elif dtype == "json":
            with open(filename, "r") as infile:
                data = json.load(infile)
        else:
            raise KeyError("Dtype not understood '{}'.".format(dtype))

        return cls(data, dtype=dtype, orient=orient)

### Parsers

    def _molecule_from_json(self, json_data):
        """
        From a given valid JSON molecule spec, rebuild the class.
        """

        for field, data in json_data.items():
            if field == "geometry":
                setattr(self, field, np.array(data, dtype=np.double))
            else:
                setattr(self, field, data)

    def _molecule_from_numpy(self, arr, frags, units="angstrom"):
        """
        Given a NumPy array of shape (N, 4) where each row is (Z_nuclear, X, Y, Z).

        Frags represents the splitting pattern for molecular fragments. Geometry must be in
        Angstroms.
        """

        arr = np.array(arr, dtype=np.double)

        if (len(arr.shape) != 2) or (arr.shape[1] != 4):
            raise AttributeError("Molecule: Molecule should be shape (N, 4) not {}.".format(arr.shape))

        if units == "bohr":
            const = 1
        elif units == "angstrom":
            const = 1 / constants.physconst["bohr2angstroms"]
        else:
            raise KeyError("Unit '{}' not understood".format(units))

        self.geometry = arr[:, 1:].copy() * const
        self.real = [True for x in arr[:, 0]]
        self.symbols = [constants.z2el[x] for x in arr[:, 0]]

        if len(frags) and (frags[-1] != arr.shape[0]):
            frags.append(arr.shape[0])

        start = 0
        for fsplit in frags:
            self.fragments.append(list(range(start, fsplit)))
            self.fragment_charges.append(0.0)
            self.fragment_multiplicities.append(1)
            start = fsplit

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
        # ghost = re.compile(r'@(.*)|Gh\((.*)\)', re.IGNORECASE)
        realNumber = re.compile(r"""[-+]?(?:(?:\d*\.\d+)|(?:\d+\.?))(?:[Ee][+-]?\d+)?""", re.VERBOSE)

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

            elif ang.match(line):
                pass

            # Handle com
            elif line.lower().strip() in ["no_com", "nocom"]:
                self._fix_com = True

            # handle orient
            elif line.lower().strip() in ["no_reorient", "noreorient"]:
                self._fix_orientation = True

            # handle charge and multiplicity
            elif cgmp.match(line):
                tempCharge = int(cgmp.match(line).group(1))
                tempMultiplicity = int(cgmp.match(line).group(2))

                if ifrag == 0:
                    self.charge = float(tempCharge)
                    self.multiplicity = tempMultiplicity
                self.fragment_charges.append(float(tempCharge))
                self.fragment_multiplicities.append(tempMultiplicity)

            # handle fragment markers and default fragment cgmp
            elif frag.match(line):
                try:
                    self.fragment_charges[ifrag]
                except IndexError:
                    self.fragment_charges.append(0.0)
                    self.fragment_multiplicities.append(1)
                ifrag += 1
                glines.append(line)

            elif atom.match(line.split()[0].strip()):
                glines.append(line)
            else:
                raise TypeError(
                    'Molecule:create_molecule_from_string: '
                    'Unidentifiable line in geometry specification: {}'.format(line))

        # catch last default fragment cgmp
        try:
            self.fragment_charges[ifrag]
        except IndexError:
            self.fragment_charges.append(0.0)
            self.fragment_multiplicities.append(1)

        # Now go through the rest of the lines looking for fragment markers
        ifrag = 0
        iatom = 0
        tempfrag = []
        atomSym = ""
        geometry = []
        tmpMass = []
        symbols = []
        custom_mass = False

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
                atomSym = atomm.group('symbol')

                # We don't know whether the @C or Gh(C) notation matched. Do a quick check.
                ghostAtom = False if (atomm.group('gh1') is None and atomm.group('gh2') is None) else True

                # Check that the atom symbol is valid
                if atomSym not in constants.el2z:
                    raise TypeError(
                        'Molecule:create_molecule_from_string: '
                        'Illegal atom symbol in geometry specification: {}'.format(atomSym))

                symbols.append(atomSym)
                zVal = constants.el2z[atomSym]
                if atomm.group('mass') is None:
                    atomMass = constants.el2masses[atomSym]
                else:
                    custom_mass = True
                    atomMass = float(atomm.group('mass'))
                tmpMass.append(atomMass)

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
                        raise TypeError("Molecule::create_molecule_from_string: "
                                        "Unidentifiable entry: {}.".format(entries[1]))

                    if realNumber.match(entries[2]):
                        yval = float(entries[2])
                    else:
                        raise TypeError("Molecule::create_molecule_from_string: "
                                        "Unidentifiable entry {}.".format(entries[2]))

                    if realNumber.match(entries[3]):
                        zval = float(entries[3])
                    else:
                        raise TypeError("Molecule::create_molecule_from_string: "
                                        "Unidentifiable entry {}.".format(entries[3]))

                    geometry.append([xval, yval, zval])
                else:
                    raise TypeError('Molecule::create_molecule_from_string: Illegal geometry specification line : {}.'
                                    'You should provide either Z-Matrix or Cartesian input'.format(line))

                iatom += 1

        if custom_mass:
            self.masses = tmpMass

        self.symbols = symbols
        self.geometry = np.array(geometry) * unit_conversion
        self.fragments.append(list(range(tempfrag[0], tempfrag[-1] + 1)))
        self.real.extend([True for x in range(tempfrag[0], tempfrag[-1] + 1)])

### Comparison and validation

    def validate(self, data=None):
        """
        Validates the current molecule for any errors
        """

        if data is None:
            data = self.to_json()

        schema.validate(data, "molecule")

    def compare(self, other, bench=None):
        """
        Checks if two molecules are identical
        """

        if isinstance(other, dict):
            other = Molecule.from_json(other, orient=False)
        elif isinstance(other, Molecule):
            pass
        else:
            raise TypeError("Comparison molecule not understood of type '{}'.".format(type(other)))

        if bench is None:
            bench = self


        match = True
        match &= bench.symbols == other.symbols
        if self._custom_masses or other._custom_masses:
            match &= np.allclose(bench.masses, other.masses, atol=MASS_NOISE)
        match &= np.equal(bench.real, other.real).all()
        match &= np.equal(bench.fragments, other.fragments).all()
        match &= np.allclose(bench.fragment_charges, other.fragment_charges, atol=CHARGE_NOISE)
        match &= np.equal(bench.fragment_multiplicities, other.fragment_multiplicities).all()

        match &= np.allclose(bench.charge, other.charge, atol=CHARGE_NOISE)
        match &= np.equal(bench.multiplicity, other.multiplicity).all()
        match &= np.allclose(bench.geometry, other.geometry, atol=GEOMETRY_NOISE)
        return match

    def pretty_print(self):
        """Print the molecule in Angstroms. Same as :py:func:`print_out` only always in Angstroms.
        (method name in libmints is print_in_angstrom)

        """
        text = ""

        text += """    Geometry (in {0:s}), charge = {1:.1f}, multiplicity = {2:d}:\n\n""".format(
            'Angstrom', self.charge, self.multiplicity)
        text += """       Center              X                  Y                   Z       \n"""
        text += """    ------------   -----------------  -----------------  -----------------\n"""

        for i in range(len(self.geometry)):
            text += """    {0:8s}{1:4s} """.format(self.symbols[i], "" if self.real[i] else "(Gh)")
            for j in range(3):
                text += """  {0:17.12f}""".format(self.geometry[i][j] * constants.physconst["bohr2angstroms"])
            text += "\n"
        text += "\n"

        return text

    def __str__(self):
        return self.pretty_print()


### Orientation methods

    def _inertial_tensor(self, geom, weight):
        """
        Compute the moment inertia tensor for a given geometry.
        """
        # Build inertia tensor
        tensor = np.zeros((3, 3))

        # Diagonal
        tensor[0][0] = np.sum(weight * (geom[:, 1]**2.0 + geom[:, 2]**2.0))
        tensor[1][1] = np.sum(weight * (geom[:, 0]**2.0 + geom[:, 2]**2.0))
        tensor[2][2] = np.sum(weight * (geom[:, 0]**2.0 + geom[:, 1]**2.0))

        # I(alpha, beta)
        # Off diagonal
        tensor[0][1] = -1.0 * np.sum(weight * geom[:, 0] * geom[:, 1])
        tensor[0][2] = -1.0 * np.sum(weight * geom[:, 0] * geom[:, 2])
        tensor[1][2] = -1.0 * np.sum(weight * geom[:, 1] * geom[:, 2])

        # Other half
        tensor[1][0] = tensor[0][1]
        tensor[2][0] = tensor[0][2]
        tensor[2][1] = tensor[1][2]
        return tensor

    def orient_molecule(self):
        """
        Centers the molecule and orients via inertia tensor.
        """

        # Get the mass as an array

        # Masses are needed for orientation
        if self._custom_masses is False:
            np_mass = np.array([constants.el2masses[x.upper()] for x in self.symbols])
        else:
            np_mass = np.array(self.masses)

        # Center on Mass
        self.geometry -= np.average(self.geometry, axis=0, weights=np_mass)

        # Rotate into inertial frame
        tensor = self._inertial_tensor(self.geometry, np_mass)
        evals, evecs = np.linalg.eigh(tensor)

        self.geometry = np.dot(self.geometry, evecs)

        # Phases? Lets do the simplest thing and ensure the first atom in each column
        # that is not on a plane is positve

        phase_check = [False, False, False]

        geom_noise = 10**(-GEOMETRY_NOISE)
        for num in range(self.geometry.shape[0]):

            for x in range(3):
                if phase_check[x]:
                    continue

                val = self.geometry[num, x]

                if abs(val) < geom_noise:
                    continue

                phase_check[x] = True

                if val < 0:
                    self.geometry[:, x] *= -1

            if sum(phase_check) == 3:
                break

    def get_fragment(self, real, ghost=None, orient=False):
        """
        A list of real and ghost fragments:
        """

        if isinstance(real, int):
            real = [real]

        if isinstance(ghost, int):
            ghost = [ghost]
        elif ghost is None:
            ghost = []

        ret_name = self.name + " (" + str(real) + "," + str(ghost) + ")"
        ret = Molecule(None, name=ret_name)

        if len(set(real) & set(ghost)):
            raise TypeError(
                "Molecule:get_fragment: real and ghost sets are overlapping! ({0}, {1}).".format(str(real), str(ghost)))

        geom_blocks = []
        symbols = []
        masses = []
        real_atoms = []

        # Loop through the real blocks
        frag_start = 0
        for frag in real:
            frag_size = len(self.fragments[frag])
            geom_blocks.append(self.geometry[self.fragments[frag]])

            for idx in self.fragments[frag]:
                symbols.append(self.symbols[idx])
                real_atoms.append(True)
                if self._custom_masses:
                    masses.append(self.masses[idx])

            ret.fragments.append(list(range(frag_start, frag_start + frag_size)))
            frag_start += frag_size

            ret.fragment_charges.append(float(self.fragment_charges[frag]))
            ret.fragment_multiplicities.append(self.fragment_multiplicities[frag])

        # Set charge and multiplicity
        ret.charge = sum(ret.fragment_charges)
        ret.multiplicity = sum(x - 1 for x in ret.fragment_multiplicities) + 1

        # Loop through the ghost blocks
        for frag in ghost:
            frag_size = len(self.fragments[frag])
            geom_blocks.append(self.geometry[self.fragments[frag]])

            for idx in self.fragments[frag]:
                symbols.append(self.symbols[idx])
                real_atoms.append(False)
                if self._custom_masses:
                    masses.append(self.masses[idx])

            ret.fragments.append(list(range(frag_start, frag_start + frag_size)))
            frag_start += frag_size

            ret.fragment_charges.append(self.fragment_charges[frag])
            ret.fragment_multiplicities.append(self.fragment_multiplicities[frag])

        ret.symbols = symbols
        ret.geometry = np.vstack(geom_blocks)
        ret.real = real_atoms
        if self._custom_masses:
            ret.masses = masses

        if orient:
            ret.orient_molecule()

        return ret

    def to_string(self, dtype="psi4"):
        """Returns a string that can be used by a variety of programs.
        """
        if dtype == "psi4":
            return self._to_psi4_string()
        else:
            raise KeyError("Molecule:to_string: dtype of '{}' not recognized.".format(dtype))

    def _to_psi4_string(self):
        """Regenerates a input file molecule specification string from the
        current state of the Molecule. Contains geometry info,
        fragmentation, charges and multiplicities, and any frame
        restriction.
        """
        text = "\n"

        # append atoms and coordinates and fragment separators with charge and multiplicity
        for num, frag in enumerate(self.fragments):
            divider = "    --"
            if num == 0:
                divider = ""

            if any(self.real[at] for at in frag):
                text += "{0:s}    \n    {1:d} {2:d}\n".format(divider, int(self.fragment_charges[num]),
                                                              self.fragment_multiplicities[num])

            for at in frag:
                if self.real[at]:
                    text += "    {0:<8s}".format(str(self.symbols[at]))
                else:
                    text += "    {0:<8s}".format("Gh(" + self.symbols[at] + ")")
                text += "    {0: 14.10f} {1: 14.10f} {2: 14.10f}\n".format(*tuple(self.geometry[at]))
        text += "\n"

        # append units and any other non-default molecule keywords
        text += "    units bohr\n"
        text += "    no_com\n"
        text += "    no_reorient\n"

        return text

    @classmethod
    def from_json(cls, data, orient=False):
        return cls(data, dtype="json", orient=orient)

    def to_json(self):
        """
        Returns a JSON form of the Molecule object.
        """

        np.set_printoptions(precision=16)
        ret = {}
        for field in schema.get_schema_keys("molecule"):

            if field == "fix_com":
                data = self._fix_com
            elif field == "fix_orientation":
                data = self._fix_orientation
            else:
                data = getattr(self, field)

            # Do we add this data?
            if isinstance(data, (np.ndarray, list, tuple, dict, str)) and (len(data) == 0): continue

            # If we added masses for orientation, continue
            if (field == "masses") and (self._custom_masses is False):
                continue

            if field == "geometry":
                ret[field] = hash_helpers.float_prep(data, GEOMETRY_NOISE).ravel().tolist()
            elif field == "fragment_charges":
                ret[field] = hash_helpers.float_prep(data, CHARGE_NOISE).tolist()
            elif field == "charge":
                ret[field] = hash_helpers.float_prep(data, CHARGE_NOISE)
            elif field == "masses":
                ret[field] = hash_helpers.float_prep(data, MASS_NOISE).tolist()
            else:
                ret[field] = data

        self.validate(data=ret)
        return ret

    def get_hash(self):
        """
        Returns the hash of the molecule.
        """

        m = hashlib.sha1()
        concat = ""

        tmp_json = self.to_json()
        for field in schema.get_hash_fields("molecule"):
            if field not in tmp_json:
                continue
            concat += json.dumps(tmp_json[field])

        m.update(concat.encode("utf-8"))
        return m.hexdigest()

    def get_molecular_formula(self):
        """
        Returns the molecular formula for a molecule. Atom symbols are sorted from
        A-Z.

        Examples
        --------

        >>> methane = portal.Molecule('''
          H      0.5288      0.1610      0.9359
          C      0.0000      0.0000      0.0000
          H      0.2051      0.8240     -0.6786
          H      0.3345     -0.9314     -0.4496
          H     -1.0685     -0.0537      0.1921
        ''')

        >>> methane.get_molecular_formula()
        CH4

        >>> hcl = portal.Molecule('''
          H      0.0000      0.0000      0.0000
          Cl     0.0000      0.0000      1.2000
        ''')

        >>> hcl.get_molecular_formula()
        ClH

        """
        count = collections.Counter(x.title() for x in self.symbols)

        ret = []
        for k in sorted(count.keys()):
            c = count[k]
            ret.append(k)
            if c > 1:
                ret.append(str(c))

        return "".join(ret)