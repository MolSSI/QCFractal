import numpy as np

import mongo_qcdb as mdb
from mongo_qcdb import molecule
from mongo_qcdb import test_util

# Build a few test molecules
_water_dimer_minima = """
0 1
O  -1.551007  -0.114520   0.000000
H  -1.934259   0.762503   0.000000
H  -0.599677   0.040712   0.000000
--
0 1
O   1.350625   0.111469   0.000000
H   1.680398  -0.373741  -0.758561
H   1.680398  -0.373741   0.758561
"""

_water_dimer_minima_np = np.array(
    [[8, -1.551007, -0.114520, 0.000000], [1, -1.934259, 0.762503, 0.000000],
     [1, -0.599677, 0.040712, 0.000000], [8, 1.350625, 0.111469, 0.000000],
     [1, 1.680398, -0.373741, -0.758561], [1, 1.680398, -0.373741, 0.758561]])

_water_dimer_stretch = """
0 1
O  -1.551007  -0.114520   0.000000
H  -1.934259   0.762503   0.000000
H  -0.599677   0.040712   0.000000
--
O  -0.114520  -1.551007  10.000000
H   0.762503  -1.934259  10.000000
H   0.040712  -0.599677  10.000000
"""

_water_dimer_stretch_2 = """
0 1
O  -1.551007  -0.114520   0.000000
H  -1.934259   0.762503   0.000000
H  -0.599677   0.040712   0.000000
--
O  -11.551007  -0.114520   0.000000
H  -11.934259   0.762503   0.000000
H  -10.599677   0.040712   0.000000
"""

_neon_tetramer = """
0 1
Ne 0.000000 0.000000 0.000000
--
Ne 3.100000 0.000000 0.000000
--
Ne 0.000000 3.200000 0.000000
--
Ne 0.000000 0.000000 3.300000
units bohr
"""

_neon_tetramer_np = np.array(
    [[10, 0.000000, 0.000000, 0.000000], [10, 3.100000, 0.000000, 0.000000],
     [10, 0.000000, 3.200000, 0.000000], [10, 0.000000, 0.000000, 3.300000]])
_neon_tetramer_np[:, 1:] *= mdb.constants.physconst["bohr2angstroms"]


# Start tests
def _compare_molecule(bench, other):
    assert test_util.compare_lists(bench.symbols, other.symbols)
    assert test_util.compare_lists(bench.masses, other.masses)
    assert test_util.compare_lists(bench.real, other.real)
    assert test_util.compare_lists(bench.fragments, other.fragments)
    assert test_util.compare_lists(bench.fragment_charges, other.fragment_charges)
    assert test_util.compare_lists(bench.fragment_multiplicities, other.fragment_multiplicities)

    assert bench.charge == other.charge
    assert bench.multiplicity == other.multiplicity
    assert np.allclose(bench.geometry, other.geometry)
    return True


def test_molecule_constructors():

    ### Water Dimer
    water_psi = molecule.Molecule(_water_dimer_minima, name="water dimer")
    water_np = molecule.Molecule(
        _water_dimer_minima_np, name="water dimer", dtype="numpy", frags=[3])

    assert _compare_molecule(water_psi, water_np)

    # Check the JSON construct/deconstruct
    water_from_json = molecule.Molecule(water_psi.to_json(), dtype="json")
    assert _compare_molecule(water_psi, water_from_json)


    ### Neon Tetramer
    neon_psi = molecule.Molecule(_neon_tetramer, name="neon tetramer")
    neon_np = molecule.Molecule(
        _neon_tetramer_np, name="neon tetramer", dtype="numpy", frags=[1, 2, 3])

    assert _compare_molecule(neon_psi, neon_np)

    # Check the JSON construct/deconstruct
    neon_from_json = molecule.Molecule(neon_psi.to_json(), dtype="json")
    assert _compare_molecule(neon_psi, neon_from_json)



def test_water_minima_data():
    mol = molecule.Molecule(_water_dimer_minima, name="water dimer")

    assert len(str(mol)) == 662
    assert len(mol.to_string()) == 442

    assert sum(
        x == y
        for x, y in zip(mol.symbols, ['O', 'H', 'H', 'O', 'H', 'H'])) == mol.geometry.shape[0]
    assert np.allclose(mol.masses, [
        15.99491461956, 1.00782503207, 1.00782503207, 15.99491461956, 1.00782503207, 1.00782503207
    ])
    assert mol.name == "water dimer"
    assert mol.charge == 0
    assert mol.multiplicity == 1
    assert np.sum(mol.real) == mol.geometry.shape[0]
    assert np.allclose(mol.fragments, [[0, 1, 2], [3, 4, 5]])
    assert np.allclose(mol.fragment_charges, [0, 0])
    assert np.allclose(mol.fragment_multiplicities, [1, 1])
    assert hasattr(mol, "provenance")
    assert np.allclose(mol.geometry, [[2.81211080, 0.1255717, 0.], [3.48216664, -1.55439981, 0.],
                                      [1.00578203, -0.1092573, 0.], [-2.6821528, -0.12325075, 0.],
                                      [-3.27523824, 0.81341093, 1.43347255],
                                      [-3.27523824, 0.81341093, -1.43347255]])
    assert mol.get_hash() == "46541b6905ff43fd6c0b9222709137584a02f7f7"


def test_water_minima_fragment():

    mol = molecule.Molecule(_water_dimer_minima, name="water dimer")

    frag_0 = mol.get_fragment(0)
    frag_1 = mol.get_fragment(1)
    assert frag_0.get_hash() == "a290235adfe412851429cd658fef0934b260977a"
    assert frag_1.get_hash() == "851d6bca6ddbda87fa58dc105c3db94bab39139c"

    frag_0_1 = mol.get_fragment(0, 1)
    frag_1_0 = mol.get_fragment(1, 0)

    assert test_util.compare_lists(mol.symbols[:3], frag_0.symbols)
    assert np.allclose(mol.masses[:3], frag_0.masses)

    assert test_util.compare_lists(mol.symbols, frag_0_1.symbols)
    assert np.allclose(mol.geometry, frag_0_1.geometry)

    assert test_util.compare_lists(mol.symbols[3:] + mol.symbols[:3], frag_1_0.symbols)
    assert np.allclose(mol.masses[3:] + mol.masses[:3], frag_1_0.masses)


def test_water_orient():
    # These are identical molecules, should find the correct results

    mol = molecule.Molecule(_water_dimer_stretch, name="water dimer stretch")
    frag_0 = mol.get_fragment(0)
    frag_1 = mol.get_fragment(1)

    # Make sure the fragments match
    assert frag_0.get_hash() == frag_1.get_hash()

    # Make sure the complexes match
    frag_0_1 = mol.get_fragment(0, 1)
    frag_1_0 = mol.get_fragment(1, 0)

    assert frag_0_1.get_hash() == frag_1_0.get_hash()

    mol = molecule.Molecule(_water_dimer_stretch_2, name="water dimer stretch 2")
    frag_0 = mol.get_fragment(0)
    frag_1 = mol.get_fragment(1)

    # Make sure the fragments match
    assert frag_0.get_hash() == frag_1.get_hash()

    # Make sure the complexes match
    frag_0_1 = mol.get_fragment(0, 1)
    frag_1_0 = mol.get_fragment(1, 0)

    # Ghost fragments should prevent overlap
    assert frag_0_1.get_hash() != frag_1_0.get_hash()
