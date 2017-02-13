import numpy as np
from mongo_qcdb import molecule
from mongo_qcdb import test_util 


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

_water_dimer_stretch = """
0 1
O  -1.551007  -0.114520   0.000000
H  -1.934259   0.762503   0.000000
H  -0.599677   0.040712   0.000000
--
0 1
O   1.350625   0.111469   10.000000
H   1.680398  -0.373741  -10.758561
H   1.680398  -0.373741   10.758561
"""


def test_water_minima_data():
    mol = molecule.Molecule(_water_dimer_minima, name="water dimer")

    assert len(str(mol)) == 660
    assert sum(x == y for x, y in zip(mol.symbols, ['O', 'H', 'H', 'O', 'H', 'H'])) == mol.geometry.shape[0]
    assert np.allclose(mol.masses, [15.99491461956, 1.00782503207, 1.00782503207, 15.99491461956, 1.00782503207, 1.00782503207])
    assert mol.name == "water dimer"
    assert mol.charge == 0
    assert mol.multiplicity == 1
    assert np.sum(mol.real) == mol.geometry.shape[0]
    assert np.allclose(mol.fragments, [[0, 1, 2], [3, 4, 5]])
    assert np.allclose(mol.fragment_charges, [0, 0])
    assert np.allclose(mol.fragment_multiplicities, [1, 1])
    assert hasattr(mol, "provenance")


def test_water_minima_fragment():

    mol = molecule.Molecule(_water_dimer_minima, name="water dimer")
    frag_0 = mol.get_fragment(0)
    frag_1 = mol.get_fragment(1)
    
    frag_0_1 = mol.get_fragment(0, 1)
    frag_1_0 = mol.get_fragment(1, 0)
    
    assert np.allclose(mol.geometry[:3], frag_0.geometry)
    assert test_util.compare_lists(mol.symbols[:3], frag_0.symbols)
    assert np.allclose(mol.masses[:3], frag_0.masses)
    
    assert np.allclose(mol.geometry[3:], frag_1.geometry)
    assert np.allclose(mol.geometry, frag_0_1.geometry)
    
    assert np.allclose(np.vstack((mol.geometry[3:], mol.geometry[:3])), frag_1_0.geometry)
    assert test_util.compare_lists(mol.symbols[3:] + mol.symbols[:3], frag_1_0.symbols)
    assert np.allclose(mol.masses[3:] + mol.masses[:3], frag_1_0.masses)

#def test_water_minima_ie_stoich():



