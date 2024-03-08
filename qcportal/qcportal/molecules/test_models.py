"""
Tests the imports and exports of the Molecule object.
"""

import json

import numpy as np
import pytest
import qcelemental as qcel

from qcarchivetesting import load_molecule_data
from .models import Molecule


def test_molecule_constructors():
    ### Water Dimer
    water_psi = load_molecule_data("water_dimer_minima")
    ele = np.array([8, 1, 1, 8, 1, 1]).reshape(-1, 1)
    npwater = np.hstack((ele, water_psi.geometry * qcel.constants.conversion_factor("Bohr", "angstrom")))
    water_from_np = Molecule.from_data(npwater, name="water dimer", dtype="numpy", frags=[3])

    assert water_psi == water_from_np
    assert water_psi.get_molecular_formula() == "H4O2"

    # Check the JSON construct/deconstruct
    water_from_json = Molecule(**water_psi.dict())
    assert water_psi == water_from_json

    ### Neon Tetramer
    neon_from_psi = load_molecule_data("neon_tetramer")
    ele = np.array([10, 10, 10, 10]).reshape(-1, 1)
    npneon = np.hstack((ele, neon_from_psi.geometry))
    neon_from_np = Molecule.from_data(npneon, name="neon tetramer", dtype="numpy", frags=[1, 2, 3], units="bohr")

    assert neon_from_psi == neon_from_np

    # Check the JSON construct/deconstruct
    neon_from_json = Molecule(**neon_from_psi.dict())
    assert neon_from_psi == neon_from_json
    assert neon_from_json.get_molecular_formula() == "Ne4"

    assert water_psi == Molecule.from_data(water_psi.to_string("psi4"))


def test_water_minima_data():
    mol = load_molecule_data("water_dimer_minima")

    assert sum(x == y for x, y in zip(mol.symbols, ["O", "H", "H", "O", "H", "H"])) == mol.geometry.shape[0]
    assert mol.molecular_charge == 0
    assert mol.molecular_multiplicity == 1
    assert np.sum(mol.real) == mol.geometry.shape[0]
    assert np.allclose(mol.fragments, [[0, 1, 2], [3, 4, 5]])
    assert np.allclose(mol.fragment_charges, [0, 0])
    assert np.allclose(mol.fragment_multiplicities, [1, 1])
    assert hasattr(mol, "provenance")
    assert np.allclose(
        mol.geometry,
        [
            [2.81211080, 0.1255717, 0.0],
            [3.48216664, -1.55439981, 0.0],
            [1.00578203, -0.1092573, 0.0],
            [-2.6821528, -0.12325075, 0.0],
            [-3.27523824, 0.81341093, 1.43347255],
            [-3.27523824, 0.81341093, -1.43347255],
        ],
    )  # yapf: disable
    assert mol.get_hash() == "3c4b98f515d64d1adc1648fe1fe1d6789e978d34"


def test_water_minima_fragment():
    mol = load_molecule_data("water_dimer_minima")

    frag_0 = mol.get_fragment(0, orient=True)
    frag_1 = mol.get_fragment(1, orient=True)
    assert frag_0.get_hash() == "5f31757232a9a594c46073082534ca8a6806d367"
    assert frag_1.get_hash() == "bdc1f75bd1b7b999ff24783d7c1673452b91beb9"

    frag_0_1 = mol.get_fragment(0, 1)
    frag_1_0 = mol.get_fragment(1, 0)

    assert np.array_equal(mol.symbols[:3], frag_0.symbols)
    assert np.allclose(mol.masses[:3], frag_0.masses)

    assert np.array_equal(mol.symbols, frag_0_1.symbols)
    assert np.allclose(mol.geometry, frag_0_1.geometry)

    assert np.array_equal(np.hstack((mol.symbols[3:], mol.symbols[:3])), frag_1_0.symbols)
    assert np.allclose(np.hstack((mol.masses[3:], mol.masses[:3])), frag_1_0.masses)


def test_pretty_print():
    mol = load_molecule_data("water_dimer_minima")
    assert isinstance(mol.pretty_print(), str)


def test_to_string():
    mol = load_molecule_data("water_dimer_minima")
    assert isinstance(mol.to_string("psi4"), str)


def test_water_orient():
    # These are identical molecules, should find the correct results

    mol = load_molecule_data("water_dimer_stretch")
    frag_0 = mol.get_fragment(0, orient=True)
    frag_1 = mol.get_fragment(1, orient=True)

    # Make sure the fragments match
    assert frag_0.get_hash() == frag_1.get_hash()

    # Make sure the complexes match
    frag_0_1 = mol.get_fragment(0, 1, orient=True, group_fragments=True)
    frag_1_0 = mol.get_fragment(1, 0, orient=True, group_fragments=True)

    assert frag_0_1.get_hash() == frag_1_0.get_hash()

    mol = load_molecule_data("water_dimer_stretch2")
    frag_0 = mol.get_fragment(0, orient=True)
    frag_1 = mol.get_fragment(1, orient=True)

    # Make sure the fragments match
    assert frag_0.molecular_multiplicity == 1
    assert frag_0.get_hash() == frag_1.get_hash()

    # Make sure the complexes match
    frag_0_1 = mol.get_fragment(0, 1, orient=True, group_fragments=True)
    frag_1_0 = mol.get_fragment(1, 0, orient=True, group_fragments=True)

    # Ghost fragments should prevent overlap
    assert frag_0_1.molecular_multiplicity == 1
    assert frag_0_1.get_hash() != frag_1_0.get_hash()


def test_molecule_errors():
    mol = load_molecule_data("water_dimer_stretch")

    data = mol.dict()
    data["whatever"] = 5
    with pytest.raises(ValueError):
        Molecule(**data)


def test_molecule_repeated_hashing():
    mol = Molecule(
        **{
            "symbols": ["H", "O", "O", "H"],
            "geometry": [
                1.73178198,
                1.29095807,
                1.03716028,
                1.31566305,
                -0.007440200000000001,
                -0.28074722,
                -1.3143081,
                0.00849608,
                -0.27416914,
                -1.7241109,
                -1.30793432,
                1.02770172,
            ],
        }
    )

    h1 = mol.get_hash()
    assert mol.get_molecular_formula() == "H2O2"

    mol2 = Molecule(**json.loads(mol.json()), orient=False)
    assert h1 == mol2.get_hash()

    mol3 = Molecule(**json.loads(mol2.json()), orient=False)
    assert h1 == mol3.get_hash()


def test_molecule_water_canary_hash():
    water_dimer_minima = Molecule.from_data(
        """
    0 1
    O  -1.551007  -0.114520   0.000000
    H  -1.934259   0.762503   0.000000
    H  -0.599677   0.040712   0.000000
    --
    O   1.350625   0.111469   0.000000
    H   1.680398  -0.373741  -0.758561
    H   1.680398  -0.373741   0.758561
    """,
        dtype="psi4",
    )
    assert water_dimer_minima.get_hash() == "42f3ac52af52cf2105c252031334a2ad92aa911c"

    # Check orientation
    mol = water_dimer_minima.orient_molecule()
    assert mol.get_hash() == "632490a0601500bfc677e9277275f82fbc45affe"

    frag_0 = mol.get_fragment(0, orient=True)
    frag_1 = mol.get_fragment(1, orient=True)
    assert frag_0.get_hash() == "d0b499739f763e8d3a5556b4ddaeded6a148e4d5"
    assert frag_1.get_hash() == "bdc1f75bd1b7b999ff24783d7c1673452b91beb9"


@pytest.mark.parametrize(
    "geom",
    [[0, 0, 0, 0, 5, 0], [0, 0, 0, 0, 5, 0 + 1.0e-12], [0, 0, 0, 0, 5, 0 - 1.0e-12], [0, 0, 0, 0, 5, 0 + 1.0e-7]],
)  # yapf: disable
def test_molecule_geometry_canary_hash(geom):
    mol = Molecule(geometry=geom, symbols=["H", "H"])

    assert mol.get_hash() == "fb69e6744407b220a96d6ddab4ec2099619db791"
